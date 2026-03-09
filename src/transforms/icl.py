import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, List, cast
from collections import OrderedDict

import pandas as pd

from schemas.brief.v2 import Brief, Memo
from schemas.judge.v1 import Judgement
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.judge.v0 import MODULE as JUDGE_MODULE
from schemas.summary.v2 import Substantive
from schemas.summary.v4 import Summary

from scripts.labeling.brief_labels import BriefLabelV3
from scripts.labeling.summary_labels import SummaryLabelV2
from src.adapters.llm.protocol import Message
from src.services.blob import BlobService, load_validated_json_blob
from src.stages import Stage
from src.transforms.differ import DiffDoc
from src.transforms.summary.diff_chunker import StandardDiffFormatter


class LabeledDataLoader(ABC):
    """Base class for loading labeled data from disk."""
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    _cache: OrderedDict
    data_subdir: str = "eval"
    MAX_CACHE = 5

    storage: BlobService

    def __init__(self, storage: BlobService):
        self.storage = storage
        self._cache = OrderedDict()

    def _cache_get(self, key):
        if key in self._cache:
            self._cache.move_to_end(key)
        return self._cache.get(key)

    def _cache_add(self, key, value) -> None:
        self._cache[key] = value
        if len(self._cache) > self.MAX_CACHE:
            self._cache.popitem(last=False)

    def load_labels(self, version: str = "", stage: str = "") -> list[dict]:
        if not version and not stage:
            raise ValueError("must provide version or stage")
        elif version:
            filepath = self._find_label_file(version)
            with open(filepath) as f:
                data = json.load(f)
        else:
            data = []
            for version in self.find_all_versions():
                version_stage = "_".join(version.split("_")[:-1])
                if version_stage == stage:
                    with open(self.DATA_DIR / self.data_subdir / version / "records.json") as f:
                        data.extend(json.load(f))
        return data

    def _find_label_file(self, version: str) -> str:
        """Find records.json file for given version in data subdirectory."""
        search_dir = self.DATA_DIR / self.data_subdir
        for root, dirs, files in os.walk(search_dir):
            if Path(root).name == version:
                return os.path.join(root, "records.json")
        raise FileNotFoundError(f"{version} not found in {search_dir}")


    def find_all_versions(self) -> Iterator[str]:
        """Find all label versions in data subdirectory."""
        search_dir = self.DATA_DIR / self.data_subdir
        for root, dirs, files in os.walk(search_dir):
            if ".argilla" in root:
                continue
            for name in files:
                if name == "records.json":
                    yield Path(root).name

    def load_blob_keys(self, version: str = "", stage: str = ""):
        labels = self.load_labels(version=version, stage=stage)
        blob_keys = []
        for label in labels:
            x = self.storage.parse_blob_path(label["metadata"]['blob_path'])
            blob_keys.append("/".join((x.company, x.policy, x.timestamp)))
        return blob_keys

    @abstractmethod
    def load_eval_labels(self, label_version: str = "", stage: str = "", prompt_version: str = "") -> pd.DataFrame:
        """Load ground truth labels from evaluation dataset."""
        pass

    @abstractmethod
    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels dataset."""
        pass

    @abstractmethod
    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from blob storage."""
        pass

    @abstractmethod
    def pick_icl(self, version: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def load_icl(self, prompt_version: str) -> List[Message]:
        pass


class JudgeDataLoader(LabeledDataLoader):

    def load_all_eval_labels(self) -> pd.DataFrame:
        return pd.concat([self.load_eval_labels(stage="brief"),
                        self.load_eval_labels(stage="summary")]
        )

    def load_all_true_labels(self) -> pd.DataFrame:
        return pd.concat([self.load_true_labels(stage="brief"),
                        self.load_true_labels(stage="summary")]
        )

    def load_eval_labels(self, label_version: str = "", stage: str = "", prompt_version: str = "") -> pd.DataFrame:
        labels = self.load_true_labels(version=label_version, stage=stage)
        return labels


    def load_true_labels(self, version: str = "", stage: str = "") -> pd.DataFrame:
        """Load ground truth labels for judge evaluation."""
        gold_list = []
        labels = self.load_labels(version, stage)
        for label in labels:
            gold_list.append(label['metadata'] | label['fields'] | SummaryLabelV2.from_dict(label).model_dump())
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore

        # Add this for easy linking
        gold['blob_key'] = gold['blob_path'].apply(self.storage.parse_blob_path).apply(
            lambda x: "/".join((x.company, x.policy, x.timestamp)))

        # Change dtypes
        remap_cols = ['practically_substantive_true', 'practically_substantive_pred']
        for col in remap_cols:
            gold[col] = gold[col].astype('boolean')

        # Drop suggestion columns and simplify names
        gold = gold.drop(columns=[c for c in gold.columns if c.endswith('_pred')])
        gold = gold.rename(columns=lambda c: c.removesuffix('_true'))

        # Drop missing data
        gold = gold.dropna()

        return gold

    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from JUDGE_CLEAN stage."""
        predictions_df = self._cache_get(Stage.JUDGE_CLEAN.value)
        if predictions_df is not None:
            return predictions_df
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.JUDGE_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            parts = self.storage.parse_blob_path(blob)
            blob_key = "/".join((parts.company, parts.policy, parts.timestamp))
            metadata = self.storage.adapter.load_metadata(blob)
            summary = load_validated_json_blob(blob, JUDGE_MODULE, self.storage)
            summary = cast(Judgement, summary)
            rating = 1.0 if summary.practically_substantive.rating else 0.0
            predictions_list.append(metadata | dict(
                blob_path=blob,
                blob_key=blob_key,
                practically_substantive=rating
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        if "AzureWebJobsParentId" in predictions_df.columns:
            predictions_df = predictions_df.drop(columns=["AzureWebJobsParentId"])
        predictions_df = predictions_df.drop_duplicates()
        self._cache_add(Stage.JUDGE_CLEAN.value, predictions_df)
        return predictions_df

    def load_raw_exists(self) -> pd.DataFrame:
        blobs = self.storage.adapter.list_blobs()
        raw_blobs = [b for b in blobs
                       if b.startswith(Stage.JUDGE_RAW.value)
                       and not b.endswith("latest.txt")]
        meta_list = []
        for blob in raw_blobs:
            metadata = self.storage.adapter.load_metadata(blob)
            parts = self.storage.parse_blob_path(blob)
            blob_key = "/".join((parts.company, parts.policy, parts.timestamp))
            meta_list.append(metadata | dict(blob_path = blob, blob_key = blob_key))
        meta_df = pd.DataFrame.from_records(meta_list)
        if "AzureWebJobsParentId" in meta_df.columns:
            meta_df = meta_df.drop(columns=["AzureWebJobsParentId"])
        meta_df = meta_df.drop_duplicates()
        return meta_df


    def load_icl(self, prompt_version: str) -> List[Message]:
        raise NotImplementedError()

    def pick_icl(self, version: str) -> pd.DataFrame:
        raise NotImplementedError()


class SummaryDataLoader(LabeledDataLoader):
    """Loads summary stage evaluation data (ground truth from labels + predictions from JUDGE_CLEAN)."""

    def load_all_eval_labels(self) -> pd.DataFrame:
            return pd.concat([self.load_eval_labels(stage="brief"),
                            self.load_eval_labels(stage="summary")]
            )


    def load_all_true_labels(self) -> pd.DataFrame:
        return pd.concat([self.load_true_labels(stage="brief"),
                        self.load_true_labels(stage="summary")]
        )


    def load_eval_labels(self, label_version: str = "", stage: str = "", prompt_version: str = "") -> pd.DataFrame:
        labels = self.load_true_labels(version=label_version, stage=stage)
        icl = self.pick_icl(prompt_version)
        return labels[~labels["blob_key"].isin(icl["blob_key"])]


    def load_true_labels(self, version: str = "", stage: str = "") -> pd.DataFrame:
        """Load ground truth labels for summary evaluation."""
        gold_list = []
        labels = self.load_labels(version, stage)
        for label in labels:
            gold_list.append(label['metadata'] | label['fields'] | SummaryLabelV2.from_dict(label).model_dump())
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore

        # Add this for easy linking
        gold['blob_key'] = gold['blob_path'].apply(self.storage.parse_blob_path).apply(
            lambda x: "/".join((x.company, x.policy, x.timestamp)))

        # Change dtypes
        remap_cols = ['practically_substantive_true', 'practically_substantive_pred']
        for col in remap_cols:
            gold[col] = gold[col].astype('boolean')

        # Drop suggestion columns and simplify names
        gold = gold.drop(columns=[c for c in gold.columns if c.endswith('_pred')])
        gold = gold.rename(columns=lambda c: c.removesuffix('_true'))

        # Drop missing data
        gold = gold.dropna()

        return gold


    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from SUMMARY_CLEAN stage."""
        predictions_df = self._cache_get(Stage.SUMMARY_CLEAN.value)
        if predictions_df is not None:
            return predictions_df
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.SUMMARY_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            parts = self.storage.parse_blob_path(blob)
            blob_key = "/".join((parts.company, parts.policy, parts.timestamp))
            metadata = self.storage.adapter.load_metadata(blob)
            summary = load_validated_json_blob(blob, SUMMARY_MODULE, self.storage)
            summary = cast(Summary, summary)
            rating = 1.0 if summary.practically_substantive.rating else 0.0
            predictions_list.append(metadata | dict(
                blob_path=blob,
                blob_key=blob_key,
                practically_substantive=rating
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        if "AzureWebJobsParentId" in predictions_df.columns:
            predictions_df = predictions_df.drop(columns=["AzureWebJobsParentId"])
        predictions_df = predictions_df.drop_duplicates()
        self._cache_add(Stage.SUMMARY_CLEAN.value, predictions_df)
        return predictions_df


    def load_raw_exists(self) -> pd.DataFrame:
        blobs = self.storage.adapter.list_blobs()
        raw_blobs = [b for b in blobs
                       if b.startswith(Stage.SUMMARY_RAW.value)
                       and not b.endswith("latest.txt")]
        meta_list = []
        for blob in raw_blobs:
            metadata = self.storage.adapter.load_metadata(blob)
            parts = self.storage.parse_blob_path(blob)
            blob_key = "/".join((parts.company, parts.policy, parts.timestamp))
            meta_list.append(metadata | dict(blob_path = blob, blob_key = blob_key))
        meta_df = pd.DataFrame.from_records(meta_list)
        if "AzureWebJobsParentId" in meta_df.columns:
            meta_df = meta_df.drop(columns=["AzureWebJobsParentId"])
        meta_df = meta_df.drop_duplicates()
        return meta_df


    def pick_icl(self, prompt_version: str = "") -> pd.DataFrame:
        y_pred = self.load_pred_labels()
        
        # Return empty DataFrame if no predictions exist
        if y_pred.empty:
            return pd.DataFrame(columns=["label", "blob_key", "text", "feedback"])
        
        y_pred["practically_substantive"] = y_pred["practically_substantive"].astype(bool)

        labels = self.load_all_true_labels()
        lineage_groups = ['brief_schema_version', 'brief_prompt_version', 'brief_model_version',
                          'summary_model_version', 'summary_prompt_version', 'summary_schema_version']
        labels = labels.drop(columns=lineage_groups, errors='ignore')
        labels = labels[labels["memo_output"].notna()]

        if prompt_version:
            y_latest = y_pred[y_pred["summary_prompt_version"] == prompt_version]
        else:
            y_latest = y_pred

        merged = labels.merge(y_latest, on="blob_key", suffixes=("_true", "_pred"))
        merged["FP"] = merged["practically_substantive_pred"] & ~merged["practically_substantive_true"]
        merged["FN"] = ~merged["practically_substantive_pred"] & merged["practically_substantive_true"]
        merged["TP"] = merged["practically_substantive_pred"] & merged["practically_substantive_true"]
        merged["TN"] = ~merged["practically_substantive_pred"] & ~merged["practically_substantive_true"]
        merged["label"] = merged[["FP","FN","TP","TN"]].idxmax(axis=1)

        errors = merged[(merged["FP"] | merged["FN"]) & merged["feedback"].notna()]
        weights = errors["FP"] * 2 + errors["FN"] + 1
        icl = errors.sample(5, weights=weights, random_state=12345)
        icl['text'] = icl['memo_output']
        return icl[["label", "blob_key", "text", "feedback"]]

    def load_icl(self, prompt_version: str = "") -> List[Message]:
        """Load ground truth labels from evaluation dataset."""
        icl = self.pick_icl(prompt_version)
        
        # Return empty list if no ICL data available
        if icl.empty:
            return []

        examples = []
        for row in icl.itertuples():
            examples.append(Message("user", row.text)) # type: ignore
            dummy = Summary(practically_substantive=Substantive(rating=False, reason=row.feedback)) # type: ignore
            examples.append(Message("assistant", dummy.model_dump_json()))
        return examples


class BriefDataLoader(LabeledDataLoader):
    """Loads brief stage evaluation data (ground truth from labels + predictions from SUMMARY_CLEAN)."""

    def load_eval_labels(self, label_version: str = "", stage: str = "", prompt_version: str = "") -> pd.DataFrame:
        labels = self.load_true_labels(version=label_version, stage=stage)
        icl = self.pick_icl(prompt_version)
        return labels[~labels["blob_key"].isin(icl["blob_key"])]


    def load_true_labels(self, version: str = "", stage: str = "") -> pd.DataFrame:
        """Load ground truth labels for brief evaluation."""
        gold_list = []
        labels = self.load_labels(version, stage)
        for label in labels:
            gold_list.append(label['metadata'] | BriefLabelV3.from_dict(label).model_dump())
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore

        # Add this for easy linking
        gold['blob_key'] = gold['blob_path'].apply(self.storage.parse_blob_path).apply(lambda x: "/".join((x.company, x.policy, x.timestamp)))

        # Change dtypes
        remap_cols = ['practically_substantive_true', 'practically_substantive_pred', 'notes_good']
        for col in remap_cols:
            gold[col] = gold[col].astype('boolean')

        # Drop suggestion columns and simplify names
        gold = gold.drop(columns=[c for c in gold.columns if c.endswith('_pred')])
        gold = gold.rename(columns=lambda c: c.removesuffix('_true'))

        # Drop missing data
        gold = gold.dropna()

        return gold


    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from SUMMARY_CLEAN stage."""
        predictions_df = self._cache_get(Stage.SUMMARY_CLEAN.value)
        if predictions_df is not None:
            return predictions_df
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.SUMMARY_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            parts = self.storage.parse_blob_path(blob)
            blob_key = "/".join((parts.company, parts.policy, parts.timestamp))
            metadata = self.storage.adapter.load_metadata(blob)
            summary = load_validated_json_blob(blob, SUMMARY_MODULE, self.storage)
            summary = cast(Summary, summary)
            rating = 1.0 if summary.practically_substantive.rating else 0.0
            predictions_list.append(metadata | dict(
                blob_path = blob,
                blob_key = blob_key,
                practically_substantive = rating
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        if "AzureWebJobsParentId" in predictions_df.columns:
            predictions_df = predictions_df.drop(columns=["AzureWebJobsParentId"])
        predictions_df = predictions_df.drop_duplicates()
        self._cache_add(Stage.SUMMARY_CLEAN.value, predictions_df)
        return predictions_df


    def load_raw_exists(self) -> pd.DataFrame:
        blobs = self.storage.adapter.list_blobs()
        raw_blobs = [b for b in blobs
                       if b.startswith(Stage.BRIEF_RAW.value)
                       and not b.endswith("latest.txt")]
        meta_list = []
        for blob in raw_blobs:
            metadata = self.storage.adapter.load_metadata(blob)
            parts = self.storage.parse_blob_path(blob)
            blob_key = "/".join((parts.company, parts.policy, parts.timestamp))
            meta_list.append(metadata | dict(blob_path = blob, blob_key = blob_key))
        meta_df = pd.DataFrame.from_records(meta_list)
        if "AzureWebJobsParentId" in meta_df.columns:
            meta_df = meta_df.drop(columns=["AzureWebJobsParentId"])
        meta_df = meta_df.drop_duplicates()
        return meta_df


    def pick_icl(self, prompt_version: str) -> pd.DataFrame:
        labels = self.load_true_labels(stage="brief")
        lineage_groups = ['brief_schema_version', 'brief_prompt_version', 'brief_model_version']
        labels = labels.drop(columns=lineage_groups, errors='ignore')

        y_pred = self.load_pred_labels()
        
        # Return empty DataFrame if no predictions exist
        if y_pred.empty:
            return pd.DataFrame(columns=["label", "blob_key", "text", "feedback"])
        
        y_pred["practically_substantive"] = y_pred["practically_substantive"].astype(bool)
        if prompt_version:
            y_latest = y_pred[y_pred["brief_prompt_version"] == prompt_version]
        else:
            y_latest = y_pred

        merged = labels.merge(y_latest, on="blob_key", suffixes=("_true", "_pred"))
        merged["FP"] = merged["practically_substantive_pred"] & ~merged["practically_substantive_true"]
        merged["FN"] = ~merged["practically_substantive_pred"] & merged["practically_substantive_true"]
        merged["TP"] = merged["practically_substantive_pred"] & merged["practically_substantive_true"]
        merged["TN"] = ~merged["practically_substantive_pred"] & ~merged["practically_substantive_true"]
        merged["label"] = merged[["FP", "FN", "TP", "TN"]].idxmax(axis=1)

        errors = merged[(merged["FP"] | merged["FN"]) & merged["feedback"].notna()]
        weights = errors["FP"] * 2 + errors["FN"] + 1
        if not errors.empty:
            icl = errors.sample(3, weights=weights, random_state=12345)
        else:
            icl = errors.copy()

        diffs = []
        for key in icl['blob_path_true']:
            parts = self.storage.parse_blob_path(key)
            path = self.storage.unparse_blob_path((Stage.DIFF_CLEAN.value, parts.company, parts.policy, parts.timestamp), ".json")
            diffs.append(self.storage.load_text_blob(path))
        icl["text"] = diffs
        return icl[["label", "blob_key", "text", "feedback"]]

    def load_icl(self, prompt_version: str = "") -> List[Message]:
        """Load ground truth labels from evaluation dataset."""
        icl = self.pick_icl(prompt_version)
        
        # Return empty list if no ICL data available
        if icl.empty:
            return []

        examples = []
        formatter = StandardDiffFormatter()
        for row in icl.itertuples():
            doc = DiffDoc.model_validate_json(row.text)  # type: ignore
            examples.append(Message("user", formatter.format_doc(doc)))
            dummy = Brief(memos=[Memo(section_memo="", running_memo=row.feedback)])  # type: ignore
            examples.append(Message("assistant", dummy.model_dump_json()))
        return examples