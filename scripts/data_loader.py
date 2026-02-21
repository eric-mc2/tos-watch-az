from abc import ABC, abstractmethod
from typing import cast

import pandas as pd

from schemas.judge.v0 import MODULE as JUDGE_MODULE
from schemas.judge.v1 import Judgement
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.summary.v1 import Summary

from src.services.blob import BlobService, load_validated_json_blob
from src.stages import Stage
from src.transforms.icl import LabeledDataLoader


class EvalDataLoader(LabeledDataLoader, ABC):
    # TODO: Move out of ICL
    """Base class for loading evaluation data (ground truth + predictions)."""

    def __init__(self, storage: BlobService):
        super().__init__(storage, data_subdir="eval")

    @abstractmethod
    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels from evaluation dataset."""
        pass

    @abstractmethod
    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from blob storage."""
        pass


class SummaryEvalDataLoader(EvalDataLoader):
    # TODO: Move out of ICL
    """Loads summary stage evaluation data (ground truth from labels + predictions from JUDGE_CLEAN)."""

    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels for summary evaluation."""
        gold_list = []
        labels = self._load_cached_labels(self._find_label_file(version))
        for label in labels:
            gold_list.append(label['metadata'] | dict(
                practically_substantive_true=label['responses']['practically_substantive'][0]['value'],
                legally_substantive_true=label['responses']['legally_substantive'][0]['value'],
                practically_substantive_pred=label['suggestions']['practically_substantive']['value'],
                legally_substantive_pred=label['suggestions']['legally_substantive']['value'],
            ))
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore
        remap_cols = ['practically_substantive_true', 'legally_substantive_true',
                      'practically_substantive_pred', 'legally_substantive_pred']
        for col in remap_cols:
            gold[col] = gold[col].map({'True': 1, 'False': 0})
        gold = gold.dropna()
        return gold

    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from JUDGE_CLEAN stage."""
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.JUDGE_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            path = self.storage.parse_blob_path(blob)
            key = self.storage.unparse_blob_path((Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp + ".json"))
            meta = self.storage.adapter.load_metadata(blob)

            summary = cast(Judgement, load_validated_json_blob(blob, JUDGE_MODULE, self.storage))
            rating = summary.practically_substantive.rating
            predictions_list.append(meta | dict(
                blob_path = key,
                practically_substantive = 1.0 if rating else 0.0
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        return predictions_df


class BriefEvalDataLoader(EvalDataLoader):
    """Loads brief stage evaluation data (ground truth from labels + predictions from SUMMARY_CLEAN)."""

    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels for brief evaluation."""
        gold_list = []
        labels = self._load_cached_labels(self._find_label_file(version))
        for label in labels:
            gold_list.append(label['metadata'] | dict(
                practically_substantive_true=label['responses'].get('practically_substantive',[{}])[0].get('value'),
                practically_substantive_pred=label['suggestions']['practically_substantive']['value'],
                notes_good=label['responses'].get('notes_good',[{}])[0].get('value'),
            ))
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore

        # Add this for easy linking
        gold['blob_key'] = gold['blob_path'].apply(self.storage.parse_blob_path).apply(lambda x: "/".join((x.company, x.policy, x.timestamp)))

        # Change dtypes
        remap_cols = ['practically_substantive_true', 'practically_substantive_pred', 'notes_good']
        for col in remap_cols:
            gold[col] = gold[col].map({'True': 1, 'False': 0})

        # Drop suggestion columns and simplify names
        gold = gold.drop(columns=[c for c in gold.columns if c.endswith('_pred')])
        gold = gold.rename(columns=lambda c: c.removesuffix('_true'))

        # Drop missing data
        gold = gold.dropna()

        return gold

    def load_pred_labels(self) -> pd.DataFrame:
        """Load predictions from SUMMARY_CLEAN stage."""
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
