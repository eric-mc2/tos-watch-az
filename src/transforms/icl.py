import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, cast
import pandas as pd
from collections import OrderedDict
from schemas.judge.v0 import MODULE as JUDGE_MODULE
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.judge.v1 import Judgement
from schemas.summary.v1 import Summary
from src.services.blob import BlobService, load_validated_json_blob
from src.stages import Stage


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"


class LabeledDataLoader(ABC):
    """Base class for loading labeled data from disk."""
    
    storage: BlobService
    _cache: OrderedDict[str, Any]
    data_subdir: str  # 'icl' or 'eval'

    def __init__(self, storage: BlobService, data_subdir: str):
        self.storage = storage
        self.data_subdir = data_subdir
        self._cache = OrderedDict()

    def _load_cached_labels(self, filepath: str) -> dict:
        """Load and cache label file contents."""
        if filepath in self._cache:
            self._cache.move_to_end(filepath)
            return self._cache[filepath]
        else:
            with open(filepath) as f:
                labels = json.load(f)
            self._cache[filepath] = labels
            if len(self._cache) > 5:
                self._cache.popitem(last=False)
            return labels

    def _find_label_file(self, version: str) -> str:
        """Find records.json file for given version in data subdirectory."""
        search_dir = DATA_DIR / self.data_subdir
        for root, dirs, files in os.walk(search_dir):
            if Path(root).name == version:
                return os.path.join(root, "records.json")
        raise FileNotFoundError(f"{version} not found in {search_dir}")

    def find_all_labels(self) -> Iterator[str]:
        """Find all label versions in data subdirectory."""
        search_dir = DATA_DIR / self.data_subdir
        for root, dirs, files in os.walk(search_dir):
            if ".argilla" in root:
                continue
            for name in files:
                if name == "records.json":
                    yield Path(root).name


class ICLDataLoader(LabeledDataLoader):
    """Loader for ICL (few-shot) examples. Reserved for training, never used in evals."""
    
    def __init__(self, storage: BlobService):
        super().__init__(storage, data_subdir="icl")

    def load_examples(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels from evaluation dataset."""
        raise NotImplementedError()

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
    # TODO: Move out of ICL
    """Loads brief stage evaluation data (ground truth from labels + predictions from SUMMARY_CLEAN)."""
    
    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels for brief evaluation."""
        gold_list = []
        labels = self._load_cached_labels(self._find_label_file(version))
        for label in labels:
            gold_list.append(label['metadata'] | dict(
                practically_substantive_true=label['responses'].get('practically_substantive',[{}])[0].get('value'),
                practically_substantive_pred=label['suggestions']['practically_substantive']['value'],
                notes_good_true=label['responses'].get('notes_good',[{}])[0].get('value'),
            ))
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore
        remap_cols = ['practically_substantive_true', 'practically_substantive_pred', 'notes_good_true']
        for col in remap_cols:
            gold[col] = gold[col].map({'True': 1, 'False': 0})
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
            diff_name = self.storage.unparse_blob_path((Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp + ".json"))
            metadata = self.storage.adapter.load_metadata(blob)
            summary = load_validated_json_blob(blob, SUMMARY_MODULE, self.storage)
            summary = cast(Summary, summary)
            rating = 1.0 if summary.practically_substantive.rating else 0.0
            predictions_list.append(metadata | dict(
                blob_path = diff_name,
                practically_substantive = rating
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        return predictions_df
    
    def load_raw_exists(self) -> pd.DataFrame:
        blobs = self.storage.adapter.list_blobs()
        raw_blobs = [b for b in blobs
                       if b.startswith(Stage.BRIEF_RAW.value)
                       and not b.endswith("latest.json")]
        meta_list = []
        for blob in raw_blobs:
            parts = self.storage.parse_blob_path(blob)
            diff_name = self.storage.unparse_blob_path((Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp + ".json"))
            metadata = self.storage.adapter.load_metadata(blob)
            blob_key = diff_name.removeprefix(f"{Stage.DIFF_RAW.value}/")
            meta_list.append(metadata | dict(blob_path = diff_name, blob_key = blob_key))
        meta_df = pd.DataFrame.from_records(meta_list)
        return meta_df