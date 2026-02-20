import json
import os
from pathlib import Path
from typing import Any, Iterator, cast
import pandas as pd
from collections import OrderedDict
from schemas.registry import load_data
from schemas.judge.v0 import MODULE as JUDGE_MODULE
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.judge.v1 import Judgement
from schemas.summary.v1 import Summary
from src.services.blob import BlobService
from src.stages import Stage


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"


class ICL:
    storage: BlobService
    _cache: OrderedDict[str, Any]

    def __init__(self, storage: BlobService):
        self.storage = storage
        self._cache = OrderedDict()


    def load_pred_labels(self) -> pd.DataFrame:
        pass




    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        pass


    def _load_cached_labels(self, filepath: str) -> dict:
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
        for root, dirs, files in os.walk(DATA_DIR):
            if Path(root).name == version:
                return os.path.join(root, "records.json")
        raise FileNotFoundError(version)


    @staticmethod
    def find_all_labels() -> Iterator[str]:
        for root, dirs, files in os.walk(DATA_DIR):
            if ".argilla" in root:
                continue
            for name in files:
                if name == "records.json":
                    yield Path(root).name

class SummaryICL(ICL):
    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        if not version:
            gold_dfs = [self.load_true_labels(f) for f in self.find_all_labels()]
            return pd.concat(gold_dfs)

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
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.JUDGE_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            path = self.storage.parse_blob_path(blob)
            key = self.storage.unparse_blob_path((Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp + ".json"))
            meta = self.storage.adapter.load_metadata(blob)

            summary = load_data(blob, JUDGE_MODULE, self.storage)
            cast(Judgement, summary)
            rating = summary.practically_substantive.rating
            predictions_list.append(meta | dict(
                blob_path = key,
                practically_substantive = 1.0 if rating else 0.0
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        return predictions_df


class BriefICL(ICL):
    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        if not version:
            gold_dfs = [self.load_true_labels(f) for f in self.find_all_labels()]
            return pd.concat(gold_dfs)

        gold_list = []
        labels = self._load_cached_labels(self._find_label_file(version))
        for label in labels:
            gold_list.append(label['metadata'] | dict(
                practically_substantive_true=label['responses'].get('practically_substantive',[{}])[0].get('value'),
                practically_substantive_pred=label['suggestions']['practically_substantive']['value'],
            ))
        gold = pd.DataFrame.from_records(gold_list)  # type: ignore
        remap_cols = ['practically_substantive_true', 'practically_substantive_pred']
        for col in remap_cols:
            gold[col] = gold[col].map({'True': 1, 'False': 0})
        gold = gold.dropna()
        return gold


    def load_pred_labels(self) -> pd.DataFrame:
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.SUMMARY_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            parts = self.storage.parse_blob_path(blob)
            key = self.storage.unparse_blob_path((Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp + ".json"))
            meta = self.storage.adapter.load_metadata(blob)

            summary = load_data(blob, SUMMARY_MODULE, self.storage)
            cast(Summary, summary)
            rating = 1.0 if summary.practically_substantive.rating else 0.0
            predictions_list.append(meta | dict(
                blob_path = key,
                practically_substantive = rating
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        return predictions_df
