import json
import os
from pathlib import Path
from typing import Any, Iterator
import pandas as pd
from collections import OrderedDict
from schemas.registry import load_data
from schemas.summary.v0 import MODULE
from schemas.summary.v4 import Summary as SummaryV4
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
        blobs = self.storage.adapter.list_blobs()
        clean_blobs = [b for b in blobs
                       if b.startswith(Stage.SUMMARY_CLEAN.value)
                       and not b.endswith("latest.json")]
        predictions_list = []
        for blob in clean_blobs:
            path = self.storage.parse_blob_path(blob)
            key = self.storage.unparse_blob_path((Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp + ".json"))
            meta = self.storage.adapter.load_metadata(blob)

            summary = load_data(blob, MODULE, self.storage)
            assert isinstance(summary, SummaryV4)
            rating = summary.practically_substantive.rating
            predictions_list.append(meta | dict(
                blob_path = key,
                practically_substantive = 1.0 if rating else 0.0
            ))
        predictions_df = pd.DataFrame.from_records(predictions_list)
        return predictions_df


    def load_true_labels(self, version: str = "") -> pd.DataFrame:
        if not version:
            gold_dfs = [self.load_true_labels(f) for f in self._find_all_labels()]
            return pd.concat(gold_dfs)
        else:
            gold_list = []
            labels = self._load_cached_labels(self._find_label_file(version))
            for label in labels:
                gold_list.append(label['metadata'] | dict(
                    practically_substantive_true = label['responses']['practically_substantive'][0]['value'],
                    legally_substantive_true = label['responses']['legally_substantive'][0]['value'],
                    practically_substantive_pred = label['suggestions']['practically_substantive']['value'],
                    legally_substantive_pred = label['suggestions']['legally_substantive']['value'],
                ))
            gold = pd.DataFrame.from_records(gold_list)  # type: ignore
            remap_cols = ['practically_substantive_true', 'legally_substantive_true',
                          'practically_substantive_pred', 'legally_substantive_pred']
            for col in remap_cols:
                gold[col] = gold[col].map({'True':1,'False':0})
            gold = gold.dropna()
            return gold


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
            for name in files:
                if name == f"{version}.json":
                    return os.path.join(DATA_DIR, root, name)
        raise FileNotFoundError(version)


    def _find_all_labels(self) -> Iterator[str]:
        for root, dirs, files in os.walk(DATA_DIR):
            if ".argilla" in root:
                continue
            for name in files:
                if name.endswith(".json"):
                    yield Path(name).stem
