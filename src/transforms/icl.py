import json
import os
from abc import ABC
from pathlib import Path
from typing import Any, Iterator
import pandas as pd
from collections import OrderedDict
from src.services.blob import BlobService

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


