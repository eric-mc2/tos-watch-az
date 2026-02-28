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
    data_subdir: str  # 'icl' or 'eval'

    def __init__(self, storage: BlobService, data_subdir: str):
        self.storage = storage
        self.data_subdir = data_subdir

    def load_labels(self, version: str = "", stage: str = ""):
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
                    with open(DATA_DIR / self.data_subdir / version / "records.json") as f:
                        data.extend(json.load(f))
        return data

    def _find_label_file(self, version: str) -> str:
        """Find records.json file for given version in data subdirectory."""
        search_dir = DATA_DIR / self.data_subdir
        for root, dirs, files in os.walk(search_dir):
            if Path(root).name == version:
                return os.path.join(root, "records.json")
        raise FileNotFoundError(f"{version} not found in {search_dir}")

    def find_all_versions(self) -> Iterator[str]:
        """Find all label versions in data subdirectory."""
        search_dir = DATA_DIR / self.data_subdir
        for root, dirs, files in os.walk(search_dir):
            if ".argilla" in root:
                continue
            for name in files:
                if name == "records.json":
                    yield Path(root).name


class ICLDataLoader(LabeledDataLoader):
    """Loader for ICL (few-shot) examples."""
    
    def __init__(self, storage: BlobService):
        super().__init__(storage, data_subdir="icl")

    def choose_examples(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels from evaluation dataset."""
        raise NotImplementedError()

    def load_examples(self, version: str = "") -> pd.DataFrame:
        """Load ground truth labels from evaluation dataset."""
        raise NotImplementedError()


