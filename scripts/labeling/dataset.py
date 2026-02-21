import os
from pathlib import Path

import argilla as rg  # type: ignore

from src.container import ServiceContainer

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"


class DatasetBase:
    client: rg.Argilla
    container: ServiceContainer

    def __init__(self):
        self.client = rg.Argilla(
            api_url="https://eric-mc22-tos-watch-ft.hf.space",
            api_key=os.environ['ARGILLA_API_KEY'], # note: this resets when the HF space goes inactive!
            headers={"Authorization": f"Bearer {os.environ['HF_TOKEN']}"}
        )
        self.container = ServiceContainer.create_real()


    def create_dataset(self, name):
        pass

    def create_records(self, dataset, schema_version, prompt_version, max_examples=10):
        pass

    def get_data(self, name: str, split: str = "eval"):
        """
        Download dataset from Argilla to local disk.
        
        Args:
            name: Dataset name (e.g., 'summary_v1', 'brief_v1')
            split: Data split - 'icl' for training examples or 'eval' for evaluation
        
        Note:
            TODO: Implement automatic split logic based on metrics optimization.
            For now, manually specify split when downloading. The split determines
            whether data is reserved for ICL examples (never used in evals) or
            available for evaluation.
        """
        dataset = self.client.datasets(name)
        if dataset is None:
            print(f"Dataset {name} does not exist yet!")
            return
        
        data_dir = DATA_DIR / split / name
        if os.path.exists(data_dir):
            print(f"Dataset already downloaded to {split}/. Archive or delete it. Then re-run.")
            return
        
        os.makedirs(data_dir, exist_ok=True)
        dataset.to_disk(str(data_dir))
        print(f"Downloaded {name} to {split}/ directory")
