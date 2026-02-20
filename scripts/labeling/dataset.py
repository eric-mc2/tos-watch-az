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

    def get_data(self, name: str):
        dataset = self.client.datasets(name)
        if dataset is None:
            print(f"Dataset {name} does not exist yet!")
            return
        data_dir = DATA_DIR / name
        if os.path.exists(data_dir):
            print("Dataset already downloaded. Archive or delete it. Then re-run.")
            return
        os.makedirs(data_dir)
        dataset.to_disk(str(data_dir))
