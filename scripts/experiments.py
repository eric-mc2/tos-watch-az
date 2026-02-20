import argparse
import logging
from pathlib import Path

from src.container import ServiceContainer
from src.stages import Stage
from src.utils.app_utils import load_env_vars
from src.transforms.icl import ICL

logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('argilla').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
EVALS_DIR = DATA_DIR / "evals"


def main(storage, label_name):
    icl = ICL(storage)
    labels = icl.load_true_labels(label_name)
    for record in labels.blob_path:
        path = storage.parse_blob_path(record)
        storage.touch_blobs(Stage.DIFF_RAW.value, path.company, path.policy, path.timestamp)


if __name__ == "__main__":
    load_env_vars()
    container = ServiceContainer.create_real()

    parser = argparse.ArgumentParser()
    parser.add_argument("--label_name", required=True)
    args = parser.parse_args()

    main(label_name=args.label_name, storage=container.storage)