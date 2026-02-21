import argparse
import logging
import time
from pathlib import Path

from src.container import ServiceContainer
from src.stages import Stage
from src.transforms.seeds import STATIC_URLS
from src.utils.app_utils import load_env_vars
from scripts.data_loader import SummaryEvalDataLoader, BriefEvalDataLoader
from src.utils.path_utils import extract_policy

logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('argilla').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
EVALS_DIR = DATA_DIR / "metrics"


def main(label_name: str, stage: str):
    """
    Prepare blob storage for running experiments on labeled data.
    
    This script creates empty blobs for all labeled examples so they can be
    processed through the pipeline.
    
    Args:
        storage: Blob storage service
        label_name: Label dataset name (e.g., 'summary_v1', 'brief_v1')
        stage: Pipeline stage being evaluated ('summary' or 'brief')
    """
    load_env_vars()
    container = ServiceContainer.create_real()
    storage = container.storage
    snap = container.snapshot_transform
    wayback = container.wayback_transform

    # Select appropriate data loader
    loaders = {
        Stage.get_transform_name(Stage.SUMMARY_CLEAN.value): SummaryEvalDataLoader,
        Stage.get_transform_name(Stage.BRIEF_CLEAN.value): BriefEvalDataLoader,
    }
    
    loader = loaders[stage](storage)
    labels = loader.load_true_labels(label_name)
    
    # Touch blobs for all labeled examples
    for record in labels.blob_path:
        parts = storage.parse_blob_path(record)
        diff_name = storage.unparse_blob_path((Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp), ".json")
        if storage.adapter.exists_blob(diff_name):
            # Force recomputation from diff stage
            storage.touch_blobs(Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp)
        else:
            # Find original url and recompute from meta stage. (can't re-compute exact snap timestamp because dont have time machine)
            urls = [url for url in STATIC_URLS[parts.company] if extract_policy(url) == parts.policy]
            assert len(urls) == 1, "Expected one url matching this policy."
            try:
                wayback.scrape_wayback_metadata(urls[0], parts.company)
            except Exception as e:
                continue
            # This might not trigger same sample as before, but if it came from wayback we can still find it.
            try:
                metadata = wayback.parse_wayback_metadata(parts.company, parts.policy)
            except Exception as e:
                continue
            rows = [row for row in metadata if row['timestamp'] == parts.timestamp]
            for row in rows:
                original_url = row['original']
                url_key = f"{parts.timestamp}/{original_url}"
                try:
                    snap.get_wayback_snapshot(parts.company, parts.policy, parts.timestamp, url_key)
                except Exception as e:
                    continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare storage for running experiments on labeled data"
    )
    parser.add_argument("--label_name", required=True,
                        help="Label dataset name (e.g., 'summary_v1', 'brief_v1')")
    parser.add_argument("--stage",
                        choices=[Stage.get_transform_name(Stage.SUMMARY_CLEAN.value),
                                 Stage.get_transform_name(Stage.BRIEF_CLEAN.value)],
                        default=Stage.get_transform_name(Stage.SUMMARY_CLEAN.value),
                        help="Pipeline stage being evaluated")
    args = parser.parse_args()

    main(label_name=args.label_name, stage=args.stage)