import argparse
import logging
import random
import time
from pathlib import Path

from src.container import ServiceContainer
from src.stages import Stage
from src.transforms.seeds import STATIC_URLS
from src.utils.app_utils import load_env_vars
from src.transforms.icl import SummaryDataLoader, BriefDataLoader, LabeledDataLoader
from src.utils.path_utils import extract_policy

logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('argilla').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
EVALS_DIR = DATA_DIR / "metrics"


def trigger_random(n: int, stage: str):
    load_env_vars()
    container = ServiceContainer.create_real()
    loader: LabeledDataLoader
    if stage == "brief":
        loader = BriefDataLoader(container.storage)
    else:
        loader = SummaryDataLoader(container.storage)

    exclude = []
    icl = loader.load_labels(stage=stage)
    exclude.extend([x['blob_path'] for x in icl])

    m = 0
    while m < n:
        company = random.choice(list(STATIC_URLS.keys()))
        url = random.choice(STATIC_URLS[company])
        policy = extract_policy(url)
        timestamp = '0'*len(time.strftime("%Y%m%d%H%M%S"))

        # TODO: verify exclusion
        blob_path = container.storage.unparse_blob_path((Stage.DIFF_CLEAN.value, company, policy, timestamp), ".json")
        if blob_path not in exclude:
            trigger_url(url, container, company, policy, timestamp)
            m += 1


def trigger_labels(label_name: str):
    """
    Trigger blobs for all labeled examples so they can be processed through the pipeline.
    """
    load_env_vars()
    container = ServiceContainer.create_real()
    storage = container.storage
    stage = label_name.split("_")[0]

    # Select appropriate data loader
    loaders = {
        Stage.get_transform_name(Stage.SUMMARY_CLEAN.value): SummaryDataLoader,
        Stage.get_transform_name(Stage.BRIEF_CLEAN.value): BriefDataLoader,
    }
    
    loader = loaders[stage](storage)
    labels = loader.load_eval_labels(label_version=label_name, prompt_version="v9") # TODO: dont hardcode
    
    # Touch blobs for all labeled examples
    for record in labels.blob_path:
        parts = storage.parse_blob_path(record)
        diff_name = storage.unparse_blob_path((Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp), ".json")
        if storage.adapter.exists_blob(diff_name):
            # Force recomputation from diff stage
            storage.touch_blobs(Stage.DIFF_RAW.value, parts.company, parts.policy, parts.timestamp)
        else:
            urls = [url for url in STATIC_URLS[parts.company] if extract_policy(url) == parts.policy]
            assert len(urls) == 1, "Expected one url matching this policy."
            trigger_url(urls[0], container, parts.company, parts.policy, parts.timestamp)


def trigger_url(url, container: ServiceContainer, company, policy, timestamp=None):
    snap = container.snapshot_transform
    wayback = container.wayback_transform

    metadata_path = container.storage.unparse_blob_path((Stage.META.value, company, policy, "metadata"), ".json")
    if not container.storage.check_blob(metadata_path):
        # Find original url and recompute from meta stage. (can't re-compute exact snap timestamp because dont have time machine)
        try:
            wayback.scrape_wayback_metadata(url, company)
        except Exception as e:
            return

    # This might not trigger same sample as before, but if it came from wayback we can still find it.
    try:
        metadata = wayback.parse_wayback_metadata(company, policy)
    except Exception as e:
        return

    rows = [row for row in metadata if row['timestamp'] == timestamp]

    if len(rows) == 0:
        print("Timestamp doesn't exist in metadata. Can't recreate from wayback. Skipping: {}/{}/{}".format(
            company, policy, timestamp
        ))
        return

    for row in rows:
        original_url = row['original']
        url_key = f"{timestamp}/{original_url}"
        try:
            snap.get_wayback_snapshot(company, policy, timestamp, url_key)
        except Exception as e:
            continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare storage for running experiments on labeled data"
    )
    sub_parsers = parser.add_subparsers(dest="source")

    rand_parser = sub_parsers.add_parser("from_random")
    rand_parser.add_argument("--n", required=True, type=int)
    rand_parser.add_argument("--stage", required=True)

    label_parser = sub_parsers.add_parser("from_labels")
    label_parser.add_argument("--label_name", required=True,
                        help="Label dataset name (e.g., 'summary_v1', 'brief_v1')")
    args = parser.parse_args()

    if args.source == "from_random":
        trigger_random(args.n, args.stage)
    else:
        trigger_labels(label_name=args.label_name)