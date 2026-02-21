"""
Compute metrics for different pipeline stages.

This script recomputes metrics from existing outputs in blob storage,
allowing comparison across different prompt/schema versions without re-running prompts.

Usage:
    python -m scripts.compute_metrics --stage summary --label_version summary_v1 --outfile summary_v1_metrics.html
    python -m scripts.compute_metrics --stage brief --label_version brief_v1 --outfile brief_v1_metrics.html
"""
import argparse
import logging

from src.container import ServiceContainer
from src.stages import Stage
from src.utils.app_utils import load_env_vars
from scripts.metrics.summary_metrics import SummaryMetrics
from scripts.metrics.brief_metrics import BriefMetrics
from scripts.metrics.judge_metrics import JudgeMetrics

logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('argilla').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)


def main():
    load_env_vars()
    container = ServiceContainer.create_real()
    
    parser = argparse.ArgumentParser(
        description="Compute metrics for pipeline stages"
    )
    parser.add_argument(
        "--stage",
        choices=[Stage.get_transform_name(Stage.SUMMARY_CLEAN.value),
                 Stage.get_transform_name(Stage.BRIEF_CLEAN.value),
                 Stage.get_transform_name(Stage.JUDGE_CLEAN.value)],
        required=True,
        help="Pipeline stage to evaluate"
    )
    parser.add_argument(
        "--label_version",
        required=True,
        help="Label dataset name (e.g., 'summary_v1', 'brief_v1')"
    )
    parser.add_argument(
        "--outfile",
        required=False,
        help="Output HTML file path relative to data/metrics/"
    )
    args = parser.parse_args()

    if args.outfile is None:
        args.outfile = args.label_version + ".html"
    
    # Select appropriate metrics class
    metrics_classes = {
        Stage.get_transform_name(Stage.SUMMARY_CLEAN.value): SummaryMetrics,
        Stage.get_transform_name(Stage.BRIEF_CLEAN.value): BriefMetrics,
        Stage.get_transform_name(Stage.JUDGE_CLEAN.value): JudgeMetrics,
    }
    
    metrics = metrics_classes[args.stage](container.storage)
    
    try:
        metrics.compute_metrics(args.label_version, args.outfile)
        print(f"Metrics computed successfully. Output: data/metrics/{args.outfile}")
    except Exception as e:
        print(f"Error computing metrics: {e}")
        raise


if __name__ == "__main__":
    main()
