"""
Metrics computation for Summary stage (evaluating end-to-end pipeline via Judge output).
"""
import os
import pandas as pd

from scripts.metrics.base import BaseMetrics
from src.transforms.icl import SummaryDataLoader
from src.services.blob import BlobService


class SummaryMetrics(BaseMetrics):
    """Compute metrics for summary stage using JUDGE_CLEAN predictions."""
    
    def __init__(self, storage: BlobService):
        self.storage = storage
        self.loader = SummaryDataLoader(storage)
    
    def compute_metrics(self, label_version: str, stage: str, outfile: str) -> None:
        """
        Compute summary stage metrics.
        
        Args:
            label_version: Label dataset name (e.g., 'summary_v1')
            outfile: Output HTML file path relative to evals directory
        """
        # Load ground truth and predictions
        gold = self.loader.load_all_eval_labels()
        pred = self.loader.load_pred_labels()
        raw = self.loader.load_raw_exists()

        summary_groups = ['summary_schema_version', 'summary_prompt_version', 'summary_model_version']

        # Filter to stuff we care about
        pred = pred[pred['blob_key'].isin(gold['blob_key'])]
        raw = raw[raw['blob_key'].isin(gold['blob_key'])]

        # Also dont care about gold label provenance
        gold = gold.drop(columns=summary_groups, errors='ignore')

        if pred.empty:
            print("No llm output for these labels.")
            return

        # How many blobs did we start with.
        n_diffs = gold['blob_path'].nunique()

        # How many did we process (e.g. not blocked by circuit)
        n_processed = raw.groupby(summary_groups)['blob_key'].agg(['count','nunique'])

        # How many made it through without structural errors?
        n_valid = pred.groupby(summary_groups)['blob_key'].agg(['count','nunique'])

        pct_valid = (n_valid['count'] / n_processed['count']).rename("Pct").round(2)

        table = gold.merge(pred, on="blob_key", how="inner", suffixes=("_true", "_pred"))

        summary_metrics = self.compute_binary_classification_metrics(
            table,
            summary_groups,
            "practically_substantive_true",
            "practically_substantive_pred"
        )

        # Generate HTML report
        sections = {
            'N_LABELED_BLOBS': str(n_diffs),
            'N_PROCESSED_BLOBS': n_processed.reset_index(),
            'N_VALID_BLOBS': n_valid.reset_index(),
            'PCT_VALID_BLOBS': pct_valid.reset_index(),
            "SUMMARY_METRICS": summary_metrics,
        }
        self.write_html_report(outfile, sections)  # type: ignore
