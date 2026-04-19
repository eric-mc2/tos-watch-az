"""
Metrics computation for Judge stage (evaluating factcheck integration).

This would evaluate how well the Judge integrates fact-checking results
into the final verdict.
"""
from scripts.metrics.base import BaseMetrics
from src.services.blob import BlobService
from src.transforms.icl import JudgeDataLoader


class JudgeMetrics(BaseMetrics):
    """Compute metrics for judge stage."""
    
    def __init__(self, storage: BlobService):
        self.storage = storage
        self.loader = JudgeDataLoader(storage)

    def compute_metrics(self, label_version: str, stage: str, outfile: str) -> None:
        """
        Compute judge stage metrics.
        
        Args:
            label_version: Label dataset name (e.g., 'judge_v1')
            outfile: Output HTML file path relative to evals directory
        """
        # Load ground truth and predictions
        gold = self.loader.load_all_eval_labels()
        pred = self.loader.load_pred_labels()
        raw = self.loader.load_raw_exists()

        summary_groups = ['summary_schema_version', 'summary_prompt_version', 'summary_model_version']
        judge_groups = ['judge_schema_version', 'judge_prompt_version', 'judge_model_version']
        lineage_groups = summary_groups + judge_groups

        # Filter to stuff we care about
        pred = pred[pred['blob_key'].isin(gold['blob_key'])]
        raw = raw[raw['blob_key'].isin(gold['blob_key'])]

        # Also dont care about gold label provenance
        gold = gold.drop(columns=lineage_groups, errors='ignore')

        if pred.empty:
            print("No llm output for these labels.")
            return

        # How many blobs did we start with.
        n_diffs = gold['blob_path'].nunique()

        # How many did we process (e.g. not blocked by circuit)
        n_processed = raw.groupby(judge_groups)["blob_key"].agg(["count", "nunique"])

        # How many made it through without structural errors?
        n_valid = pred.groupby(judge_groups)["blob_key"].agg(["count", "nunique"])

        pct_valid = (n_valid['count'] / n_processed['count']).rename("Pct").round(2)

        table = gold.merge(pred, on="blob_key", how="inner", suffixes=("_true", "_pred"))

        summary_metrics = self.compute_binary_classification_metrics(
            table,
            lineage_groups,
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



