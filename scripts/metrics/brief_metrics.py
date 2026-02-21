"""
Metrics computation for Brief stage (evaluating briefer output via Summarizer).
"""
from scripts.metrics.base import BaseMetrics
from scripts.data_loader import BriefEvalDataLoader
from src.services.blob import BlobService


class BriefMetrics(BaseMetrics):
    """Compute metrics for brief stage using SUMMARY_CLEAN predictions."""
    
    def __init__(self, storage: BlobService):
        self.storage = storage
        self.loader = BriefEvalDataLoader(storage)
    
    def compute_metrics(self, label_version: str, outfile: str) -> None:
        """
        Compute brief stage metrics.
        
        Args:
            label_version: Label dataset name (e.g., 'brief_v1')
            outfile: Output HTML file path relative to evals directory
        """
        # Load ground truth and predictions
        gold = self.loader.load_true_labels(label_version)
        pred = self.loader.load_pred_labels()
        raw = self.loader.load_raw_exists()

        brief_groups = ['brief_schema_version', 'brief_prompt_version','brief_model_version']
        summary_groups = ['summary_schema_version', 'summary_prompt_version','summary_model_version']
        lineage_groups = brief_groups + summary_groups

        # Filter to stuff we care about
        pred = pred[pred['blob_key'].isin(gold['blob_key'])]
        raw = raw[raw['blob_key'].isin(gold['blob_key'])]

        # Also dont care about gold label provenance
        gold = gold.drop(columns=lineage_groups)

        if pred.empty:
            print("No llm output for these labels.")
            return


        # How many blobs did we start with.
        n_diffs = gold['blob_path'].nunique()

        # How many did we process (e.g. not blocked by circuit)
        n_processed = raw.groupby(brief_groups).size().rename('N')

        # How many made it through without structural errors?
        n_valid = pred.groupby(lineage_groups).size().rename('N')

        pct_valid = (n_valid / n_processed).rename("Pct").round(2)

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
