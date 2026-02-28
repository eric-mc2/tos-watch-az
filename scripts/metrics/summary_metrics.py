"""
Metrics computation for Summary stage (evaluating end-to-end pipeline via Judge output).
"""
import os
import pandas as pd

from scripts.metrics.base import BaseMetrics
from scripts.data_loader import SummaryEvalDataLoader
from src.services.blob import BlobService


class SummaryMetrics(BaseMetrics):
    """Compute metrics for summary stage using JUDGE_CLEAN predictions."""
    
    def __init__(self, storage: BlobService):
        self.storage = storage
        self.loader = SummaryEvalDataLoader(storage)
    
    def compute_metrics(self, label_version: str, stage: str, outfile: str) -> None:
        """
        Compute summary stage metrics.
        
        Args:
            label_version: Label dataset name (e.g., 'summary_v1')
            outfile: Output HTML file path relative to evals directory
        """
        # Load ground truth and predictions
        gold = self.loader.load_true_labels(label_version)
        pred = self.loader.load_pred_labels()
        
        # Drop suggestion columns and rename true labels
        cols_to_drop = ['practically_substantive_pred', 'legally_substantive_pred']
        cols_to_drop = [c for c in cols_to_drop if c in gold.columns]
        gold = (gold
                .drop(columns=cols_to_drop)
                .rename(columns={
                    'practically_substantive_true': 'practically_substantive',
                    'legally_substantive_true': 'legally_substantive'
                }))
        
        # Normalize blob paths for joining
        gold['blob_path'] = gold['blob_path'].apply(self.storage.parse_blob_path)
        gold['blob_path'] = gold['blob_path'].apply(
            lambda x: os.path.join(x.company, x.policy, x.timestamp)
        )
        pred['blob_path'] = pred['blob_path'].apply(self.storage.parse_blob_path)
        pred['blob_path'] = pred['blob_path'].apply(
            lambda x: os.path.join(x.company, x.policy, x.timestamp)
        )
        
        # Merge gold and predictions
        compare = gold.merge(pred, on=["blob_path"], how="left", suffixes=("_true", "_pred"))
        
        # Compute totals
        totals = (compare.groupby(['model_version'])['blob_path']
                  .nunique()
                  .rename('n')
                  .reset_index())
        
        # Group by relevant version columns
        groups = ['model_version', 'prompt_version_pred', 'schema_version_true']
        
        # Valid JSON percentage (has run_id)
        valid_json = (compare[compare['run_id'].notna()]
                      .groupby(groups)['run_id']
                      .nunique()
                      .rename('m')
                      .reset_index())
        
        if valid_json.empty:
            print(f"No valid predictions found for {label_version}")
            return
        
        valid_json_pct = totals.merge(valid_json)
        valid_json_pct['pct'] = (valid_json_pct['m'] / valid_json_pct['n']).round(2)
        valid_json_pct = valid_json_pct[groups + ['pct']]
        
        # Valid schema percentage (all records in compare)
        valid_schema = compare.groupby(groups).size().rename('m').reset_index()
        valid_schema_pct = totals.merge(valid_schema)
        valid_schema_pct['pct'] = (valid_schema_pct['m'] / valid_schema_pct['n']).round(2)
        valid_schema_pct = valid_schema_pct[groups + ['pct']]
        
        # Semantic metrics (binary classification)
        confusion = compare.copy()
        if confusion.empty:
            print(f"No confusion matrix data for {label_version}")
            return
        
        semantic = self.compute_binary_classification_metrics(
            confusion,
            groups=groups,
            true_col='practically_substantive_true',
            pred_col='practically_substantive_pred'
        )
        
        # Generate HTML report
        # TODO: undo str conversion
        sections = {
            'TOTALS': str(totals),
            'VALID JSON': str(valid_json_pct),
            'VALID SCHEMA': str(valid_schema_pct),
            'CONFUSION MATRIX': str(semantic),
            'SCHEMA': ', '.join(compare.columns.to_list())
        }
        
        self.write_html_report(outfile, sections) # type: ignore
