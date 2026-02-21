"""
Base classes for metrics computation across pipeline stages.
"""
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Union

import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
EVALS_DIR = DATA_DIR / "metrics"


class BaseMetrics(ABC):
    """Base class for stage-specific metrics computation."""
    
    @abstractmethod
    def compute_metrics(self, label_version: str, outfile: str) -> None:
        """
        Compute metrics for this stage.
        
        Args:
            label_version: Name of the label dataset (e.g., 'summary_v1')
            outfile: Output HTML file path relative to evals directory
        """
        pass
    
    @staticmethod
    def compute_binary_classification_metrics(
        df: pd.DataFrame, 
        groups: list[str],
        true_col: str,
        pred_col: str
    ) -> pd.DataFrame:
        """
        Compute standard binary classification metrics grouped by specified columns.
        
        Args:
            df: DataFrame with predictions and ground truth
            groups: Column names to group by
            true_col: Name of column with ground truth labels
            pred_col: Name of column with predicted labels
            
        Returns:
            DataFrame with accuracy, precision, recall, f1 for each group
        """
        def compute_metric(x: pd.DataFrame, metric: Callable, name: str) -> pd.Series:
            wrapper = lambda y: metric(y[true_col], y[pred_col])
            return x.groupby(groups).apply(wrapper).rename(name).round(2)  # type: ignore
        
        accuracy = compute_metric(df, accuracy_score, "accuracy")
        precision = compute_metric(df, precision_score, "precision")
        recall = compute_metric(df, recall_score, "recall")
        f1 = compute_metric(df, f1_score, "f1")
        
        return pd.concat([accuracy, precision, recall, f1], axis=1).reset_index()
    
    @staticmethod
    def write_html_report(outfile: str, sections: dict[str, Union[pd.DataFrame, str]]) -> None:
        """
        Write metrics report as HTML.
        
        Args:
            outfile: Output file path relative to evals directory
            sections: Dict mapping section titles to DataFrames or strings
        """
        html_parts = []
        for title, df in sections.items():
            html_parts.append(f"<h1>{title}</h1>")
            if isinstance(df, pd.DataFrame):
                html_parts.append(df.to_html(index=False))
            else:
                html_parts.append(f"<p>{df}</p>")
        
        out_path = os.path.join(EVALS_DIR, outfile)
        out_dir = os.path.dirname(out_path)
        os.makedirs(out_dir, exist_ok=True)
        
        with open(out_path, "w") as f:
            f.write("\n".join(html_parts))
