"""
Metrics computation for Judge stage (evaluating factcheck integration).

TODO: Implement when judge labels are available.
This would evaluate how well the Judge integrates fact-checking results
into the final verdict.
"""
from scripts.metrics.base import BaseMetrics
from src.services.blob import BlobService


class JudgeMetrics(BaseMetrics):
    """Compute metrics for judge stage."""
    
    def __init__(self, storage: BlobService):
        self.storage = storage
        # TODO: Create JudgeEvalDataLoader when labels exist
    
    def compute_metrics(self, label_version: str, outfile: str) -> None:
        """
        Compute judge stage metrics.
        
        Args:
            label_version: Label dataset name (e.g., 'judge_v1')
            outfile: Output HTML file path relative to evals directory
        """
        raise NotImplementedError("Judge metrics not yet implemented - need labeled data")
