from enum import Enum
from typing import Optional, List


class Stage(Enum):
    SCHEMA = "00-schemas"
    META = "01-metadata"
    SNAP = "02-snapshots"
    DOCTREE = "03-doctrees"
    DOCCHUNK = "04-doclines"
    DIFF_RAW = "05-diffs-raw"
    DIFF_SPAN = "05-diffs-span"
    DIFF_CLEAN = "05-diffs-clean"
    BRIEF_RAW = "06-brief-raw"
    BRIEF_CLEAN = "06-brief-clean"
    SUMMARY_RAW = "07-summary-raw"
    SUMMARY_CLEAN = "08-summary-clean"
    LABELS = "09-labels"
    CLAIM_RAW = "10-claim-raw"
    CLAIM_CLEAN = "11-claim-clean"
    FACTCHECK_RAW = "12-factcheck-raw"
    FACTCHECK_CLEAN = "13-factcheck-clean"
    JUDGE_RAW = "14-judge-raw"
    JUDGE_CLEAN = "15-judge-clean"

    @staticmethod
    def _mapping():
        return {
            Stage.BRIEF_RAW.value: "brief",
            Stage.BRIEF_CLEAN.value: "brief",
            Stage.SUMMARY_RAW.value: "summary",
            Stage.SUMMARY_CLEAN.value: "summary",
            Stage.CLAIM_RAW.value: "claim",
            Stage.CLAIM_CLEAN.value: "claim",
            Stage.FACTCHECK_RAW.value: "proof",
            Stage.FACTCHECK_CLEAN.value: "proof",
            Stage.JUDGE_RAW.value: "judge",
            Stage.JUDGE_CLEAN.value: "judge",
        }
    
    @classmethod
    def get_transform_names(cls) -> List[str]:
        return list(set(cls._mapping().values()))

    @classmethod
    def get_transform_name(cls, stage: str) -> Optional[str]:
        """
        Get the transform name for a given stage.
        
        Args:
            stage: Stage enum value (e.g., Stage.BRIEF_RAW.value)
            
        Returns:
            Transform name (e.g., "briefer") or None if not an LLM stage
        """

        return cls._mapping().get(stage)
