from enum import Enum

class Stage(Enum):
    SCHEMA = "00-schemas"
    META = "01-metadata"
    SNAP = "02-snapshots"
    DOCTREE = "03-doctrees"
    DOCCHUNK = "04-doclines"
    DIFF_RAW = "05-diffs-raw"
    DIFF_CLEAN = "05-diffs-clean"
    PROMPT = "06-prompts"
    SUMMARY_RAW = "07-summary-raw"
    SUMMARY_CLEAN = "08-summary-clean"
    LABELS = "09-labels"