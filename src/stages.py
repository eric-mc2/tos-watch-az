from enum import Enum

class Stage(Enum):
    SNAP = "01-snapshots"
    DOCTREE = "02-doctrees"
    DOCCHUNK = "03-doclines"
    DIFF = "04-diffs"
    PROMPT = "05-prompts"
    SUMMARY_RAW = "06-summary-raw"
    SUMMARY_CLEAN = "07-summary-clean"