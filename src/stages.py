from enum import Enum

class Stage(Enum):
    META = "01-metadata"
    SNAP = "02-snapshots"
    DOCTREE = "03-doctrees"
    DOCCHUNK = "04-doclines"
    DIFF = "05-diffs"
    PROMPT = "06-prompts"
    SUMMARY_RAW = "07-summary-raw"
    SUMMARY_CLEAN = "08-summary-clean"