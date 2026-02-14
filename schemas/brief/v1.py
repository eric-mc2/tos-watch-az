from typing import List

from schemas.brief.v0 import BriefBase, MemoBase, MEMO_MODULE, BRIEF_MODULE
from schemas.registry import register

MEMO_VERSION = "v1"

@register(MEMO_MODULE, MEMO_VERSION)
class Memo(MemoBase):
    relevance_flag: bool
    section_memo: str
    running_memo: str

    @classmethod
    def VERSION(cls) -> str:
        return MEMO_VERSION


BRIEF_VERSION = "v1"

@register(BRIEF_MODULE, BRIEF_VERSION)
class Brief(BriefBase):
    memos : List[Memo]

    @classmethod
    def VERSION(cls) -> str:
        return BRIEF_VERSION
