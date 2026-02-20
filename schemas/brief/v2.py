from typing import List, Self

from schemas.brief.v0 import BriefBase, MemoBase, MEMO_MODULE, BRIEF_MODULE
from schemas.registry import register

MEMO_VERSION = "v2"


@register(MEMO_MODULE, MEMO_VERSION)
class Memo(MemoBase):
    section_memo: str
    running_memo: str

    @classmethod
    def VERSION(cls) -> str:
        return MEMO_VERSION


BRIEF_VERSION = "v2"


@register(BRIEF_MODULE, BRIEF_VERSION)
class Brief(BriefBase):
    memos: List[Memo]

    @classmethod
    def VERSION(cls) -> str:
        return BRIEF_VERSION

    @classmethod
    def merge(cls, a: Self, b: Self) -> Self:
        return cls(memos=a.memos + b.memos)


def merge_memos(a: Memo | Brief, b: Memo | Brief) -> Brief:
    if isinstance(a, Memo) and isinstance(b, Memo):
        return Brief(memos=[a, b])
    elif isinstance(a, Memo) and isinstance(b, Brief):
        return Brief(memos=b.memos + [a])
    elif isinstance(a, Brief) and isinstance(b, Memo):
        return Brief(memos=a.memos + [b])
    elif isinstance(a, Brief) and isinstance(b, Brief):
        return Brief.merge(a, b)
    else:
        raise RuntimeError("Should not get here.")
