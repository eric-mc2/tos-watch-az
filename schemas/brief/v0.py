from schemas.base import SchemaBase
from schemas.registry import register

MEMO_VERSION = "v0"
MEMO_MODULE = "memo"

@register(MEMO_MODULE, MEMO_VERSION)
class MemoBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return MEMO_VERSION


BRIEF_VERSION = "v0"
BRIEF_MODULE = "brief"

@register(BRIEF_MODULE, BRIEF_VERSION)
class BriefBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return BRIEF_VERSION
