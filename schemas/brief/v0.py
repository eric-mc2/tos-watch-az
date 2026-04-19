from typing import cast

from schemas.base import SchemaBase
from schemas.registry import register
from src.stages import Stage

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
BRIEF_MODULE = cast(str, Stage.get_transform_name(Stage.BRIEF_CLEAN.value))

@register(BRIEF_MODULE, BRIEF_VERSION)
class BriefBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return BRIEF_VERSION
