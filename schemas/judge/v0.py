from typing import cast

from schemas.base import SchemaBase
from schemas.registry import register
from src.stages import Stage

VERSION = "v0"
MODULE = cast(str, Stage.get_transform_name(Stage.JUDGE_CLEAN.value))

@register(MODULE, VERSION)
class JudgeBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return VERSION