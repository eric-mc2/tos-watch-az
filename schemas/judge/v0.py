from schemas.base import SchemaBase
from schemas.registry import register

VERSION = "v0"
MODULE = "judge"

@register(MODULE, VERSION)
class JudgeBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return VERSION