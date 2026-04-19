from schemas.base import SchemaBase
from schemas.registry import register

VERSION = "v0"
MODULE = "llmerror"

@register(MODULE, VERSION)
class LLMErrorBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return VERSION