from schemas.base import SchemaBase
from schemas.registry import register

VERSION = "v0"
MODULE = "summary"

@register(MODULE, VERSION)
class SummaryBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return VERSION