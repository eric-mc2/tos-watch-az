from pydantic import BaseModel
from schemas.registry import register

VERSION = "v0"
MODULE = "llmerror"

@register(MODULE, VERSION)
class LLMErrorBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass