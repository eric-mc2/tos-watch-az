from pydantic import BaseModel
from schemas.registry import register

VERSION = "v0"
MODULE = "judge"

@register(MODULE, VERSION)
class JudgeBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass