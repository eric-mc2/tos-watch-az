from pydantic import BaseModel
from schemas.registry import register

VERSION = "v0"
MODULE = "claim"

@register(MODULE, VERSION)
class ClaimsBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass