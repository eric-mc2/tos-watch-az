from pydantic import BaseModel
from schemas.registry import register

VERSION = "v0"
MODULE = "factcheck"

@register(MODULE, VERSION)
class FactCheckBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass