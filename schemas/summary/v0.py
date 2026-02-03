from pydantic import BaseModel
from typing import List
from schemas.summary.registry import register

VERSION = "v0"

@register(VERSION)
class SummaryBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass