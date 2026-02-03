from pydantic import BaseModel
from schemas.summary.registry import register
from schemas.summary.v0 import SummaryBase

VERSION = "v2"

class Substantive(BaseModel):
    rating: bool
    reason: str

@register(VERSION)
class Summary(SummaryBase):
    practically_substantive: Substantive