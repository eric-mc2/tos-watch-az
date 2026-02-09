from pydantic import BaseModel, Field
from schemas.registry import register
from schemas.summary.v0 import SummaryBase, MODULE

VERSION = "v2"

class Substantive(BaseModel):
    rating: bool
    reason: str = Field(..., min_length=1)

@register(MODULE, VERSION)
class Summary(SummaryBase):
    practically_substantive: Substantive