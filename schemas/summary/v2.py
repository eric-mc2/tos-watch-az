from pydantic import BaseModel, Field
from typing import Self
from schemas.registry import register
from schemas.summary.v0 import SummaryBase, MODULE
from schemas.summary.v1 import Summary as SummaryV1

VERSION = "v2"

class Substantive(BaseModel):
    rating: bool
    reason: str = Field(..., min_length=1)

@register(MODULE, VERSION)
class Summary(SummaryBase):
    practically_substantive: Substantive

    @classmethod
    def migrate(cls, v1: SummaryV1) -> Self:
        if not isinstance(v1, SummaryV1):
            raise TypeError(f"Expected SummaryV1, got {type(v1)}")
        ps = v1.practically_substantive
        v2 = cls(
                practically_substantive=Substantive(
                rating=ps.rating,
                reason=ps.explanation
            )
        )
        return v2

    @classmethod
    def VERSION(cls) -> str:
        return VERSION