from pydantic import Field
from typing import List, Self

from schemas.summary.v0 import SummaryBase, MODULE
from schemas.summary.v2 import Summary as SummaryV2
from schemas.registry import register

VERSION = "v3"

@register(MODULE, VERSION)
class Summary(SummaryBase):
    chunks: List[SummaryV2] = Field(..., min_length=1)

    @classmethod
    def migrate(cls, v2: SummaryV2) -> Self:
        if not isinstance(v2, SummaryV2):
            v2 = SummaryV2.migrate(v2)
        v3 = cls(chunks=[v2])
        return v3

    @classmethod
    def VERSION(cls) -> str:
        return VERSION