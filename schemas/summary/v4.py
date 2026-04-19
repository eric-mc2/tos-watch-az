import functools
from typing import Self

from schemas.summary.v0 import MODULE, SummaryBase
from schemas.summary.v2 import Substantive
from schemas.summary.v3 import Summary as SummaryV3
from schemas.registry import register

VERSION = "v4"

@register(MODULE, VERSION)
class Summary(SummaryBase):
    # Basically reverting to non-chunked representation
    practically_substantive: Substantive

    @classmethod
    def migrate(cls, v3: SummaryV3) -> Self:
        if not isinstance(v3, SummaryV3):
            v3 = SummaryV3.migrate(v3)
        migrated_chunks = (cls(**v2.model_dump()) for v2 in v3.chunks)
        return functools.reduce(cls.merge, migrated_chunks)

    @classmethod
    def merge(cls, a: Self, b: Self) -> Self:
        """Merge two Summary instances by combining substantive assessments."""
        aa, bb = a.practically_substantive, b.practically_substantive
        rating = aa.rating or bb.rating
        positive = (aa.reason if aa.rating else "") + "\n" + (bb.reason if bb.rating else "")
        negative = (aa.reason if not aa.rating else "") + "\n" + (bb.reason if not bb.rating else "")
        reason = positive if rating else negative
        return cls(
            practically_substantive=Substantive(
                rating = rating,
                reason = reason
            )
        )

    @classmethod
    def VERSION(cls) -> str:
        return VERSION