from typing import Self, cast

from schemas.summary.v0 import MODULE
from schemas.summary.v2 import Summary as SummaryV2, Substantive
from schemas.registry import register

VERSION = "v4"

@register(MODULE, VERSION)
class Summary(SummaryV2):
    # Basically reverting to non-chunked representation
    pass

    @classmethod
    def merge(cls, a: Self, b: Self) -> Self:
        # TODO: These are being input as BaseClass (the parent class) not Summary.
        a, b = cast(Self, a), cast(Self, b)
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