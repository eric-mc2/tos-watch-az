import functools

from pydantic import BaseModel
from schemas.summary.v0 import SummaryBase
from schemas.summary.v1 import (Summary as SummaryV1, Substantive as SubstantiveV1)
from schemas.summary.v2 import (Summary as SummaryV2, Substantive as SubstantiveV2)
from schemas.summary.v3 import Summary as SummaryV3
from schemas.summary.v4 import Summary as SummaryV4

def migrate_v1(v1: SummaryV1) -> SummaryV2:
    ps = v1.practically_substantive
    v2 = SummaryV2(
        practically_substantive=SubstantiveV2(
            rating = ps.rating,
            reason = ps.explanation
        )
    )
    return v2


def migrate_v2(v2: SummaryV2) -> SummaryV3:
    v3 = SummaryV3(
        chunks = [v2]
    )
    return v3


def migrate_v3(v3: SummaryV3) -> SummaryV4:
    def migrate_v2_v4(v2: SummaryV2) -> SummaryV4:
        return SummaryV4(practically_substantive=v2.practically_substantive)
    return functools.reduce(SummaryV4.merge, map(migrate_v2_v4, v3.chunks))

def migrate(model: SummaryBase, version: str) -> SummaryV4:
    if version == "v1":
        assert isinstance(model, SummaryV1)
        v2 = migrate_v1(model)
        return migrate(v2, "v2")
    elif version == "v2":
        assert isinstance(model, SummaryV2)
        v3 = migrate_v2(model)
        return migrate(v3, "v3")
    elif version == "v3":
        assert isinstance(model, SummaryV3)
        v4 = migrate_v3(model)
        return v4
    else:
        assert isinstance(model, SummaryV4)
        return model
