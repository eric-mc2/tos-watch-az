from pydantic import BaseModel
from schemas.summary.v1 import (Summary as SummaryV1, Substantive as SubstantiveV1)
from schemas.summary.v2 import (Summary as SummaryV2, Substantive as SubstantiveV2)
from schemas.summary.v3 import Summary as SummaryV3

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


def migrate(model: BaseModel, version: str) -> SummaryV3:
    if version == "v1":
        v2 = migrate_v1(model)
        return migrate(v2, "v2")
    elif version == "v2":
        v3 = migrate_v2(model)
        return v3
    else:
        return model