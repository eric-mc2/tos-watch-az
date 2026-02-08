from schemas.summary.v0 import SummaryBase, MODULE
from schemas.summary.v2 import Summary as SummaryV2
from typing import List
from schemas.registry import register

VERSION = "v3"

@register(MODULE, VERSION)
class Summary(SummaryBase):
    chunks: List[SummaryV2]