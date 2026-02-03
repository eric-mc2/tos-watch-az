from schemas.summary.v0 import SummaryBase
from schemas.summary.v2 import Summary as SummaryV2
from typing import List
from schemas.summary.registry import register

VERSION = "v3"

@register(VERSION)
class Summary(SummaryBase):
    chunks: List[SummaryV2]