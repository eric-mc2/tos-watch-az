from pydantic import BaseModel
from typing import List
from schemas.summary.registry import register
from schemas.summary.v0 import SummaryBase

VERSION = "v1"

class Substantive(BaseModel):
    rating: bool
    explanation: str

@register(VERSION)
class Summary(SummaryBase):
    legally_substantive: Substantive
    practically_substantive: Substantive
    change_keywords: List[str]
    subject_keywords: List[str]
    helm_keywords: List[str]
