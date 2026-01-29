from pydantic import BaseModel
from typing import List
from schemas.summary.registry import register

VERSION = "v1"

class Substantive(BaseModel):
    rating: bool
    explanation: str

@register(VERSION)
class Summary(BaseModel):
    legally_substantive: Substantive
    practically_substantive: Substantive
    change_keywords: List[str]
    subject_keywords: List[str]
    helm_keywords: List[str]
