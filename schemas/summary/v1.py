from pydantic import BaseModel
from typing import List

class Substantive(BaseModel):
    rating: bool
    explanation: str

class Summary(BaseModel):
    legally_substantive: Substantive
    practically_substantive: Substantive
    change_keywords: List[str]
    subject_keywords: List[str]
    helm_keywords: List[str]
    