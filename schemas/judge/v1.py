from pydantic import BaseModel
from schemas.judge.v0 import JudgeBase, MODULE
from schemas.registry import register

VERSION = "v1"

class Substantive(BaseModel):
    rating: bool
    reason: str

@register(MODULE, VERSION)
class Judgement(JudgeBase):
    practically_substantive: Substantive