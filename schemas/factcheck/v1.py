from typing import List

from schemas.factcheck.v0 import FactCheckBase, MODULE
from schemas.registry import register

VERSION = "v1"

@register(MODULE, VERSION)
class FactCheck(FactCheckBase):
    claims: List[str]

def hello():
    return __name__