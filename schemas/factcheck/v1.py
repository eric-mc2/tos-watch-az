from typing import List

from pydantic import BaseModel

from schemas.factcheck.v0 import FactCheckBase, MODULE
from schemas.registry import register

VERSION = "v1"

@register(MODULE, VERSION)
class FactCheck(FactCheckBase):
    claim: str
    veracity: bool
    reason: str
