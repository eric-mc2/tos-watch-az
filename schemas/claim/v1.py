from typing import List

from schemas.claim.v0 import ClaimsBase, MODULE
from schemas.registry import register

VERSION = "v1"

@register(MODULE, VERSION)
class Claims(ClaimsBase):
    claims: List[str]