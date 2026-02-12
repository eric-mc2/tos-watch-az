from typing import List

from schemas.llmerror.v0 import LLMErrorBase, MODULE
from schemas.registry import register

VERSION = "v1"

@register(MODULE, VERSION)
class LLMError(LLMErrorBase):
    error: str
    raw: str

    @classmethod
    def VERSION(cls) -> str:
        return VERSION
