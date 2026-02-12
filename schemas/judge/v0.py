from abc import abstractmethod, ABC

from pydantic import BaseModel
from schemas.registry import register

VERSION = "v0"
MODULE = "judge"

@register(MODULE, VERSION)
class JudgeBase(BaseModel, ABC):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    @abstractmethod
    def VERSION(cls) -> str:
        return VERSION