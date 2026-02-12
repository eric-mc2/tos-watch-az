from abc import abstractmethod, ABC

from pydantic import BaseModel
from schemas.registry import register

VERSION = "v0"
MODULE = "summary"

@register(MODULE, VERSION)
class SummaryBase(BaseModel, ABC):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    @abstractmethod
    def VERSION(cls) -> str:
        return VERSION