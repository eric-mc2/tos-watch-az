from abc import abstractmethod, ABC
from pydantic import BaseModel


class SchemaBase(BaseModel, ABC):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    @abstractmethod
    def VERSION(cls) -> str:
        pass

