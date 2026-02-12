from abc import abstractmethod, ABC

from pydantic import BaseModel

from schemas.registry import register

CLAIMS_MODULE = "claim"
CLAIMS_VERSION = "v0"


@register(CLAIMS_MODULE, CLAIMS_VERSION)
class ClaimsBase(BaseModel, ABC):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    @abstractmethod
    def VERSION(cls) -> str:
        return CLAIMS_VERSION


FACT_MODULE = "fact"
FACT_VERSION = "v0"


@register(FACT_MODULE, FACT_VERSION)
class FactBase(BaseModel, ABC):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    @abstractmethod
    def VERSION(cls) -> str:
        return FACT_VERSION


PROOF_MODULE = "proof"
PROOF_VERSION = "v0"


@register(PROOF_MODULE, PROOF_VERSION)
class ProofBase(BaseModel, ABC):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    @abstractmethod
    def VERSION(cls) -> str:
        return PROOF_VERSION
