from schemas.base import SchemaBase
from schemas.registry import register

CLAIMS_MODULE = "claim"
CLAIMS_VERSION = "v0"


@register(CLAIMS_MODULE, CLAIMS_VERSION)
class ClaimsBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return CLAIMS_VERSION


FACT_MODULE = "fact"
FACT_VERSION = "v0"


@register(FACT_MODULE, FACT_VERSION)
class FactBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return FACT_VERSION


PROOF_MODULE = "proof"
PROOF_VERSION = "v0"


@register(PROOF_MODULE, PROOF_VERSION)
class ProofBase(SchemaBase):
    # This is intentionally empty to basically facilitate a union type.
    pass

    @classmethod
    def VERSION(cls) -> str:
        return PROOF_VERSION
