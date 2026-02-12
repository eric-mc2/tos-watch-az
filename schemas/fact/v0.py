from pydantic import BaseModel

from schemas.registry import register

CLAIMS_MODULE = "claim"
CLAIMS_VERSION = "v0"


@register(CLAIMS_MODULE, CLAIMS_VERSION)
class ClaimsBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass


FACT_MODULE = "fact"
FACT_VERSION = "v0"


@register(FACT_MODULE, FACT_VERSION)
class FactBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass


PROOF_MODULE = "proof"
PROOF_VERSION = "v0"


@register(PROOF_MODULE, PROOF_VERSION)
class ProofBase(BaseModel):
    # This is intentionally empty to basically facilitate a union type.
    pass
