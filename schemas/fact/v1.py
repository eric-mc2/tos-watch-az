from typing import List, Self
from pydantic import Field

from schemas.fact.v0 import CLAIMS_MODULE, ClaimsBase, FACT_MODULE, FactBase, PROOF_MODULE, ProofBase
from schemas.registry import register

CLAIMS_VERSION = "v1"


@register(CLAIMS_MODULE, CLAIMS_VERSION)
class Claims(ClaimsBase):
    claims: List[str]


FACT_VERSION = "v1"


@register(FACT_MODULE, FACT_VERSION)
class Fact(FactBase):
    claim: str
    veracity: bool
    reason: str


PROOF_VERSION = "v1"


@register(PROOF_MODULE, PROOF_VERSION)
class Proof(ProofBase):
    facts: List[Fact] = Field(..., min_length=1)

    @classmethod
    def merge(cls, a: Self, b: Self) -> Self:
        return cls(facts = a.facts + b.facts)


def merge_facts(a: Fact | Proof, b: Fact | Proof) -> Proof:
    if isinstance(a, Fact) and isinstance(b, Fact):
        return Proof(facts=[a, b])
    elif isinstance(a, Fact) and isinstance(b, Proof):
        return Proof(facts=b.facts + [a])
    elif isinstance(a, Proof) and isinstance(b, Fact):
        return Proof(facts=a.facts + [b])
    elif isinstance(a, Proof) and isinstance(b, Proof):
        return Proof.merge(a, b)
    else:
        raise RuntimeError("Should not get here.")
