import logging
from dataclasses import dataclass
from typing import Iterator

from schemas.registry import load_data
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.summary.v4 import Summary as SummaryV4
from schemas.fact.v0 import PROOF_MODULE
from schemas.fact.v1 import Fact, Proof
from schemas.judge.v1 import VERSION as JUDGE_SCHEMA_VERSION, MODULE as JUDGE_MODULE
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v1"
N_ICL = 3
SYSTEM_PROMPT = """
You are part of a team that is analyzing terms of service changes.
The team's goal is to determine whether changes are practically substantive—meaning 
they materially affect what a typical user can do, must do, or what happens to them.

CRITERIA FOR PRACTICALLY SUBSTANTIVE:
- Alters data collection/usage
- Changes user permissions, restrictions, or account termination conditions
- Modifies pricing/payments/refunds
- Affects dispute resolution or liability
- New requirements or prohibitions on user behavior

NOT PRACTICALLY SUBSTANTIVE:
- Reformatting or reorganization only
- Clarifies language without changing meaning
- Administrative updates (names, addresses, dates)
- Typo or grammar fixes
- Adds legally required boilerplate that doesn't change user experience

Your role is the final judge. Your task is to take the initial analysis,
the fact-checked claims, and check whether the reasoning follows from
the factual claims. In other words, given that the analysis was proven
or disproven, does the original conclusion still hold? What is the 
final conclusion?

You should respond with valid JSON:
OUTPUT FORMAT:
{
  "practically_substantive" : 
  {
    "rating": boolean,
    "reason": "One or two sentences explaining the key factor"
  }
}
  
"""


@dataclass
class JudgeBuilder:
    storage: BlobService

    def build_prompt(self, facts_blob_name: str, summary_blob_name: str) -> Iterator[PromptMessages]:
        examples: list = [] # self.read_examples()

        # Get Summary
        summary = load_data(summary_blob_name, SUMMARY_MODULE, self.storage)
        assert isinstance(summary, SummaryV4)

        # Get Facts
        facts = load_data(facts_blob_name, PROOF_MODULE, self.storage)
        assert isinstance(facts, Fact) or isinstance(facts, Proof)
        facts = facts if isinstance(facts, Proof) else Proof(facts=[facts])

        # Build Prompt - pass the full summary structure
        prompt_msg = Message("user", self._format_prompt(summary, facts))
        yield PromptMessages(system=SYSTEM_PROMPT,
                             history=examples,
                             current=prompt_msg)


    @classmethod
    def _format_prompt(cls, summary: SummaryV4, facts: Proof):
        plaintext_summary = cls._format_summary(summary)
        plaintext_facts = cls._format_proof(facts)
        formatted = [plaintext_summary,
                      "Fact-Checking:",
                      plaintext_facts]
        return "\n".join(formatted)


    @classmethod
    def _format_summary(cls, summary: SummaryV4):
        formatted = ["Preliminary analysis:",
                     "Is the change practically substantive?",
                     str(summary.practically_substantive.rating),
                     "Reasoning:",
                      summary.practically_substantive.reason]
        return "\n".join(formatted)

    @classmethod
    def _format_fact(cls, fact: Fact) -> str:
        formatted = [f"Claim: {fact.claim}",
                    f"Veracity: {fact.veracity}",
                    f"Reason: {fact.reason}"]
        return "\n".join(formatted)

    @classmethod
    def _format_proof(cls, facts: Proof) -> str:
        formatted = [f"Case {i}:\n{cls._format_fact(x)}"
                    for i,x in enumerate(facts.facts, start=1)]
        return "\n".join(formatted)

@dataclass
class Judge:
    storage: BlobService
    executor: LLMTransform

    def judge(self, facts_blob_name: str, summary_blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Judging {summary_blob_name}")
        prompter = JudgeBuilder(self.storage)
        messages = prompter.build_prompt(facts_blob_name, summary_blob_name)
        return self.executor.execute_prompts(messages, JUDGE_MODULE, JUDGE_SCHEMA_VERSION, PROMPT_VERSION)

