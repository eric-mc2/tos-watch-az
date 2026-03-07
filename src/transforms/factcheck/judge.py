import logging
from dataclasses import dataclass
from typing import Iterator

from schemas.brief.v0 import BRIEF_MODULE
from schemas.brief.v2 import Brief, Memo
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.summary.v4 import Summary as SummaryV4, Summary
from schemas.fact.v0 import PROOF_MODULE
from schemas.fact.v1 import Fact, Proof
from schemas.judge.v1 import VERSION as JUDGE_SCHEMA_VERSION, MODULE as JUDGE_MODULE, Judgement, Substantive
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService, load_validated_json_blob
from src.stages import Stage
from src.transforms.llm_transform import LLMTransform
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v3"
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

You receive: a preliminary analysis + a set of fact-checked claims.
Your job is to reason through whether the preliminary conclusion still holds given
what was verified. You are not bound by the preliminary analysis — if key claims
were found to be false or overstated, you should revise the conclusion accordingly.
If the verified claims still support the conclusion, confirm it.


When making your final judgment, ask: would a regular person — not a lawyer —
be meaningfully affected by these changes? Consider especially:
- Changes to how their content or data might be used
- Changes to their ability to speak freely or remain on the platform

OUTPUT FORMAT:
You should respond only with valid JSON:
{
  "practically_substantive" : 
  {
    "rating": boolean,
    "reason": "One to three sentences a regular user can understand, 
        explaining what changed and why it matters (or doesn't)."
  }
}
  
"""


@dataclass
class JudgeBuilder:
    storage: BlobService

    def build_prompt(self, brief_blob_name: str, summary_blob_name: str, facts_blob_name: str | None) -> Iterator[PromptMessages]:
        examples: list = [] # self.read_examples()

        # Get Summary
        brief = load_validated_json_blob(brief_blob_name, BRIEF_MODULE, self.storage)
        assert isinstance(brief, Brief) or isinstance(brief, Memo)

        summary = load_validated_json_blob(summary_blob_name, SUMMARY_MODULE, self.storage)
        assert isinstance(summary, SummaryV4)

        # Get Facts
        if facts_blob_name is not None:
            facts = load_validated_json_blob(facts_blob_name, PROOF_MODULE, self.storage)
            assert isinstance(facts, Fact) or isinstance(facts, Proof)
            facts = facts if isinstance(facts, Proof) else Proof(facts=[facts])
        else:
            facts = None

        # Build Prompt - pass the full summary structure
        prompt_msg = Message("user", self._format_prompt(brief, summary, facts))
        yield PromptMessages(system=SYSTEM_PROMPT,
                             history=examples,
                             current=prompt_msg)


    @classmethod
    def _format_prompt(cls, brief: Brief, summary: SummaryV4, facts: Proof | None):
        plaintext_summary = cls._format_summary(summary, brief)
        plaintext_facts = cls._format_proof(facts) if facts is not None else "None"
        formatted = [plaintext_summary,
                      "Fact-Checking:",
                      plaintext_facts]
        return "\n".join(formatted)


    @classmethod
    def _format_summary(cls, summary: SummaryV4, brief: Brief):
        formatted = ["Preliminary analysis:",
                     "Is the change practically substantive?",
                     str(summary.practically_substantive.rating),
                     "Reasoning notes:"] + \
                    [m.section_memo for m in brief.memos] if isinstance(brief, Brief) else [brief.section_memo]
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

    def judge(self, facts_blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Judging {facts_blob_name}")
        parts = self.storage.parse_blob_path(facts_blob_name)
        brief_blob_name = self.storage.unparse_blob_path((Stage.BRIEF_CLEAN.value, parts.company, parts.policy, parts.timestamp, "latest.json"))
        summary_blob_name = self.storage.unparse_blob_path((Stage.SUMMARY_CLEAN.value, parts.company, parts.policy, parts.timestamp, "latest.json"))
        prompter = JudgeBuilder(self.storage)
        if parts.stage != Stage.FACTCHECK_CLEAN.value:
            facts_blob_name = None  # Just passing along summary with no factchecking.
        messages = prompter.build_prompt(brief_blob_name, summary_blob_name, facts_blob_name)
        return self.executor.execute_prompts(messages, JUDGE_MODULE, JUDGE_SCHEMA_VERSION, PROMPT_VERSION)

