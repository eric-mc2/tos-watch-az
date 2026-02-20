import logging
from dataclasses import dataclass
from typing import Iterator

from schemas.registry import load_data
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.summary.v4 import Summary as SummaryV4
from schemas.fact.v1 import CLAIMS_VERSION as CLAIMS_SCHEMA_VERSION, CLAIMS_MODULE
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v2"
SYSTEM_PROMPT = """
You are part of a team analyzing changes to terms of service documents.

Your role is the claim decomposer. You receive a preliminary analysis of a ToS diff
and break it down into a list of specific, verifiable factual claims — things that
can be directly confirmed or denied by reading the source document.

Good claims are specific and falsifiable:
  ✓ "The document adds language granting the company a license to use user content for AI training."
  ✓ "The document removes the 30-day notice requirement before account termination."
  ✗ "The document discusses data privacy." (too vague to verify)
  ✗ "The changes are significant." (a conclusion, not a factual claim)

Each claim should correspond to a distinct assertion from the analysis.
If the analysis cites memo indices like [2], preserve that context in the claim.

OUTPUT FORMAT:
Respond with valid JSON only:
{
  "claims": ["Claim one.", "Claim two.", "Claim three."]
}
"""


@dataclass
class ClaimExtractorBuilder:
    storage: BlobService

    def build_prompt(self, blob_name: str) -> Iterator[PromptMessages]:
        examples: list = [] # self.read_examples()
        summary = load_data(blob_name, SUMMARY_MODULE, self.storage)
        assert isinstance(summary, SummaryV4)

        if not summary.practically_substantive.rating:
            return # Nothing to process downstream. Ends iterator

        txt = f"DOCUMENT ANALYSIS: \n {summary.practically_substantive.reason}"
        prompt = Message("user", txt)
        yield PromptMessages(system=SYSTEM_PROMPT,
                             history=examples,
                             current=prompt)


@dataclass
class ClaimExtractor:
    storage: BlobService
    executor: LLMTransform

    def extract_claims(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Extracting claims from {blob_name}")
        prompter = ClaimExtractorBuilder(self.storage)
        messages = prompter.build_prompt(blob_name)
        return self.executor.execute_prompts(messages, CLAIMS_MODULE, CLAIMS_SCHEMA_VERSION, PROMPT_VERSION)

