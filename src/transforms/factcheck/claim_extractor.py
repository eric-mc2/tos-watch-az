import logging
from dataclasses import dataclass
from typing import Iterator

from schemas.registry import SCHEMA_REGISTRY
from schemas.summary.v0 import MODULE as SUMMARY_MODULE, SummaryBase
from schemas.summary.v4 import Summary as SummaryV4
from schemas.summary.migration import migrate
from schemas.fact.v1 import CLAIMS_VERSION as CLAIMS_SCHEMA_VERSION, CLAIMS_MODULE
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

Your role is the expert fact checker. Your task is to decompose a
document analysis into several claims that can be factually verified. Generally the 
analysis will be of the form "The document mentions X,Y,Z" and
you should respond with valid JSON:

{"claims": ["The document mentions X","The document mentions Y","The document mentions Z"]}  
"""


@dataclass
class ClaimExtractorBuilder:
    storage: BlobService

    def build_prompt(self, blob_name: str) -> Iterator[PromptMessages]:
        examples: list = [] # self.read_examples()
        summary_text = self.storage.load_text_blob(blob_name)
        metadata = self.storage.adapter.load_metadata(blob_name)
        schema = SCHEMA_REGISTRY[SUMMARY_MODULE][metadata['schema_version']]
        # TODO: Doesn't handle chunked!
        summary = schema.model_validate_json(summary_text)
        assert isinstance(summary, SummaryBase)
        summary = migrate(summary, metadata['schema_version'])
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

