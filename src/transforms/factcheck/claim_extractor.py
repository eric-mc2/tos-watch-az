import json
import logging
from dataclasses import dataclass

import ulid  # type: ignore

from schemas.registry import SCHEMA_REGISTRY
from schemas.summary.migration import migrate
from schemas.summary.v0 import MODULE
from schemas.summary.v3 import VERSION as SUMMARY_SCHEMA_VERSION, Summary as SummaryV3
from schemas.claim.v1 import VERSION as CLAIMS_SCHEMA_VERSION
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService
from src.services.llm import LLMService
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

    def build_prompt(self, blob_name: str) -> PromptMessages:
        examples = [] # self.read_examples()
        summary_text = self.storage.load_text_blob(blob_name)
        metadata = self.storage.adapter.load_metadata(blob_name)
        schema = SCHEMA_REGISTRY[MODULE][metadata['schema_version']]
        summary = schema.model_validate_json(summary_text)
        summary = migrate(summary, SUMMARY_SCHEMA_VERSION)
        assert isinstance(summary, SummaryV3)

        txt = '\n'.join([x.practically_substantive.reason for x in summary.chunks])
        prompt = Message("user", txt)
        return PromptMessages(system=SYSTEM_PROMPT,
                             history=examples,
                             current=prompt)


@dataclass
class ClaimExtractor:
    storage: BlobService
    llm: LLMService

    def extract_claims(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Extracting claims from {blob_name}")
        prompter = ClaimExtractorBuilder(self.storage)
        message = prompter.build_prompt(blob_name)

        txt = self.llm.call_unsafe(message.system, message.history + [message.current])
        parsed = self.llm.extract_json_from_response(txt)
        if parsed['success']:
            response = parsed['data']
        else:
            logger.warning(f"Failed to parse response: {parsed['error']}")
            response = {"error": parsed['error'], "raw": txt}

        response = json.dumps(response)
        metadata = dict(
            run_id = ulid.ulid(),
            prompt_version = PROMPT_VERSION,
            schema_version = CLAIMS_SCHEMA_VERSION,
        )
        return response, metadata

