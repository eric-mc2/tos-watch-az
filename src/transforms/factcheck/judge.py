import json
import logging
from dataclasses import dataclass

import ulid  # type: ignore

from schemas.registry import SCHEMA_REGISTRY
from schemas.summary.migration import migrate
from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.summary.v3 import VERSION as SUMMARY_SCHEMA_VERSION, Summary as SummaryV3
from schemas.judge.v1 import VERSION as JUDGE_SCHEMA_VERSION
from schemas.factcheck.v1 import FactCheck
from schemas.factcheck.v0 import MODULE as FACTCHECK_MODULE
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

    def build_prompt(self, summary_blob_name: str, facts_blob_name: str) -> PromptMessages:
        examples = [] # self.read_examples()

        # Get Summary
        summary_text = self.storage.load_text_blob(summary_blob_name)
        metadata = self.storage.adapter.load_metadata(summary_blob_name)
        schema = SCHEMA_REGISTRY[SUMMARY_MODULE][metadata['schema_version']]
        summary = schema.model_validate_json(summary_text)
        summary = migrate(summary, SUMMARY_SCHEMA_VERSION)
        assert isinstance(summary, SummaryV3)

        # Get Facts
        facts_text = self.storage.load_text_blob(facts_blob_name)
        metadata = self.storage.adapter.load_metadata(facts_blob_name)
        schema = SCHEMA_REGISTRY[FACTCHECK_MODULE][metadata['schema_version']]
        facts = schema.model_validate_json(facts_text)
        assert isinstance(facts, FactCheck)

        # Build Prompt
        prompt = dict(
            summary=summary,
            facts=facts,
        )
        prompt = Message("user", json.dumps(prompt))
        return PromptMessages(system=SYSTEM_PROMPT,
                             history=examples,
                             current=prompt)


@dataclass
class Judge:
    storage: BlobService
    llm: LLMService

    def judge(self, summary_blob_name: str, facts_blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Judging {summary_blob_name}")
        prompter = JudgeBuilder(self.storage)
        message = prompter.build_prompt(summary_blob_name, facts_blob_name)

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
            schema_version = JUDGE_SCHEMA_VERSION,
        )
        return response, metadata

