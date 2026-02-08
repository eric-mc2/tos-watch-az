import json
import logging
from dataclasses import dataclass
from typing import Iterator

import ulid  # type: ignore

from schemas.registry import SCHEMA_REGISTRY
from schemas.claim.v1 import Claims as ClaimsV1
from schemas.claim.v0 import MODULE
from schemas.factcheck.v1 import VERSION as FACTCHECK_SCHEMA_VERSION
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v1"
N_ICL = 3
SYSTEM_PROMPT = """
Your role is the expert fact checker. Your task is to 
verify whether a document entails a specific claim. 
You should respond with valid json.

INPUT FORMAT:
{"claim": "abcde ...", "document": "defgh ...."}


OUTPUT FORMAT:
{"veracity": bool, "reason": "One sentence describing why claim is true or not."}  
"""

@dataclass
class ClaimCheckerBuilder:
    storage: BlobService

    def build_prompt(self, blob_name: str) -> Iterator[PromptMessages]:
        examples = [] # self.read_examples()
        claims_text = self.storage.load_text_blob(blob_name)
        metadata = self.storage.adapter.load_metadata(blob_name)
        schema = SCHEMA_REGISTRY[MODULE][metadata['schema_version']]
        claims = schema.model_validate_json(claims_text)
        assert isinstance(claims, ClaimsV1)

        for claim in claims.claims:
            prompt = Message("user", claim)
            yield PromptMessages(system=SYSTEM_PROMPT,
                             history=examples,
                             current=prompt)


@dataclass
class ClaimChecker:
    storage: BlobService
    llm: LLMService

    def check_claim(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Extracting claims from {blob_name}")
        prompter = ClaimCheckerBuilder(self.storage)
        messages = prompter.build_prompt(blob_name)

        responses = []
        for message in messages:
            txt = self.llm.call_unsafe(message.system, message.history + [message.current])
            parsed = self.llm.extract_json_from_response(txt)
            if parsed['success']:
                responses.append(parsed['data'])
            else:
                logger.warning(f"Failed to parse response: {parsed['error']}")
                responses.append({"error": parsed['error'], "raw": txt})

        response = json.dumps(dict(claims=responses))
        metadata = dict(
            run_id = ulid.ulid(),
            prompt_version = PROMPT_VERSION,
            schema_version = FACTCHECK_SCHEMA_VERSION,
        )
        return response, metadata

