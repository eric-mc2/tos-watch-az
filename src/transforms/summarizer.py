import json
import logging
from dataclasses import dataclass
import ulid

from schemas.summary.v3 import VERSION as SCHEMA_VERSION, Summary
from src.utils.log_utils import setup_logger
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.transforms.prompt_builder import PromptBuilder, PROMPT_VERSION

logger = setup_logger(__name__, logging.DEBUG)


@dataclass
class Summarizer:
    storage: BlobService
    llm: LLMService

    def summarize(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = PromptBuilder(self.storage)
        messages = prompter.build_prompt(blob_name)

        responses = []
        for message in messages:
            txt = self.llm.call_unsafe(message.system, 
                                       message.history + [message.current],
                                       Summary)
            parsed = self.llm.extract_json_from_response(txt)
            if parsed['success']:
                responses.append(parsed['data'])
            else:
                logger.warning(f"Failed to parse response: {parsed['error']}")
                responses.append({"error": parsed['error'], "raw": txt})

        response = json.dumps(dict(chunks = responses))
        metadata = dict(
            run_id = ulid.ulid(),
            prompt_version = PROMPT_VERSION,
            schema_version = SCHEMA_VERSION,
        )
        return response, metadata
