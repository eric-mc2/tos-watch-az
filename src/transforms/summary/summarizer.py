import logging
from dataclasses import dataclass

from schemas.summary.v4 import VERSION as SCHEMA_VERSION, MODULE
from src.transforms.prompt_eng import PromptEng
from src.utils.log_utils import setup_logger
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.transforms.llm_transform import LLMTransform
from src.transforms.summary.prompt_builder import PromptBuilder, PROMPT_VERSION

logger = setup_logger(__name__, logging.DEBUG)


@dataclass
class Summarizer:
    storage: BlobService
    llm: LLMService
    prompt_eng: PromptEng
    executor: LLMTransform

    def summarize(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = PromptBuilder(self.storage, self.prompt_eng)
        messages = prompter.build_prompt(blob_name)
        return self.executor.execute_prompts(messages, MODULE, SCHEMA_VERSION, PROMPT_VERSION)
