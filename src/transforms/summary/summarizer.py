import logging
from dataclasses import dataclass
from typing import Iterable, List

from schemas.summary.v4 import VERSION as SCHEMA_VERSION, MODULE
from src.transforms.icl import SummaryDataLoader
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.llm import LLMService
from src.utils.log_utils import setup_logger
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform


logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v11"
LABELS_VERSION = "summary_v1"
SYSTEM_PROMPT = """
You are part of a team that is analyzing terms of service changes.
The team's goal is to determine whether changes are practically substantive—meaning 
they materially affect what a typical user can do, must do, or what happens to them.

You are the summarizer. You receive a set of memos from the note-taker and
synthesize them into a preliminary assessment.

PRACTICALLY SUBSTANTIVE changes include:
- Alterations to data collection, retention, sharing, or use (including for AI/ML training)
- New or expanded rights the company claims over user content
- Changes to user permissions, account suspension, or termination conditions
- Modifications to pricing, payments, billing cycles, or refunds
- Changes to dispute resolution, arbitration, or liability limits
- New requirements or prohibitions on user behavior
- Expansions of what the platform can do with user data or content

NOT PRACTICALLY SUBSTANTIVE:
- Reformatting or reorganization with no change in meaning
- Clarifications that don't change what either party can do
- Administrative updates (entity names, addresses, dates)
- Typo or grammar fixes
- Legally required boilerplate that doesn't change user experience

GUIDELINES:
When assessing consumer impact, consider:
- Would a typical user notice a difference in how the service behaves toward them?
- Does the change affect what the platform can do with their content, posts, or personal data?
- Could the change affect account access, free expression, or dispute outcomes?
- Does vague new language quietly expand company rights in ways users would object to
  if they understood them plainly?
  
It is important that you cite the note index (e.g. [3]) whenever you make a claim about the document. 
Your assessment will be fact-checked against the raw document, so be precise and include concrete evidence.


OUTPUT FORMAT:
Respond with valid JSON only:
{
    "practically_substantive":
    {
        "rating": boolean,
        "reason": "Two to four sentences or bullets explaining the key factors, with citations."
    }
}
"""


@dataclass
class Summarizer:
    storage: BlobService
    executor: LLMTransform

    def __init__(self, storage: BlobService, executor: LLMTransform):
        self.storage = storage
        self.executor = executor
        self.icl = SummaryDataLoader(self.storage)


    def summarize(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = PromptBuilder(self.storage, self.icl, self.executor.llm)
        messages = prompter.build_prompt(blob_name)
        return self.executor.execute_prompts(messages, MODULE, SCHEMA_VERSION, PROMPT_VERSION)


@dataclass
class PromptBuilder:
    storage: BlobService
    icl: SummaryDataLoader
    llm: LLMService
    _cache = None

    def build_prompt(self, blob_name: str) -> Iterable[PromptMessages]:
        examples : List[Message] = self.icl.load_icl()
        brief_txt = self.storage.load_text_blob(blob_name)
        
        # XXX: This is now expected to be small enough to run in one pass.
        prompt = Message("user", brief_txt)
        yield PromptMessages(system = SYSTEM_PROMPT,
                            history = examples,
                            current = prompt)