import logging
from dataclasses import dataclass
from typing import Iterable, List

from schemas.brief.v1 import MEMO_MODULE, MEMO_VERSION, Memo
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.llm import TOKEN_LIMIT, LLMService
from src.services.blob import BlobService
from src.transforms.differ import DiffDoc
from src.transforms.summary.diff_chunker import DiffChunker, StandardDiffFormatter
from src.transforms.llm_transform import LLMTransform
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v1"
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

Your role is the note taker. You will be assigned sections
of the raw document and your task  is to write a brief memo noting what is important
or might be relevant for other team members to check back on. 
Your memo will mark and organize potentially relevant info pertaining to the current
document section. It will also update a running summary of all sections seen so far.
If helpful, use organization strategies like a checklist or keyword list, etc.
If the document is entirely irrelevant to the question at hand (practically substantive clauses or changes),
mark its relevance as false.

OUTPUT FORMAT:
Respond with valid JSON with the following structure. 
If quoting from the document make sure to properly escape the quotes.

{
"running_memo": "Notes in narrative form or markdown with \"escaped\" quotes.",
"section_memo": "Notes in narrative form or markdown with \"escaped\" quotes.",
"relevance_flag": boolean
}
"""


@dataclass
class Briefer:
    storage: BlobService
    executor: LLMTransform

    def brief(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = BriefBuilder(self.storage, self.executor.llm)
        empty_brief = Memo(relevance_flag=False,
                            section_memo="",
                            running_memo="")
        messages = prompter.build_prompt(blob_name)
        
        return self.executor.execute_prompts_serial(
            messages,
            MEMO_MODULE,
            MEMO_VERSION,
            PROMPT_VERSION,
            initial_state=empty_brief.model_dump_json()
        )
    

@dataclass
class BriefBuilder:
    storage: BlobService
    llm: LLMService

    def build_prompt(self, blob_name: str) -> Iterable[PromptMessages]:
        examples : List[Message] = [] # self.read_examples()
        diffs = self.storage.load_text_blob(blob_name)

        limit = max(TOKEN_LIMIT // 2, TOKEN_LIMIT - self.llm.adapter.get_max_output())
        chunker = DiffChunker(self.llm, limit, StandardDiffFormatter())
        chunks = chunker.chunk_diff(SYSTEM_PROMPT, examples, DiffDoc.model_validate_json(diffs))

        for chunk in chunks:
            prompt = Message("user", "\n".join((c.format() for c in chunk)))
            yield PromptMessages(system = SYSTEM_PROMPT,
                                history = examples,
                                current = prompt)
            
