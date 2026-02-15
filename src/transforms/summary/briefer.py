import json
import logging
from dataclasses import dataclass, asdict
from typing import Iterable, List

from schemas.brief.v1 import BRIEF_MODULE, MEMO_MODULE, BRIEF_VERSION, MEMO_VERSION, Brief, Memo
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.llm import TOKEN_LIMIT, LLMService
from src.services.blob import BlobService
from src.stages import Stage
from src.transforms.differ import DiffDoc
from src.transforms.summary.diff_chunker import DiffChunker, StandardDiffFormatter
from src.transforms.llm_transform import LLMTransform, create_llm_parser_saver, create_llm_parser, create_llm_saver
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
"relevance_flag": bool,
"section_memo": "Notes in narrative form or markdown with \"escaped\" quotes.",
"running_memo": "Notes in narrative form or markdown with \"escaped\" quotes."
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
        previous_message = Message("assistant", empty_brief.model_dump_json())
        responses = []
        for message in messages:
            # Force serial execution of prompts. Inject previous response into next prompt.
            message.history = [previous_message]
            response, metadata = self.executor.execute_prompts([message], MEMO_MODULE, MEMO_VERSION, PROMPT_VERSION)
            # Should we save the raw here? Probably not? The whole thing is ONE operation. Chunks live in memory.
            # Breaking into multiple files adds complexity. Really if there's ANY structurally invalid parts,
            # the whole stage will fail.
            # Will save LAST raw (unchunked) in case of errors for error diagnosis. Previous chunks were at least
            # structurally valid.
            saver = create_llm_saver(self.storage, Stage.BRIEF_RAW.value)
            saver(blob_name, response, metadata)
            parser = create_llm_parser(self.executor.llm, MEMO_MODULE)
            response_clean, metadata = parser(blob_name, response, metadata)
            previous_message = Message("assistant", response_clean)
            responses.append((response_clean, metadata))
        memos = [Memo.model_validate_json(txt) for txt, meta in responses]
        return Brief(memos=memos).model_dump_json(), responses[-1][1]



@dataclass
class BriefBuilder:
    storage: BlobService
    llm: LLMService

    def build_prompt(self, blob_name: str) -> Iterable[PromptMessages]:
        examples : List[Message] = [] # self.read_examples()
        diffs = self.storage.load_text_blob(blob_name)

        chunker = DiffChunker(self.llm, TOKEN_LIMIT, StandardDiffFormatter())
        # TODO: we are ignoring the previous memo size in this calculation
        chunks = chunker.chunk_diff(SYSTEM_PROMPT, examples, DiffDoc.model_validate_json(diffs))

        for chunk in chunks:
            prompt = Message("user", json.dumps([asdict(c) for c in chunk]))
            yield PromptMessages(system = SYSTEM_PROMPT,
                                history = examples,
                                current = prompt)