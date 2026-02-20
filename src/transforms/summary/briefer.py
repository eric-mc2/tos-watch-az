import logging
from dataclasses import dataclass
from typing import Iterable, List

from schemas.brief.v2 import MEMO_MODULE, MEMO_VERSION, Memo
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.llm import TOKEN_LIMIT, LLMService
from src.services.blob import BlobService
from src.transforms.differ import DiffDoc
from src.transforms.summary.diff_chunker import DiffChunker, StandardDiffFormatter
from src.transforms.llm_transform import LLMTransform
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v2"
N_ICL = 3
SYSTEM_PROMPT = """
You are part of a team analyzing changes to terms of service documents.
The team's goal is to determine whether changes are practically substantive — meaning
they materially affect what a typical user can do, must do, or what happens to them.
"Change" is the operative word.

Your role is the note-taker. You are condensing the document (particularly its changes)
into key points: verifiable statements, phrases, and facts. You are stripping out
the legalese and boilerplate (except when it matters) and preserving important 
but succinct details. 

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

NOTE TAKING GUIDANCE:
Specific details matter. Here is an example of a note that identifies
relevant topics but is vague and not actionable:

    ### Notable Additions/Changes
    - More explicit about AI product usage terms
    - Enhanced clarity around content ownership
    - Refined termination conditions

INPUT FORMAT:
You are reading a diff: additions and removals 
from a specific section of the document. Old document sections are prefixed with (-)
while new document sections are prefixed with (+). If the document is short you will see
both (+) and (-) together, but if the document is long you may only see the (+) part or the (-) part
at a time.

You will receive: the document section's diffs + 
your notes from the previous document section.

OUTPUT FORMAT:
You will produce a section memo containing notes from this input text,
a running memo containing your cumulative relevant notes from previous texts + the current text.
Respond with valid JSON. Properly escape any quotes from the document.

{
  "section_memo": "Markdown notes",
  "running_memo": "Markdown notes",
}
"""


@dataclass
class Briefer:
    storage: BlobService
    executor: LLMTransform

    def brief(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = BriefBuilder(self.storage, self.executor.llm)
        empty_brief = Memo( section_memo="",
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

        #  the goal is to stay well below the limit due to degrading attention weight
        token_limit = (TOKEN_LIMIT * 6) // 10
        chunker = DiffChunker(self.llm, token_limit, StandardDiffFormatter())
        chunks = chunker.chunk_diff(SYSTEM_PROMPT, examples, DiffDoc.model_validate_json(diffs))

        for chunk in chunks:
            prompt = Message("user", "\n".join((c.format() for c in chunk)))
            yield PromptMessages(system = SYSTEM_PROMPT,
                                history = examples,
                                current = prompt)
            
