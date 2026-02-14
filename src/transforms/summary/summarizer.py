import logging
import os
from dataclasses import dataclass
from itertools import chain
from typing import Iterable

import numpy as np

from schemas.summary.v2 import Summary as SummaryV2, Substantive
from schemas.summary.v4 import VERSION as SCHEMA_VERSION, MODULE
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.llm import TOKEN_LIMIT, LLMService
from src.stages import Stage
from src.transforms.differ import DiffDoc
from src.transforms.icl import ICL
from src.transforms.summary.diff_chunker import DiffChunker
from src.utils.log_utils import setup_logger
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform


logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v7"
LABELS_VERSION = "substantive_v1"
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

Your role is the summarizer. You will be assigned multiple memos produced
by the note-taker. You will synthesize these notes into a preliminary 
assessment. It is important that you cite the note index (e.g. [3]) whenever you
make a claim about the document. If you quote from the document, make sure to
properly \"escape\" the quotation marks. Your claims will later be fact-checked against the
raw document text.

OUTPUT FORMAT:
Respond with valid JSON only:
{
    "practically_substantive":
    {
        "rating": boolean,
        "reason": "A few sentences or bullets explaining the key factor(s)"
    }
}
"""


@dataclass
class Summarizer:
    storage: BlobService
    icl: ICL
    executor: LLMTransform

    def summarize(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = PromptBuilder(self.storage, self.icl, self.executor.llm)
        messages = prompter.build_prompt(blob_name)
        return self.executor.execute_prompts(messages, MODULE, SCHEMA_VERSION, PROMPT_VERSION)


@dataclass
class PromptBuilder:
    storage: BlobService
    icl: ICL
    llm: LLMService
    _cache = None

    def build_prompt(self, blob_name: str) -> Iterable[PromptMessages]:
        examples = [] # self.read_examples()
        diffs = self.storage.load_text_blob(blob_name)

        chunker = DiffChunker(self.llm, TOKEN_LIMIT)
        chunks = chunker.chunk_diff(SYSTEM_PROMPT, examples, DiffDoc.model_validate_json(diffs))

        for chunk in chunks:
            prompt = Message("user", chunk.model_dump_json())
            yield PromptMessages(system = SYSTEM_PROMPT,
                              history = examples,
                              current = prompt)


    def read_examples(self) -> list[Message]:
        if self._cache:
            return self._cache

        gold = self.icl.load_true_labels(LABELS_VERSION)
        # Filter to false negatives
        gold = gold[(gold['practically_substantive_true']==0) & (gold['practically_substantive_pred']==1)]
        icl_queries = []
        icl_responses = []
        for row in gold.itertuples():
            path = self.storage.parse_blob_path(str(row.blob_path))
            diff_path = os.path.join(Stage.DIFF_CLEAN.value, path.company, path.policy, path.timestamp + ".json")
            if not self.storage.check_blob(diff_path):
              # For some reason or another (like random sampling across different envs), some snapshots may be missing
              continue
            diff = self.storage.load_text_blob(diff_path)
            icl_queries.append(Message("user", diff))
            answer = SummaryV2(practically_substantive=
                               Substantive(
                                   rating= False,
                                   reason = "Does not materially impact user experience, rights, or risks."
                               )
            )
            icl_responses.append(Message("assistant", answer.model_dump_json()))

        # Pick shortest examples to economize on tokens
        # If the first k already exceed the prompt length, use 0 < k
        lengths_list = [len(x.content) for x in icl_queries]
        order = np.argsort(lengths_list)
        lengths = np.cumsum(np.array(lengths_list)[order])
        limit = min(N_ICL, int(np.searchsorted(lengths, TOKEN_LIMIT)))
        if len(icl_queries) < limit:
            logger.warning("No labeled examples found for ICL.")
        icl_queries = list(np.array(icl_queries)[order])[:limit]
        icl_responses = list(np.array(icl_responses)[order])[:limit]
        self._cache = list(chain.from_iterable(zip(icl_queries, icl_responses)))
        return self._cache


