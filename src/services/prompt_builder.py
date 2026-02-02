# TODO: The next thing is to have a second agent in the conversation or after it basically either directly doing RAG or
#     just asking it to double-check that the thing mentioned is real.
# TODO: When the two documents are really just not the same at all then how can we chunk it?
import os
from dataclasses import dataclass
from functools import lru_cache
from itertools import chain
from typing import Iterable

import numpy as np

from schemas.summary.v2 import Summary as SummaryV2, Substantive
from src.clients.llm.protocol import Message, PromptMessages
from src.prompt_eng import load_true_labels
from src.services.blob import BlobService
from src.services.differ import DiffDoc
from src.services.llm import TOKEN_LIMIT
from src.services.prompt_chunker import PromptChunker
from src.services.summarizer import logger
from src.stages import Stage

PROMPT_VERSION = "v6"
N_ICL = 3
SYSTEM_PROMPT = """
You are an expert at analyzing terms of service changes. Your task is to 
determine whether changes are practically substantive—meaning they materially 
affect what a typical user can do, must do, or what happens to them.

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

OUTPUT FORMAT:
Respond with valid JSON only:
{
  "practically_substantive" : 
  {
    "rating": boolean,
    "reason": "One or two sentences explaining the key factor"
  }
}
"""

@dataclass
class PromptBuilder:
    storage: BlobService

    def build_prompt(self, blob_name: str) -> Iterable[PromptMessages]:
        examples = self.read_examples()
        diffs = self.storage.load_text_blob(blob_name)

        chunker = PromptChunker(TOKEN_LIMIT)
        chunks = chunker.chunk_prompt(DiffDoc.model_validate_json(diffs))

        for chunk in chunks:
            prompt = Message("user", chunk.model_dump_json())
            yield PromptMessages(system = SYSTEM_PROMPT,
                              history = examples,
                              current = prompt)


    @lru_cache(1)
    def read_examples(self) -> list[Message]:
        # TODO: Need to create a test set of labels or make sure this file is available in the test env.
        gold = load_true_labels(os.path.join(Stage.LABELS.value, "substantive_v1.json"))
        # Filter to false negatives
        gold = gold[(gold['practically_substantive_true']==0) & (gold['practically_substantive_pred']==1)]
        icl_queries = []
        icl_responses = []
        for row in gold.itertuples():
            path = self.storage.parse_blob_path(row.blob_path)
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
        lengths = [len(x.content) for x in icl_queries]
        order = np.argsort(lengths)
        lengths = np.cumsum(np.array(lengths)[order])
        limit = min(N_ICL, np.searchsorted(lengths, TOKEN_LIMIT))
        if len(icl_queries) < limit:
            logger.warning("No labeled examples found for ICL.")
        icl_queries = list(np.array(icl_queries)[order])[:limit]
        icl_responses = list(np.array(icl_responses)[order])[:limit]
        return list(chain.from_iterable(zip(icl_queries, icl_responses)))