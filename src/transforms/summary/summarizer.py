import logging
import os
from dataclasses import dataclass
from itertools import chain
from typing import Iterable, List

import numpy as np

from schemas.summary.v2 import Summary as SummaryV2, Substantive
from schemas.summary.v4 import VERSION as SCHEMA_VERSION, MODULE
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.llm import TOKEN_LIMIT, LLMService
from src.stages import Stage
from src.transforms.icl import ICLDataLoader
from src.utils.log_utils import setup_logger
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform


logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v8"
LABELS_VERSION = "summary_v1"
N_ICL = 3
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
    icl: ICLDataLoader
    executor: LLMTransform

    def summarize(self, blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Summarizing {blob_name}")
        prompter = PromptBuilder(self.storage, self.icl, self.executor.llm)
        messages = prompter.build_prompt(blob_name)
        return self.executor.execute_prompts(messages, MODULE, SCHEMA_VERSION, PROMPT_VERSION)


@dataclass
class PromptBuilder:
    storage: BlobService
    icl: ICLDataLoader
    llm: LLMService
    _cache = None

    def build_prompt(self, blob_name: str) -> Iterable[PromptMessages]:
        examples : List[Message] = [] # self.read_examples()
        brief_txt = self.storage.load_text_blob(blob_name)
        
        # XXX: This is now expected to be small enough to run in one pass.
        prompt = Message("user", brief_txt)
        yield PromptMessages(system = SYSTEM_PROMPT,
                            history = examples,
                            current = prompt)


    def read_examples(self) -> list[Message]:
        """
        Load ICL (few-shot) examples for prompts.
        
        TODO: DATA LEAKAGE ISSUE - Currently loads from eval labels.
        Should use ICLDataLoader reading from data/icl/ instead of EvalDataLoader.
        Once ICL split logic is implemented, update to:
        """
        if self._cache:
            return self._cache

        icl_loader = ICLDataLoader(self.storage)
        gold = icl_loader.load_examples(LABELS_VERSION)
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


