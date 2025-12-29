import json
import logging
import os
import pickle
import ulid
import numpy as np
from itertools import chain
from src.log_utils import setup_logger
from src.blob_utils import load_text_blob, parse_blob_path, load_blob, check_blob
from src.stages import Stage
from src.prompt_eng import load_true_labels
from src.claude_utils import call_api, Message, TOKEN_LIMIT
from functools import lru_cache

logger = setup_logger(__name__, logging.DEBUG)

SCHEMA_VERSION = "v2"
PROMPT_VERSION = "v5"
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
    "reason": "One-two sentences explaining the key factor"
  }
}
"""

def summarize(blob_name: str) -> tuple[str, dict]:
    logger.debug(f"Summarizing {blob_name}")
    examples = read_examples()
    prompt = [Message("user", load_text_blob(blob_name))]
    txt = call_api(SYSTEM_PROMPT, examples + prompt)
    metadata = dict(
        run_id = ulid.ulid(),
        prompt_version = PROMPT_VERSION,
        schema_version = SCHEMA_VERSION,
    )
    return txt, metadata


@lru_cache(1)
def read_examples() -> list[Message]:
    gold = load_true_labels(os.path.join(Stage.LABELS.value, "substantive_v1.json"))
    # Filter to false negatives
    gold = gold[(gold['practically_substantive_true']==0) & (gold['practically_substantive_pred']==1)]
    schema = pickle.loads(load_blob(os.path.join(Stage.SCHEMA.value, "summary", SCHEMA_VERSION + ".pkl")))
    icl_queries = []
    icl_responses = []
    for row in gold.itertuples():
        path = parse_blob_path(row.blob_path)
        diff_path = os.path.join(Stage.DIFF_CLEAN.value, path.company, path.policy, path.timestamp + ".json")
        if not check_blob(diff_path):
          # For some reason or another (like random sampling across different envs), some snapshots may be missing
          continue
        diff = load_text_blob(diff_path)
        icl_queries.append(Message("user", diff))
        answer = {"practically_substantive": {"rating": False, "reason": "Does not materially impact user experience, rights, or risks."}}
        schema.model_validate(answer)
        icl_responses.append(Message("assistant", json.dumps(answer)))
    
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
    