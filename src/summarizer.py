import json
import logging
import os
import ulid
import numpy as np
from itertools import chain
from src.log_utils import setup_logger
from src.blob_utils import load_text_blob, load_json_blob, parse_blob_path, load_blob, check_blob
from src.stages import Stage
from src.prompt_eng import load_true_labels
from src.differ import DiffSection, DiffDoc
from src.claude_utils import call_api, Message, TOKEN_LIMIT, extract_json_from_response
from functools import lru_cache
from itertools import accumulate
from schemas.summary.v3 import VERSION as SCHEMA_VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive
from schemas.summary.registry import CLASS_REGISTRY

logger = setup_logger(__name__, logging.DEBUG)

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

# TODO: The next thing is to have a second agent in the conversation or after it basically either directly doing RAG or 
#     just asking it to double-check that the thing mentioned is real.
# TODO: When the two documents are really just not the same at all then how can we chunk it?

def summarize(blob_name: str) -> tuple[str, dict]:
    logger.debug(f"Summarizing {blob_name}")
    examples = read_examples()
    diffs = load_text_blob(blob_name)
    chunks = _chunk_prompt(DiffDoc.model_validate_json(diffs))

    responses = []
    for chunk in chunks:
        prompt = [Message("user", chunk.model_dump_json())]
        txt = call_api(SYSTEM_PROMPT, examples + prompt)
        parsed = extract_json_from_response(txt)
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


def _chunk_prompt(doc: DiffDoc) -> list[DiffDoc]:
    HEADROOM = 0.8
    limit = TOKEN_LIMIT * HEADROOM
    # split_diffs = []
    # for i,d in enumerate(doc.diffs):
    #     if len(d.before) + len(d.after) < limit:
    #         split_diffs.append(d)
    #     else:
            
    chunk_sizes = [len(d.before) + len(d.after) for d in doc.diffs]  # XXX: approx! slightly under-counts!

    page_nums = [size // limit for size in accumulate(chunk_sizes)]
    pages = []
    for i,d in enumerate(doc.diffs):
        if i == 0 or page_nums[i-1] != page_nums[i]:
            pages.append(DiffDoc(diffs=[d]))
        else:
            pages[-1].diffs.append(d)
    return pages


@lru_cache(1)
def read_examples() -> list[Message]:
    gold = load_true_labels(os.path.join(Stage.LABELS.value, "substantive_v1.json"))
    # Filter to false negatives
    gold = gold[(gold['practically_substantive_true']==0) & (gold['practically_substantive_pred']==1)]
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
    