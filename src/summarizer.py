import anthropic
import json
import logging
import os
from typing import Any
from bleach.sanitizer import Cleaner
from src.log_utils import setup_logger
from src.blob_utils import load_text_blob, upload_text_blob, parse_blob_path, load_metadata, upload_json_blob
from src.stages import Stage
from src.chat_parser import extract_json_from_response
import ulid

logger = setup_logger(__name__, logging.DEBUG)
_client = None
cleaner = Cleaner()

def get_client():
    # Note: we don't need to close the client. In practice it's better to keep one single 
    # client open during the lifetime of the applicaiton. Not per function invocation.
    global _client
    if _client is None:
        key = os.environ.get('ANTHROPIC_API_KEY')
        if not key:
            raise ValueError("Missing environment variable ANTHROPIC_API_KEY")
        _client = anthropic.Anthropic(api_key=key)
    return _client


def read_examples():
    with open("data/substantive_v1/records.json") as f:
        records = json.load(f)
    limit = 5
    examples = []
    for record in records:
        if len(examples) == limit:
            break
        y_pred = record['suggestions']['practically_substantive']['value']
        y_true = record['responses']['practically_substantive'][0]['value']
        if y_true == "False" and y_pred == "False":
            continue
        if y_true == "True" and y_pred == "True":
            continue
        if y_true == "True" and y_pred == "False":
            continue
        if y_true == "False" and y_pred == "True":
            path = parse_blob_path(record['metadata']['blob_path'])
            prompt_path = os.path.join(Stage.PROMPT.value, path.company, path.policy, path.timestamp.removesuffix(".json") + ".txt")
            prompt = load_text_blob(prompt_path)
            label = prompt.replace("<diff_sections>","<diff_sections><practically_substantive>False</practically_substantive>")
            examples.append(label)
    return "\n".join(examples)


SCHEMA_VERSION = "v1"
PROMPT_VERSION = "v2"

SCHEMA = """
{
"legally_substantive": {
    "rating": bool,
    "explanation": str
},
"practically_substantive": {
    "rating": bool,
    "explanation": str
},
"change_keywords": list[str],
"subject_keywords": list[str],
"helm_keywords": list[str],
}
"""

EXAMPLES = read_examples()

SYSTEM_PROMPT = f"""
You are a legal assistant and an expert in contract law. 
Your task is to read and compare different versions of terms of service.
You will be given only the diff (changes) between the versions; 
you will not have the context of unchanged sections.
You need to answer three questions w.r.t. the totality of the diff:
    1) Does the new version contain a substantive change over the previous? 
    Answer this first from a legal and next from a practical lay-person perspective.
    2) Categorize the kind of changes with 1-3 keywords. Do not give an explanation for the categories.
        e.g. ['formatting','clarification']
    3) Categorize the concrete topics addressed in the changed sections. Do not give an explanation for the categories.
    4) Categorize and mark whether any of the following HELM benchmark topics are addressed in the changed sections. Do not give an explanation.
        ['Child Harm',
        'Criminal Activities',
        'Deception',
        'Defamation',
        'Discrimination/Bias',
        'Economic Harm',
        'Fundamental Rights',
        'Hate/Toxicity',
        'Manipulation',
        'Operational Misuses',
        'Political Usage',
        'Privacy',
        'Security Risks',
        'Self-harm',
        'Sexual Content',
        'Violence & Extremism']

Format your answer according to this json schema:
{SCHEMA}

In the following message I will provide the actual diff.
It will be formatted as an xml list of non-contiguous diff sections according to the following schema:
<diff_sections>
    <section idx=[INDEX]>
        <before>[ORIGINAL TEXT]</before>
        <after>[ALTERED TEXT]</after>
    </section>
</diff_sections>

Here are some labeled examples:
{EXAMPLES}
"""

def is_diff(diff_str: str) -> bool:
    diff_obj = json.loads(diff_str)
    diffs = diff_obj.get('diffs', [])
    return any([d['tag'] != 'equal' for d in diffs])


def summarize(blob_name: str):
    prompt = load_text_blob(blob_name)
    
    logger.debug(f"Summarizing {blob_name}")
    try:
        summary_result = _summarize(prompt)
    except ValueError:
        return
    
    run_id = ulid.ulid()
    in_path = parse_blob_path(blob_name)
    out_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/{run_id}.txt"
    metadata = dict(
        run_id = run_id,
        prompt_version = PROMPT_VERSION,
        schema_version = SCHEMA_VERSION,
    )
    upload_text_blob(summary_result, out_path, metadata=metadata)

    # XXX: There is a race condition here IF you fan out across versions. Would need new orchestrator for updating latest.
    latest_path = f"{Stage.SUMMARY_RAW.value}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.json"
    upload_text_blob(summary_result, latest_path, metadata=metadata)

    logger.info(f"Successfully summarized blob: {blob_name}")
    
    
def _summarize(prompt: str) -> str:
    prompt_length = len(SYSTEM_PROMPT) + len(prompt)
    context_window = 200000
    token_limit = 50000
    if prompt_length >= context_window:
        logger.error(f"Prompt length {prompt_length} exceeds context window {context_window}")
    if prompt_length >= token_limit:
        # TODO: THIS GETS HIT QUITE A LOT FOR V2 PROMPT!
        logger.error(f"Prompt length {prompt_length} exceeds rate limit of {token_limit} / minute.")
        raise ValueError(f"Prompt length {prompt_length} exceeds rate limit of {token_limit} / minute.")
    client = get_client()
    response = client.messages.create(
        model = "claude-3-5-haiku-20241022",
        max_tokens = 1000,
        system = [{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"}
        }],
        messages = [
            dict(role = "user",
                content = prompt,
            ),
        ]
    )
    return _parse_response(response)


def _parse_response(resp: anthropic.types.message.Message) -> dict:
    if resp.stop_reason != 'end_turn':
        pass # might need to fix
    if not resp.content:
        raise ValueError("Empty LLM response")
    if len(resp.content) > 1:
        logger.warning("Multiple LLM outputs")
    return resp.content[0].text


def parse_response_json(blob_name: str, resp: str):
    in_path = parse_blob_path(blob_name)
    result = extract_json_from_response(resp)
    if not result['success']:
        raise ValueError(f"Failed to parse json from chat. Error: {result['error']}. Original: {resp}")
    cleaned = sanitize_response(result['data'])
    cleaned_txt = json.dumps(cleaned, indent=2)

    in_meta = load_metadata(blob_name)
    run_id = in_meta['run_id']
    
    out_path = os.path.join(Stage.SUMMARY_CLEAN.value, in_path.company, in_path.policy, in_path.timestamp, f"{run_id}.json")
    upload_json_blob(cleaned_txt, out_path, metadata=in_meta)
    out_path = os.path.join(Stage.SUMMARY_CLEAN.value, in_path.company, in_path.policy, in_path.timestamp, "latest.json")
    upload_json_blob(cleaned_txt, out_path, metadata=in_meta)
    
def sanitize_response(data: dict|list|str|Any) -> dict|list|str|Any:
    if isinstance(data, dict):
        return {k: sanitize_response(v) for k,v in data.items()}
    elif isinstance(data, list):
        return [sanitize_response(v) for v in data]
    elif isinstance(data, str):
        return cleaner.clean(data)
    else:
        return data

def prompt_diff(diff_str: str) -> str:
    diff_obj = json.loads(diff_str)
    output = []
    diffs = [d for d in diff_obj['diffs'] if d['tag'] != 'equal']
    for i, diff in enumerate(diffs):
        before = ' '.join(diff['before'])
        after = ' '.join(diff['after'])
        xml = (f'<section idx="{i}">\n'
            f"<before>{before}</before>\n"
            f"<after>{after}</after>\n"
            "</section>")
        output.append(xml)
    sections = '\n'.join(output)
    return f"<diff_sections>{sections}</diff_sections>"
    