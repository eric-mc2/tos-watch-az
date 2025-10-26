import anthropic
import json
import logging
from src.log_utils import setup_logger
import os

logger = setup_logger(logging.DEBUG)
_client = None

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
"""

def is_diff(diff_str: str) -> str:
    diff_obj = json.loads(diff_str)
    diffs = diff_obj.get('diffs', [])
    return any([d['tag'] != 'equal' for d in diffs])

def create_prompt(diff_str: str) -> str:
    diff = _structure_diff(diff_str)
    prompt = [SYSTEM_PROMPT, diff]
    prompt = '\n'.join(prompt)
    logger.debug(f"Prompting: {prompt}")
    return prompt

def summarize(prompt: str) -> str:
    client = get_client()
    response = client.messages.create(
        model = "claude-3-5-haiku-20241022",
        max_tokens = max(1000, len(prompt)),
        messages = [
            dict(role = "user",
                content = prompt,
            ),
        ]
    )
    output = _parse_response(response)
    return json.dumps(output, indent=2)


def _parse_response(resp: anthropic.types.message.Message) -> dict:
    if resp.stop_reason != 'end_turn':
        pass # might need to fix
    if not resp.content:
        raise ValueError("Empty LLM response")
    if len(resp.content) > 1:
        logger.warning("Multiple LLM outputs")
    try:
        output = json.loads(resp.content[0].text)
        return output
    except Exception as e:
        logger.error(f"Failed to parse json {resp.content[0].text}")
        raise e
    

def _structure_diff(diff_str: str) -> str:
    diff_obj = json.loads(diff_str)
    output = []
    diffs = [d for d in diff_obj['diffs'] if d['tag'] != 'equal']
    for i, diff in enumerate(diffs):
        before = ' '.join(diff['before'])
        after = ' '.join(diff['after'])
        xml = (f"<section idx={i}>\n"
            f"<before>{before}</before>\n"
            f"<after>{after}</after>\n"
            "</section>")
        output.append(xml)
    sections = '\n'.join(output)
    return f"<diff_sections>{sections}</diff_sections>"
    