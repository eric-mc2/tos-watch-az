import anthropic
import logging
import os
from pydantic import BaseModel
from typing import Any
import json
import re
from dataclasses import dataclass, asdict
from bleach.sanitizer import Cleaner
from src.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)
_client = None
cleaner = Cleaner()


CONTEXT_WINDOW = 200000
TOKEN_LIMIT = 50000  # TODO: Next priority is breaking up summaries to be robust to this!

@dataclass
class Message:
    role: str
    content: str

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


def call_api(system: str, messages: list[Message]) -> str:
    _validate_input(system, messages)
    client = get_client()
    response = client.messages.create(
        model = "claude-3-5-haiku-20241022",
        max_tokens = 1000,
        system = [{
            "type": "text",
            "text": system,
            "cache_control": {"type": "ephemeral"}
        }],
        messages = [asdict(m) for m in messages]
    )
    if response.stop_reason != 'end_turn':
        pass # might need to fix
    if not response.content:
        raise ValueError("Empty LLM response")
    if len(response.content) > 1:
        logger.warning("Multiple LLM outputs")
    txt = response.content[0].text
    return txt


def _validate_input(system: str, messages: list[Message]) -> None:
    prompt_length = len(system) + sum([len(m.content) for m in messages])
    if prompt_length >= CONTEXT_WINDOW:
        logger.error(f"Prompt length {prompt_length} exceeds context window {CONTEXT_WINDOW}")
    if prompt_length >= TOKEN_LIMIT:
        logger.error(f"Prompt length {prompt_length} exceeds rate limit of {TOKEN_LIMIT} / minute.")
        raise ValueError(f"Prompt length {prompt_length} exceeds rate limit of {TOKEN_LIMIT} / minute.") # TODO: dont let this crash the circuit


def validate_output(resp: str, validator: BaseModel) -> str:
    result = extract_json_from_response(resp)
    if not result['success']:
        raise ValueError(f"Failed to parse json from chat. Error: {result['error']}. Original: {resp}")
    if isinstance(result['data'], list):
        raise ValueError(f"Expected dictionary output. Got list.")
    model = validator(**result['data'])
    cleaned = sanitize_response(model.model_dump())
    cleaned_txt = json.dumps(cleaned, indent=2)
    return cleaned_txt


def sanitize_response(data: dict|list|str|Any) -> dict|list|str|Any:
    if isinstance(data, dict):
        return {k: sanitize_response(v) for k,v in data.items()}
    elif isinstance(data, list):
        return [sanitize_response(v) for v in data]
    elif isinstance(data, str):
        return cleaner.clean(data)
    else:
        return data


def extract_json_from_response(response: str) -> dict:
    """
    Extract JSON with additional context about the extraction process.

    Args:
        response: The chatbot response string

    Returns:
        Dictionary containing:
        - 'success': boolean indicating if JSON was successfully extracted
        - 'data': the parsed JSON object (if successful)
        - 'raw_match': the raw string that was matched (if any)
        - 'error': error message (if unsuccessful)
    """
    result = {
        'success': False,
        'data': None,
        'raw_match': None,
        'error': None
    }

    if not response or not isinstance(response, str):
        result['error'] = 'Invalid input: response must be a non-empty string'
        return result

    # Try object pattern first
    json_pattern = r'\{[^{}]*?(?:\{[^{}]*?\}[^{}]*?)*\}'
    matches = re.findall(json_pattern, response, re.DOTALL)

    for match in matches:
        result['raw_match'] = match
        try:
            cleaned_match = re.sub(r'\s+', ' ', match).strip()
            result['data'] = json.loads(cleaned_match)
            result['success'] = True
            return result
        except json.JSONDecodeError as e:
            # Store the first error encountered
            if not result['error']:
                result['error'] = f'JSON decode error:\n{e}'

    # Try array pattern
    array_pattern = r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]'
    array_matches = re.findall(array_pattern, response, re.DOTALL)

    for match in array_matches:
        result['raw_match'] = match
        try:
            cleaned_match = re.sub(r'\s+', ' ', match).strip()
            result['data'] = json.loads(cleaned_match)
            result['success'] = True
            return result
        except json.JSONDecodeError as e:
            if not result['error']:
                result['error'] = f'JSON decode error:\n{e}'

    if not result['error']:
        result['error'] = 'No valid JSON structure found in response'

    return result
