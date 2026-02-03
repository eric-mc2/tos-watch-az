import logging
import json
import re
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any
from bleach.sanitizer import Cleaner

from schemas.summary.v0 import SummaryBase
from src.utils.log_utils import setup_logger
from src.adapters.llm.protocol import Message, LLMProtocol

logger = setup_logger(__name__, logging.DEBUG)
_client = None
cleaner = Cleaner()

CONTEXT_WINDOW = 200000
TOKEN_LIMIT = 50000  # TODO: Next priority is breaking up summaries to be robust to this!

@dataclass
class LLMService:
    adapter: LLMProtocol

    def call_unsafe(self, system: str, messages: list[Message]) -> str:
        """Call LLM."""
        self.validate_input(system, messages)
        resp = self.adapter.call(system, messages)
        return resp

    def call_and_validate(self, system: str, messages: list[Message], validator: type[SummaryBase]) -> str:
        """Call LLM and validate output against a Pydantic model."""
        self.validate_input(system, messages)
        resp = self.adapter.call(system, messages)
        return self.validate_output(resp, validator)

    def validate_output(self, resp: str, validator: type[SummaryBase]) -> str:
        """Extract JSON from response, validate against model, and sanitize."""
        result = self.extract_json_from_response(resp)
        if not result['success']:
            raise ValueError(f"Failed to parse json from chat. Error: {result['error']}. Original: {resp}")
        if isinstance(result['data'], list):
            raise ValueError(f"Expected dictionary output. Got list.")
        model = validator(**result['data'])
        cleaned = self.sanitize_response(model.model_dump())
        cleaned_txt = json.dumps(cleaned, indent=2)
        return cleaned_txt

    def sanitize_response(self, data: dict | list | str | Any) -> dict | list | str | Any:
        """Recursively sanitize HTML from response data."""
        if isinstance(data, dict):
            return {k: self.sanitize_response(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.sanitize_response(v) for v in data]
        elif isinstance(data, str):
            return cleaner.clean(data)
        else:
            return data

    @staticmethod
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
            'error': ""
        }

        if not response or not isinstance(response, str):
            result['error'] = 'Invalid input: response must be a non-empty string'
            return result

        # Try simply parsing it first
        try:
            result['raw_match'] = response
            cleaned_match = re.sub(r'\s+', ' ', response).strip()
            result['data'] = json.loads(cleaned_match)
            result['success'] = True
            return result
        except json.JSONDecodeError as e:
            pass

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

    @staticmethod
    def validate_input(system: str, messages: list[Message]) -> None:
        prompt_length = len(system) + sum([len(m.content) for m in messages])
        if prompt_length >= CONTEXT_WINDOW:
            logger.error(f"Prompt length {prompt_length} exceeds context window {CONTEXT_WINDOW}")
        if prompt_length >= TOKEN_LIMIT:
            logger.error(f"Prompt length {prompt_length} exceeds rate limit of {TOKEN_LIMIT} / minute.")
            raise ValueError(
                f"Prompt length {prompt_length} exceeds rate limit of {TOKEN_LIMIT} / minute.")  # TODO: dont let this crash the circuit
