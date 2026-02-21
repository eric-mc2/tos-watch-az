import json
from typing import Optional

import anthropic
import logging
import os
from dataclasses import asdict
from anthropic.types import MessageParam
from src.utils.log_utils import setup_logger
from src.adapters.llm.protocol import Message, LLMProtocol

logger = setup_logger(__name__, logging.DEBUG)
_client: Optional[anthropic.Anthropic] = None

class ClaudeAdapter(LLMProtocol):
    model_version = "claude-haiku-4-5-20251001"

    def __init__(self, max_output: int = 1000):
        self.max_output = max_output

    @staticmethod
    def _get_client() -> anthropic.Anthropic:
        # Note: we don't need to close the client. In practice it's better to keep one single
        # client open during the lifetime of the applicaiton. Not per function invocation.
        global _client
        if _client is None or _client.is_closed():
            key = os.environ.get('ANTHROPIC_API_KEY')
            if not key:
                raise ValueError("Missing environment variable ANTHROPIC_API_KEY")
            _client = anthropic.Anthropic(api_key=key)
        return _client


    def close(self) -> None:
        global _client
        if _client is not None:
            _client.close()
            _client = None


    def call(self, system: str, messages: list[Message]) -> str:
        if not system or not system.strip():
            raise ValueError("Claude API requires non-empty system text.")

        client = self._get_client()
        response = client.messages.create(**self._config_messages_for_model(system, messages))
        if response.stop_reason != 'end_turn':
            pass  # might need to fix
        if not response.content:
            raise ValueError("Empty LLM response")
        if len(response.content) > 1:
            logger.warning("Multiple LLM outputs")
        txt = response.content[0].text # type:ignore
        return txt
    

    def count_tokens(self, system: str, messages: list[Message]) -> int:
        client = self._get_client()
        response = client.messages.count_tokens(**self._config_messages(system, messages))
        return response.input_tokens
    

    def _config_messages(self, system: str, messages: list[Message]) -> dict:
        return dict(
            model=self.model_version,
            system=[{
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[MessageParam(content=m.content, role=m.role) for m in messages]
        )
    
    def _config_messages_for_model(self, system: str, messages: list[Message]) -> dict:
        return self._config_messages(system, messages) | dict(max_tokens=self.max_output)
            
    
    def get_max_output(self) -> int:
        return self.max_output


    def get_model_version(self) -> str:
        return self.model_version