from dataclasses import dataclass
from typing import Protocol, List


@dataclass
class Message:
    role: str
    content: str

@dataclass
class PromptMessages:
    system: str
    history: List[Message]
    current: Message

class LLMProtocol(Protocol):

    def call(self, system: str, messages: list[Message]) -> str: ...

    def close(self) -> None: ...