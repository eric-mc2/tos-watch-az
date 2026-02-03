from dataclasses import dataclass
from typing import Protocol, List, Literal


@dataclass
class Message:
    role: Literal["user", "assistant"]
    content: str

@dataclass
class PromptMessages:
    system: str
    history: List[Message]
    current: Message

class LLMProtocol(Protocol):

    def call(self, system: str, messages: list[Message]) -> str: ...

    def close(self) -> None: ...