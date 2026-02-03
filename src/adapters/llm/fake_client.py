from src.adapters.llm.protocol import LLMProtocol, Message

class FakeLLMAdapter(LLMProtocol):

    def call(self, system: str, messages: list[Message]) -> str:
        return "Hello world"