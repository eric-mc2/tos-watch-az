from src.adapters.llm.protocol import LLMProtocol, Message

class FakeLLMAdapter(LLMProtocol):
    _response = ""

    def call(self, system: str, messages: list[Message]) -> str:
        return self._response
    
    def close(self):
        pass

    def set_response(self, response):
        self._response = response

    def count_tokens(self, system: str, messages: list[Message]) -> int:
        return 1