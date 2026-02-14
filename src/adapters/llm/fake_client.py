from src.adapters.llm.protocol import LLMProtocol, Message

class FakeLLMAdapter(LLMProtocol):
    _response = ""
    _response_func = None

    def call(self, system: str, messages: list[Message]) -> str:
        if self._response_func is not None:
            return self._response_func(system, messages)
        return self._response
    
    def close(self):
        pass

    def set_response_static(self, response):
        self._response = response

    def set_response_func(self, func):
        self._response_func = func

    def count_tokens(self, system: str, messages: list[Message]) -> int:
        return len(system) + sum(len(m.content) for m in messages)