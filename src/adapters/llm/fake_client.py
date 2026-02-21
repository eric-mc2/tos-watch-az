from src.adapters.llm.protocol import LLMProtocol, Message

class FakeLLMAdapter(LLMProtocol):
    _response = ""
    _response_func = None

    def call(self, system: str, messages: list[Message]) -> str:
        if self._response_func is not None:
            resp = self._response_func(system, messages)
        else:
            resp = self._response
        return self._truncate(resp)
    
    def _truncate(self, txt: str) -> str:
        """Makes responses self-consistent with output param."""
        return txt[:self.get_max_output()]

    def close(self):
        pass

    def set_response_static(self, response):
        self._response = response

    def set_response_func(self, func):
        self._response_func = func

    def count_tokens(self, system: str, messages: list[Message]) -> int:
        return len(system) + sum(len(m.content) for m in messages)
    
    def get_max_output(self):
        return 9999

    def get_model_version(self) -> str:
        return "parrot"