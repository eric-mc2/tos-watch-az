from typing import Any, Optional
import json as json_lib
from requests import HTTPError
from src.adapters.http.protocol import HttpResponseProtocol, HttpProtocol


class FakeHttpResponse(HttpResponseProtocol):
    """Fake HTTP response for testing"""

    def __init__(self, status_code: int = 200, text: str = "", json_data: Optional[Any] = None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data

    def json(self):
        if self._json_data is not None:
            return self._json_data
        return json_lib.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")


class FakeHttpAdapter(HttpProtocol):
    """Fake HTTP client for testing with configurable responses"""

    def __init__(self) -> None:
        self._responses : dict[str, FakeHttpResponse] = {}
        self._default_response = FakeHttpResponse()
        self._error_response: Optional[FakeHttpResponse] = None
        self._call_count = 0
        self._error_until_call: Optional[int] = None

    def configure_response(self, url: str, response: FakeHttpResponse):
        """Configure a specific response for a URL"""
        self._responses[url] = response

    def configure_default_response(self, response: FakeHttpResponse):
        """Configure default response for unconfigured URLs"""
        self._default_response = response

    def configure_error(self, status_code: int, until_call: Optional[int] = None):
        """Configure the client to return an error response"""
        self._error_response = FakeHttpResponse(status_code=status_code)
        self._error_until_call = until_call

    def get(self, url: str, **kwargs) -> FakeHttpResponse:
        self._call_count += 1

        if self._error_response:
            if self._error_until_call is None or self._call_count <= self._error_until_call:
                return self._error_response

        return self._responses.get(url, self._default_response)

    def reset(self):
        """Reset the fake client state"""
        self._responses = {}
        self._default_response = FakeHttpResponse()
        self._error_response = None
        self._call_count = 0
        self._error_until_call = None
