from datetime import datetime
from typing import Optional
from requests import Response
from src.adapters.http.protocol import HttpProtocol
from urllib.parse import urlparse


class FakeHttpAdapter(HttpProtocol):
    """Fake HTTP client for testing with configurable responses"""

    def __init__(self) -> None:
        self._responses: dict[str, Response] = {}
        self._default_response = self._create_response()
        self._error_response: Optional[Response] = None
        self._call_count = 0
        self._error_until_call: Optional[int] = None

    def _create_response(self,
                         status_code: int = 200,
                         text: str | bytes = "",
                         json_data: Optional[dict] = None,
                         headers: Optional[dict] = None) -> Response:
        """Create a fake requests.Response object"""
        response = Response()
        response.status_code = status_code
        response._content = text.encode('utf-8') if isinstance(text, str) and text else text if text else b''
        if response.headers is None and headers is not None:
            response.headers = headers
        elif headers is not None:
            response.headers.update(headers)

        if json_data is not None:
            import json
            response._content = json.dumps(json_data).encode('utf-8')
            response.headers['Content-Type'] = 'application/json'
        
        return response

    def configure_response(self,
                           url: str,
                           status_code: int = 200,
                           text: str | bytes = "",
                           json_data: Optional[dict] = None):
        """Configure a specific response for a URL"""
        self._responses[url] = self._create_response(status_code, text, json_data)

    def configure_default_response(self,
                                   status_code: int = 200,
                                   text: str | bytes = "",
                                   json_data: Optional[dict] = None,
                                   headers: Optional[dict] = None):
        """Configure default response for unconfigured URLs"""
        self._default_response = self._create_response(status_code, text, json_data, headers)

    def configure_error(self, status_code: int, until_call: Optional[int] = None):
        """Configure the client to return an error response"""
        self._error_response = self._create_response(status_code=status_code)
        self._error_until_call = until_call

    def get(self, url: str, **kwargs) -> Response:
        self._call_count += 1

        if self._error_response is not None:
            if self._error_until_call is None or self._call_count <= self._error_until_call:
                return self._error_response

        return self._responses.get(url, self._default_response)

    def reset(self):
        """Reset the fake client state"""
        self._responses = {}
        self._default_response = self._create_response()
        self._error_response = None
        self._call_count = 0
        self._error_until_call = None
