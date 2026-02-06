from typing import Protocol
import requests


class HttpProtocol(Protocol):
    """Protocol for HTTP client operations"""
    
    def get(self, url: str, **kwargs) -> requests.Response: ...
    
    def get_and_raise(self, url: str, **kwargs) -> requests.Response:
        resp = self.get(url, **kwargs)
        resp.raise_for_status()
        return resp
