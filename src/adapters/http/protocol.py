from typing import Protocol
import requests


class HttpProtocol(Protocol):
    """Protocol for HTTP client operations"""
    
    def get(self, url: str, **kwargs) -> requests.Response: ...
