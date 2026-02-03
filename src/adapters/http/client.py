import requests
from src.adapters.http.protocol import HttpProtocol, HttpResponseProtocol


class RequestsAdapter(HttpProtocol):
    """Production HTTP adapter using requests library"""

    def get(self, url: str, **kwargs) -> HttpResponseProtocol:
        return requests.get(url, **kwargs) # type: ignore

