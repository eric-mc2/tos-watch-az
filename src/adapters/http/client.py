import requests
from src.adapters.http.protocol import HttpProtocol


class RequestsAdapter(HttpProtocol):
    """Production HTTP adapter using requests library"""

    def get(self, url: str, **kwargs) -> requests.Response:
        return requests.get(url, **kwargs)

