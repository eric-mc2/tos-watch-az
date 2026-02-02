import requests
from src.clients.http.protocol import HttpProtocol, HttpResponseProtocol


class RequestsAdapter(HttpProtocol):
    """Production HTTP adapter using requests library"""
    
    @staticmethod
    def get(url: str, **kwargs) -> HttpResponseProtocol:
        return requests.get(url, **kwargs)

