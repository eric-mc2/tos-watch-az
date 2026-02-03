from typing import Protocol, Any

class HttpResponseProtocol(Protocol):
    """Protocol for HTTP response objects"""
    status_code: int
    text: str

    def json(self) -> Any: ...

    def raise_for_status(self) -> None: ...


class HttpProtocol(Protocol):
    """Protocol for HTTP client operations"""
    
    def get(self, url: str, **kwargs) -> HttpResponseProtocol: ...
