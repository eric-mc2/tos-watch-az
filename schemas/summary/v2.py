from pydantic import BaseModel

class Substantive(BaseModel):
    rating: bool
    reason: str

class Summary(BaseModel):
    practically_substantive: Substantive