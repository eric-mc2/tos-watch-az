# This has to be in its own file or else pickle cant un-pickle it.
from dataclasses import dataclass
import json
from typing import Self


@dataclass
class DocChunk:
    company: str
    policy: str
    version_ts: str
    chunk_idx: int
    text: str

    @classmethod
    def from_dict(cls, d: dict) -> Self:
        return cls(**d)
    
    @classmethod
    def from_str(cls, s: str) -> Self:
        return cls.from_dict(json.loads(s))
    
    def as_dict(self):
        d = dict(
            company = self.company,
            policy = self.policy,
            version_ts = self.version_ts,
            chunk_idx = self.chunk_idx,
            text = self.text
        )
        return d
    
    def __str__(self):
        return json.dumps(self.as_dict(), sort_keys=False)
        
    def __repr__(self):
        return json.dumps(self.as_dict(), sort_keys=False)
    