from dataclasses import dataclass

# This has to be in its own file or else pickle cant un-pickle it.
@dataclass
class DocChunk():
    company: str
    policy: str
    version_ts: str
    chunk_idx: int
    text: str