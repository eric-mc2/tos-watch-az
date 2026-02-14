from dataclasses import dataclass
from typing import List

from src.adapters.llm.protocol import Message
from src.services.llm import LLMService
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.chunker import chunk_list, chunk_string

@dataclass
class TextChunk:
    text: str
    parent_index: int
    index: int

@dataclass
class DiffChunker:
    llm: LLMService
    token_limit: int
    headroom: float = 0.9

    def chunk_diff(self, system: str, history: list[Message], doc: DiffDoc) -> List[List[TextChunk]]:
        # First break DiffDoc's list of diffs into smaller DiffDocs with smaller lists.
        # These text do NOT overlap.
        # Not sure if I really want to use the full context or not ...
        # Maybe static 1K tokens is better?
        token_limit = int(self.token_limit * self.headroom)
        text_len = sum(map(len, doc.diffs))
        test_msgs = [Message("user", d.before + d.after) for d in doc.diffs]
        token_len = self.llm.adapter.count_tokens(system, test_msgs)
        chunks = chunk_list(doc.diffs, token_limit, text_len, token_len)
        docs = (DiffDoc(diffs=x) for x in chunks)

        # Check each DiffDoc and break up very large diffs into smaller overlapping sections.
        pages = []
        for doc in docs:
            if len(doc.diffs) > 1:
                # If multiple diffs are in here its guaranteed to be under the limit
                pages.append([TextChunk(parent_index=d.index,
                                        index=len(pages),
                                        text=_format_diff(d, len(pages)))
                              for d in doc.diffs])
            else:
                # It's a single item and it's probably too big.
                diff = doc.diffs[0]
                diff_text = diff.model_dump_json()
                text_len = len(diff_text)
                token_len = self.llm.adapter.count_tokens(system, [Message("user", diff.before + diff.after)])
                texts = chunk_string(diff_text, token_limit, text_len, token_len)
                # These have erased the before / after info so don't pretend to use it.
                new_docs = ([TextChunk(parent_index=doc.index,
                                        index=len(pages)+i,
                                        text=t)] for i,t in enumerate(texts))
                pages.extend(new_docs)
        return pages


def _format_diff(diff: DiffSection, i: int) -> str:
    """Format DiffDoc into readable context for the LLM."""
    section = f"Section {i}:\n"
    if diff.before:
        section += f"Before: {diff.before}\n"
    if diff.after:
        section += f"After: {diff.after}\n"
    return section

def _format_doc(doc: DiffDoc) -> str:
    """Format DiffDoc into readable context for the LLM."""
    formatted_sections = (_format_diff(diff, i)
                          for i, diff in enumerate(doc.diffs, 1))
    return "\n".join(formatted_sections)

