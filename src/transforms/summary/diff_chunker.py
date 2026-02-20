from dataclasses import dataclass
from typing import List, Protocol

from src.adapters.llm.protocol import Message
from src.services.llm import LLMService
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.chunker import chunk_list, chunk_string


@dataclass
class TextChunk:
    """A token-limited chunk ready for LLM consumption, with provenance tracking.

    Attributes:
        text: The chunk content formatted according to the chosen formatter.
        parent_index: Index of the originating DiffSection.
        index: Page index this chunk belongs to.
    """
    text: str
    parent_index: int
    index: int

    def format(self) -> str:
        parts = [f"Section: {self.parent_index}",
                 f"Sub-Section: {self.index}",
                 self.text]
        return "\n".join(parts)


class DiffFormatter(Protocol):
    """Protocol for formatting DiffSections into text for LLM consumption."""
    
    def format_section(self, section: DiffSection) -> str:
        """Format a single DiffSection."""
        ...
    
    def format_doc(self, doc: DiffDoc) -> str:
        """Format an entire DiffDoc."""
        ...


@dataclass
class DiffChunker:
    """Splits a DiffDoc into token-limited pages of TextChunks.

    Two-phase process:
      1. **Group**: partition the DiffDoc's sections into batches that fit
         within the token budget (non-overlapping).
      2. **Paginate** each group:
         - Multiple sections → format each as a structured chunk.
         - Single oversized section → format, then split into plain-text fragments.
    
    The formatter is used consistently for both length calculations and output,
    ensuring parity regardless of document size.
    """
    llm: LLMService
    token_limit: int
    formatter: DiffFormatter
    headroom: float = 0.9

    @property
    def _effective_limit(self) -> int:
        return int(self.token_limit * self.headroom)

    def chunk_diff(self, system: str, history: list[Message], doc: DiffDoc) -> List[List[TextChunk]]:
        """Split a DiffDoc into token-limited pages of TextChunks.

        Each page is a list of TextChunks that collectively fit within the
        token limit.  Pages do NOT overlap.
        """
        groups = self._group_sections(system, doc)
        pages: List[List[TextChunk]] = []
        for sections in groups:
            pages.extend(self._paginate_group(system, sections))
        return pages

    # -- Phase 1: group sections into token-limited batches -----------------

    def _estimate_tokens(self, system: str, text: str) -> int:
        """Count tokens for a user message containing *text*."""
        return self.llm.adapter.count_tokens(system, [Message("user", text)])

    def _group_sections(self, system: str, doc: DiffDoc) -> List[List[DiffSection]]:
        """Partition DiffSections into groups that fit within the token budget.
        
        Uses formatted text for length calculations to ensure accuracy, including
        the overhead of TextChunk formatting (Section: N, Sub-Section: M).
        """
        # Calculate total length using formatted text wrapped in TextChunks
        # to include the overhead of Section/Sub-Section labels
        sample_chunks = [
            TextChunk(
                text=self.formatter.format_section(section),
                parent_index=section.index,
                index=0
            )
            for section in doc.diffs
        ]
        formatted_txt = "\n".join(chunk.format() for chunk in sample_chunks)
        text_len = len(formatted_txt)
        token_len = self._estimate_tokens(system, formatted_txt)
        
        # Use a length function that accounts for both formatter and TextChunk overhead
        def section_length(section: DiffSection) -> int:
            # Create a dummy TextChunk to get the full formatted length
            chunk = TextChunk(
                text=self.formatter.format_section(section),
                parent_index=section.index,
                index=0
            )
            return len(chunk.format())
        
        return chunk_list(doc.diffs, self._effective_limit, text_len, token_len, item_length_fn=section_length)

    # -- Phase 2: convert each group into one or more pages -----------------

    def _paginate_group(self, system: str, sections: List[DiffSection]) -> List[List[TextChunk]]:
        """Convert a group of sections into pages of TextChunks.

        A multi-section group is guaranteed to fit and yields one page of
        formatted chunks.  A single-section group may exceed the limit and
        is split into plain-text fragment pages.
        """
        if not sections:
            return []
        if len(sections) > 1:
            return [self._format_sections(sections)]
        return self._split_oversized_section(system, sections[0])

    def _format_sections(self, sections: List[DiffSection]) -> List[TextChunk]:
        """Create structured TextChunks from sections that fit within the limit."""
        return [
            TextChunk(
                text=self.formatter.format_section(section),
                parent_index=section.index,
                index=0,  # Each section produces one chunk, so index is always 0
            )
            for section in sections
        ]

    def _split_oversized_section(self, system: str, section: DiffSection) -> List[List[TextChunk]]:
        """Split a single oversized section into plain-text fragment pages.

        The section is formatted first, then split into fragments. Each fragment
        is wrapped in a TextChunk with overhead (Section: N, Sub-Section: M).
        Length calculations account for this overhead to ensure each formatted
        chunk fits within the token limit.
        """
        # Format the section using the formatter
        formatted_section_text = self.formatter.format_section(section)

        # Calculate the overhead for a TextChunk with this parent_index
        # Use index=0 as representative (actual indexes may vary by 1-2 chars)
        dummy_chunk = TextChunk(text="", parent_index=section.index, index=0)
        overhead_text = dummy_chunk.format()  # "Section: N\nSub-Section: 0\n"
        overhead_len = len(overhead_text)
        overhead_tokens = self._estimate_tokens(system, overhead_text)
        
        # Calculate available space after accounting for overhead
        available_token_limit = self._effective_limit - overhead_tokens
        
        # Guard against edge cases
        if available_token_limit <= 0:
            # Overhead alone exceeds limit; return formatted section as-is
            return [[TextChunk(text=formatted_section_text, parent_index=section.index, index=0)]]

        # Calculate text->token ratio for the formatted section
        section_text_len = len(formatted_section_text)
        section_token_len = self._estimate_tokens(system, formatted_section_text)

        if section_token_len == 0 or section_text_len == 0:
            return [[TextChunk(text=formatted_section_text, parent_index=section.index, index=0)]]

        # Split the formatted section into fragments that fit in available space
        # (after subtracting overhead)
        fragments = chunk_string(formatted_section_text, available_token_limit,
                                section_text_len, section_token_len)

        # Filter out empty fragments
        fragments = [f for f in fragments if f.strip()]

        # If no valid fragments, return the original formatted text
        if not fragments:
            return [[TextChunk(text=formatted_section_text, parent_index=section.index, index=0)]]

        # Each fragment becomes a TextChunk with its own overhead
        return [
            [TextChunk(
                text=fragment, # TODO: prefix with (+) or (-) diff annotation
                parent_index=section.index,
                index=i,  # Sub-index into this diff, resets to 0 per diff
            )]
            for i, fragment in enumerate(fragments)
        ]


# -- Formatting helpers (importable by other modules) ----------------------

class StandardDiffFormatter:
    """Standard formatter for DiffSections with Before/After labels."""

    @staticmethod
    def format_section(section: DiffSection) -> str:
        """Format a single DiffSection into readable text for the LLM."""
        parts = [f"Before:\n{section.before}",
                f"After:\n{section.after}"]
        return "\n".join(parts)
    
    def format_doc(self, doc: DiffDoc) -> str:
        """Format an entire DiffDoc into readable text for the LLM."""
        return "\n".join(map(self.format_section, doc.diffs))
