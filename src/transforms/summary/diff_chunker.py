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


class DiffFormatter(Protocol):
    """Protocol for formatting DiffSections into text for LLM consumption."""
    
    def format_section(self, section: DiffSection, index: int) -> str:
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
            pages.extend(self._paginate_group(system, sections, page_offset=len(pages)))
        return pages

    # -- Phase 1: group sections into token-limited batches -----------------

    def _estimate_tokens(self, system: str, text: str) -> int:
        """Count tokens for a user message containing *text*."""
        return self.llm.adapter.count_tokens(system, [Message("user", text)])

    def _group_sections(self, system: str, doc: DiffDoc) -> List[List[DiffSection]]:
        """Partition DiffSections into groups that fit within the token budget.
        
        Uses formatted text for length calculations to ensure accuracy.
        """
        # Calculate total length using formatted text
        # Use 0 as placeholder index since we just need lengths, not actual output
        formatted_sections = [self.formatter.format_section(d, 0) for d in doc.diffs]
        text_len = sum(len(f) for f in formatted_sections)
        combined_text = "".join(formatted_sections)
        token_len = self._estimate_tokens(system, combined_text)
        return chunk_list(doc.diffs, self._effective_limit, text_len, token_len)

    # -- Phase 2: convert each group into one or more pages -----------------

    def _paginate_group(
        self, system: str, sections: List[DiffSection], page_offset: int
    ) -> List[List[TextChunk]]:
        """Convert a group of sections into pages of TextChunks.

        A multi-section group is guaranteed to fit and yields one page of
        formatted chunks.  A single-section group may exceed the limit and
        is split into plain-text fragment pages.
        """
        if not sections:
            return []
        if len(sections) > 1:
            return [self._format_sections(sections, page_offset)]
        return self._split_oversized_section(system, sections[0], page_offset)

    def _format_sections(
        self, sections: List[DiffSection], page_index: int
    ) -> List[TextChunk]:
        """Create structured TextChunks from sections that fit within the limit."""
        return [
            TextChunk(
                text=self.formatter.format_section(section, page_index),
                parent_index=section.index,
                index=page_index,
            )
            for section in sections
        ]

    def _split_oversized_section(
        self, system: str, section: DiffSection, page_offset: int
    ) -> List[List[TextChunk]]:
        """Split a single oversized section into plain-text fragment pages.

        The section is formatted first, then split into fragments to ensure
        consistency with the formatting of smaller sections.
        """
        # Format the section using the formatter
        formatted_text = self.formatter.format_section(section, page_offset)
        text_len = len(formatted_text)
        token_len = self._estimate_tokens(system, formatted_text)
        
        # Guard against edge cases
        if token_len == 0 or text_len == 0:
            return [[TextChunk(text=formatted_text, parent_index=section.index, index=page_offset)]]
        
        # Split the formatted text into fragments
        fragments = chunk_string(formatted_text, self._effective_limit, text_len, token_len)
        
        # Filter out empty fragments
        fragments = [f for f in fragments if f.strip()]
        
        # If no valid fragments, return the original formatted text
        if not fragments:
            return [[TextChunk(text=formatted_text, parent_index=section.index, index=page_offset)]]
        
        return [
            [TextChunk(
                text=fragment,
                parent_index=section.index,
                index=page_offset + i,
            )]
            for i, fragment in enumerate(fragments)
        ]


# -- Formatting helpers (importable by other modules) ----------------------

class StandardDiffFormatter:
    """Standard formatter for DiffSections with Before/After labels."""
    
    def format_section(self, section: DiffSection, index: int) -> str:
        """Format a single DiffSection into readable text for the LLM."""
        parts = [f"Section {index}:"]
        if section.before:
            parts.append(f"Before: {section.before}")
        if section.after:
            parts.append(f"After: {section.after}")
        return "\n".join(parts) + "\n"
    
    def format_doc(self, doc: DiffDoc) -> str:
        """Format an entire DiffDoc into readable text for the LLM."""
        return "\n".join(
            self.format_section(section, i) for i, section in enumerate(doc.diffs, 1)
        )
