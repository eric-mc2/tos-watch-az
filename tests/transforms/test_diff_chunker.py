import pytest
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.services.llm import LLMService, TOKEN_LIMIT
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.summary.diff_chunker import DiffChunker, StandardDiffFormatter


@pytest.fixture
def llm_service():
    return LLMService(FakeLLMAdapter())


class TestDiffChunkerFormatting:
    """Test that formatting is consistent regardless of document size."""

    def test_small_doc_uses_formatter(self, llm_service):
        """Small documents should be formatted using the configured formatter."""
        doc = DiffDoc(diffs=[DiffSection(index=0, before="before", after="after")])
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        assert len(pages) == 1
        assert len(pages[0]) == 1
        chunk = pages[0][0]
        
        # chunk.text should contain the DiffFormatter output
        assert "Before:\nbefore" in chunk.text
        assert "After:\nafter" in chunk.text
        assert '{"index"' not in chunk.text  # Should NOT be JSON
        
        # chunk.format() should include Section labels plus formatted text
        formatted = chunk.format()
        assert "Section: 0" in formatted
        assert "Sub-Section: 0" in formatted
        assert "Before:\nbefore" in formatted
        assert "After:\nafter" in formatted

    def test_large_doc_uses_formatter(self, llm_service):
        """Large documents should also be formatted using the configured formatter."""
        # Create an oversized section
        large_text = "11 22 33 " * int(TOKEN_LIMIT * 0.9)
        doc = DiffDoc(diffs=[DiffSection(index=0, before=large_text, after="after")])
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # Should be chunked into multiple pages
        assert len(pages) >= 2
        
        # Each chunk should contain formatted text from StandardDiffFormatter
        # The first chunk should contain the "Before:" header
        first_chunk = pages[0][0]
        assert "Before:" in first_chunk.text
        
        # All chunks should be plain text fragments from the formatted output
        # (not JSON with {"index": ...})
        for page in pages:
            for chunk in page:
                # Should NOT be raw JSON
                assert not chunk.text.strip().startswith('{"index"')
                # The formatted output should include Section labels
                formatted = chunk.format()
                assert "Section: 0" in formatted

    def test_formatted_length_calculation(self, llm_service):
        """Length calculations should account for formatting overhead."""
        
        # Create sections that would fit without formatting overhead
        # but might not fit with it
        N = 200
        sections = [
            DiffSection(index=i, before="hello", after="world")
            for i in range(N)
        ]
        doc = DiffDoc(diffs=sections)
        
        formatter = StandardDiffFormatter()
        limit = len("hello" + "world") * N / 4
        chunker = DiffChunker(llm_service, limit, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        assert len(pages) >= 2

        # Verify that each page actually fits within the token limit
        from src.adapters.llm.protocol import Message
        for page in pages:
            combined_text = "".join(chunk.text for chunk in page)
            tokens = llm_service.adapter.count_tokens("system", [Message("user", combined_text)])
            assert tokens <= chunker._effective_limit, f"Page exceeds token limit: {tokens} > {chunker._effective_limit}"


class TestDiffChunker:
    def test_multiple_long(self, llm_service):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=i,
                                           before="before",
                                           after="after")
                               for i in range(TOKEN_LIMIT)])

        chunker = DiffChunker(llm_service, TOKEN_LIMIT, StandardDiffFormatter())

        # Act
        chunks = chunker.chunk_diff("system", [], data)

        # Assert
        for cc in chunks:
            assert sum((len(c.text) for c in cc)) < TOKEN_LIMIT
            assert sum((len(c.format()) for c in cc)) < TOKEN_LIMIT

    def test_long_lines(self, llm_service):
        # Arrange
        before_txt = "\n".join(["abc def "*10000]*10)
        after_txt = "\n".join(["qrs tuv "*10000]*10)
        data  = DiffDoc(diffs=[DiffSection(index=0,
                                           before=before_txt,
                                           after=after_txt)])

        chunker = DiffChunker(llm_service, TOKEN_LIMIT, StandardDiffFormatter())

        # Act
        chunks = chunker.chunk_diff("system", [], data)

        # Assert
        assert len(chunks) > 1
        for cc in chunks:
            assert sum((len(c.text) for c in cc)) < TOKEN_LIMIT
            assert sum((len(c.format()) for c in cc)) < TOKEN_LIMIT
    
    def test_single_long_line(self, llm_service):
        # Arrange
        before_txt = "abc def "*100000
        after_txt = "qrs tuv "*100000
        data  = DiffDoc(diffs=[DiffSection(index=0,
                                           before=before_txt,
                                           after=after_txt)])

        chunker = DiffChunker(llm_service, TOKEN_LIMIT, StandardDiffFormatter())

        # Act
        chunks = chunker.chunk_diff("system", [], data)

        # Assert
        assert len(chunks) > 1
        for cc in chunks:
            assert sum((len(c.text) for c in cc)) < TOKEN_LIMIT
            assert sum((len(c.format()) for c in cc)) < TOKEN_LIMIT

class TestDiffChunkerIndexing:
    """Test that TextChunk indexes are correct according to requirements:
    - parent_index points to the original diff's index
    - index is a sub-index into a diff (resets to 0 per diff, continues across pages)
    - index should NOT be the page index and NOT a global index
    """

    def test_multiple_sections_single_page_indexes(self, llm_service):
        """Multiple sections on one page should each have index=0 with different parent_indexes."""
        sections = [
            DiffSection(index=0, before="first", after="changed"),
            DiffSection(index=1, before="second", after="modified"),
            DiffSection(index=2, before="third", after="updated"),
        ]
        doc = DiffDoc(diffs=sections)
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # Should fit in one page
        assert len(pages) == 1
        chunks = pages[0]
        
        # Each section should have index=0 (since each produces only one chunk)
        assert len(chunks) == 3
        assert chunks[0].parent_index == 0
        assert chunks[0].index == 0
        assert chunks[1].parent_index == 1
        assert chunks[1].index == 0
        assert chunks[2].parent_index == 2
        assert chunks[2].index == 0

    def test_oversized_section_split_across_pages(self, llm_service):
        """One oversized section split across pages should have sequential indexes (0, 1, 2, ...)."""
        # Create a section large enough to be split
        large_text = "word " * int(TOKEN_LIMIT * 0.9)
        doc = DiffDoc(diffs=[DiffSection(index=5, before=large_text, after="after")])
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # Should be split into multiple pages
        assert len(pages) >= 2
        
        # Each page should have one chunk (from the same diff)
        # All should have the same parent_index but increasing index
        for i, page in enumerate(pages):
            assert len(page) == 1
            chunk = page[0]
            assert chunk.parent_index == 5  # Original diff index
            assert chunk.index == i  # Sub-index: 0, 1, 2, ...

    def test_mixed_normal_and_oversized_sections(self, llm_service):
        """Mixed scenario: some normal sections and one oversized section."""
        large_text = "word " * int(TOKEN_LIMIT * 0.9)
        sections = [
            DiffSection(index=0, before="small1", after="after1"),
            DiffSection(index=1, before=large_text, after="after2"),  # Will be split
            DiffSection(index=2, before="small3", after="after3"),
        ]
        doc = DiffDoc(diffs=sections)
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # Section 0 should be on one page with index=0
        # Section 1 will be split across multiple pages with index=0, 1, 2, ...
        # Section 2 should be on one page with index=0
        
        # Find chunks by parent_index
        all_chunks = [chunk for page in pages for chunk in page]
        
        # Section 0 chunks
        section_0_chunks = [c for c in all_chunks if c.parent_index == 0]
        assert len(section_0_chunks) == 1
        assert section_0_chunks[0].index == 0
        
        # Section 1 chunks (oversized, split)
        section_1_chunks = [c for c in all_chunks if c.parent_index == 1]
        assert len(section_1_chunks) >= 2  # Should be split
        # Check that indexes are sequential: 0, 1, 2, ...
        for i, chunk in enumerate(section_1_chunks):
            assert chunk.index == i
        
        # Section 2 chunks
        section_2_chunks = [c for c in all_chunks if c.parent_index == 2]
        assert len(section_2_chunks) == 1
        assert section_2_chunks[0].index == 0

    def test_parent_index_matches_original_diff_index(self, llm_service):
        """Verify parent_index always points to the original diff's index."""
        sections = [
            DiffSection(index=10, before="a", after="b"),
            DiffSection(index=25, before="c", after="d"),
            DiffSection(index=99, before="e", after="f"),
        ]
        doc = DiffDoc(diffs=sections)
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # Flatten all chunks
        all_chunks = [chunk for page in pages for chunk in page]
        
        # Verify parent_indexes match the original diff indexes
        parent_indexes = {c.parent_index for c in all_chunks}
        assert parent_indexes == {10, 25, 99}
        
        # Each chunk should have the correct parent_index
        for chunk in all_chunks:
            assert chunk.parent_index in {10, 25, 99}

    def test_index_not_page_index(self, llm_service):
        """Verify that index is NOT the page index."""
        sections = [
            DiffSection(index=0, before="a", after="b"),
            DiffSection(index=1, before="c", after="d"),
        ]
        doc = DiffDoc(diffs=sections)
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # If there are multiple sections on one page, they should both have index=0
        # (not different indexes based on page position)
        if len(pages) == 1 and len(pages[0]) >= 2:
            chunks = pages[0]
            # All chunks from different diffs should have index=0
            assert all(c.index == 0 for c in chunks)
    
    def test_index_not_global_index(self, llm_service):
        """Verify that index is NOT a global index across all chunks."""
        # Create oversized sections that will be split
        large_text = "word " * int(TOKEN_LIMIT * 0.9)
        sections = [
            DiffSection(index=0, before=large_text, after="after1"),  # Will be split
            DiffSection(index=1, before=large_text, after="after2"),  # Will be split
        ]
        doc = DiffDoc(diffs=sections)
        formatter = StandardDiffFormatter()
        chunker = DiffChunker(llm_service, TOKEN_LIMIT, formatter)

        pages = chunker.chunk_diff("system", [], doc)

        # Both sections should be split
        all_chunks = [chunk for page in pages for chunk in page]
        
        # Section 0 chunks should have index 0, 1, 2, ...
        section_0_chunks = [c for c in all_chunks if c.parent_index == 0]
        assert len(section_0_chunks) >= 2
        for i, chunk in enumerate(section_0_chunks):
            assert chunk.index == i  # Resets to 0 for this diff
        
        # Section 1 chunks should ALSO have index 0, 1, 2, ... (not continuing from section 0)
        section_1_chunks = [c for c in all_chunks if c.parent_index == 1]
        assert len(section_1_chunks) >= 2
        for i, chunk in enumerate(section_1_chunks):
            assert chunk.index == i  # Resets to 0 for this diff (not a global index)
