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
        
        # Should contain formatted output, not raw JSON
        assert "Section 0:" in chunk.text
        assert "Before: before" in chunk.text
        assert "After: after" in chunk.text
        assert '{"index"' not in chunk.text  # Should NOT be JSON

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
        
        # The first chunk should contain the formatted headers
        first_chunk = pages[0][0]
        assert "Section 0:" in first_chunk.text or "Before:" in first_chunk.text
        
        # All chunks should be plain text fragments from the formatted output
        # (not JSON with {"index": ...})
        for page in pages:
            for chunk in page:
                # Should NOT be raw JSON
                assert not chunk.text.strip().startswith('{"index"')

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
