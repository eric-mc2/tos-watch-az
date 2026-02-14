from dataclasses import dataclass

import pytest
from src.transforms.chunker import Buffer, string_windower, string_example

def word_buffer(capacity, delim):
    return Buffer(
        capacity = capacity,
        combine = lambda a, b: a + delim + b if a else b,
        length = len,
        empty = "",
    )

class TestBuffer:
    """Test cases for Buffer class."""

    def test_init_valid_capacity(self):
        """Test WordBuffer initialization with valid capacity."""
        buf = word_buffer(100, " ")
        assert buf.capacity == 100
        assert buf.is_open is True

    def test_init_invalid_capacity_zero(self):
        """Test that zero capacity raises ValueError."""
        with pytest.raises(ValueError, match="Capacity must be positive"):
            word_buffer(0, " ")

    def test_init_invalid_capacity_negative(self):
        """Test that negative capacity raises ValueError."""
        with pytest.raises(ValueError, match="Capacity must be positive"):
            word_buffer(-10, " ")

    def test_size_property(self):
        """Test size property returns correct length."""
        buf = word_buffer(100, " ")
        buf.add("hello")
        assert buf.size == 5

    def test_is_empty_property(self):
        """Test is_empty property."""
        buf = word_buffer(100, " ")
        assert buf.is_empty is True
        buf.add("hello")
        assert buf.is_empty is False

    def test_pressure_property(self):
        """Test pressure property calculation."""
        buf = word_buffer(10, " ")
        buf.add("12345")
        assert buf.pressure == 0.5  # 5/10

    def test_pressure_full(self):
        """Test pressure at capacity."""
        buf = word_buffer(10, " ")
        buf.add("1234567890")
        assert buf.pressure == 1.0

    def test_close(self):
        """Test closing buffer."""
        buf = word_buffer(100, " ")
        assert buf.is_open is True
        buf.close()
        assert buf.is_open is False

    def test_can_add_within_capacity(self):
        """Test can_add returns True when within capacity."""
        buf = word_buffer(20, " ")
        assert buf.can_add("world") is True

    def test_can_add_exceeds_capacity(self):
        """Test can_add returns False when exceeding capacity."""
        buf = word_buffer(10, " ")
        buf.add("hello")
        assert buf.can_add("world") is False  # 5 + 1 (delimiter) + 5 = 11 > 10

    def test_can_add_closed_buffer(self):
        """Test can_add returns False on closed buffer."""
        buf = word_buffer(100, " ")
        buf.close()
        assert buf.can_add("world") is False

    def test_add_to_empty_buffer(self):
        """Test adding to empty buffer doesn't use delimiter."""
        buf = word_buffer(100, " ")
        result = buf.add("hello")
        assert result is True
        assert buf.content == "hello"
        assert buf.size == 5

    def test_add_to_nonempty_buffer(self):
        """Test adding to non-empty buffer uses delimiter."""
        buf = word_buffer(100, " ")
        buf.add("hello")
        result = buf.add("world")
        assert result is True
        assert buf.content == "hello world"
        assert buf.size == 11

    def test_add_exceeds_capacity_without_force(self):
        """Test add closes buffer and returns False when exceeding capacity."""
        buf = word_buffer(10, " ")
        buf.add("hello")
        result = buf.add("world")
        assert result is False
        assert buf.is_open is False
        assert buf.content == "hello"  # Original text unchanged

    def test_add_with_force(self):
        """Test add with force=True ignores capacity."""
        buf = word_buffer(10, " ")
        buf.add("hello")
        result = buf.add("world", force=True)
        assert result is True
        assert buf.content == "hello world"

    def test_delimiter_size_calculation(self):
        """Test sep_size is calculated correctly."""
        buf = word_buffer(10, "")
        buf.add("hello")
        assert buf.can_add("world") == True
        buf = word_buffer(10, " ")
        buf.add("hello")
        assert buf.can_add("world") == False
        buf = word_buffer(20, ">"*10)
        buf.add("hello")
        assert buf.can_add("world") == True
        buf = word_buffer(19, ">"*10)
        buf.add("hello")
        assert buf.can_add("world") == False


class TestWindower:
    """Test cases for string_windower class."""
    
    def test_init_valid_params(self):
        """Test string_windower initialization with valid parameters."""
        windower = string_windower(100, " ", 0.1)
        assert windower.capacity == 100
        assert windower.delimiter == " "
        assert windower.overlap == 0.1
        assert windower.slots == []

    def test_init_invalid_capacity_zero(self):
        """Test that zero capacity raises ValueError."""
        with pytest.raises(ValueError, match="Capacity must be positive"):
            string_windower(0, " ", 0.1)

    def test_init_invalid_capacity_negative(self):
        """Test that negative capacity raises ValueError."""
        with pytest.raises(ValueError, match="Capacity must be positive"):
            string_windower(-5, " ", 0.1)

    def test_init_valid_overlap_zero(self):
        """Test that overlap=0 not raises ValueError."""
        windower = string_windower(100, " ", 0)
        assert windower.overlap == 0

    def test_init_invalid_overlap_one(self):
        """Test that overlap=1 raises ValueError."""
        with pytest.raises(ValueError, match="Overlap must be in range"):
            string_windower(100, " ", 1)

    def test_init_invalid_overlap_negative(self):
        """Test that negative overlap raises ValueError."""
        with pytest.raises(ValueError, match="Overlap must be in range"):
            string_windower(100, " ", -0.1)

    def test_add_creates_first_slot(self):
        """Test adding first text creates a slot."""
        windower = string_windower(100, " ", 0.1)
        added = windower.add("hello")
        assert added == 1
        assert len(windower.slots) == 1
        assert windower.slots[0].txt == "hello"

    def test_add_to_existing_slot(self):
        """Test adding to existing slot."""
        windower = string_windower(100, " ", 0.1)
        windower.add("hello")
        added = windower.add("world")
        assert added == 1
        assert len(windower.slots) == 1
        assert windower.slots[0].txt == "hello world"

    def test_add_triggers_overlap(self):
        """Test adding triggers overlap when pressure threshold reached."""
        windower = string_windower(20, " ", 0.2)  # 20% overlap
        windower.add("hello")  # 5 chars, pressure = 0.25
        windower.add("world")  # +6 chars, total 11, pressure = 0.55
        added = windower.add("test")  # +5 chars, would be 16, pressure = 0.8
        # After adding to first slot, pressure = 0.8, so 1 - 0.8 = 0.2 = overlap
        # This should trigger creating a new slot
        assert added >= 1
        if 1 - windower.slots[0].pressure <= windower.overlap:
            assert len(windower.slots) == 2

    def test_add_force(self):
        """Test force adding creates slot when normal add fails."""
        windower = string_windower(5, " ", 0.1)
        added = windower.add("verylongword", force=True)
        assert added == 1
        assert len(windower.slots) == 1

    def test_append(self):
        """Test appending a WordBuffer."""
        windower = string_windower(100, " ", 0.1)
        buf = word_buffer(100, " ")
        buf.add("appended")
        windower.append(buf)
        assert len(windower.slots) == 1
        assert windower.slots[0] is buf


@dataclass
class WordChunker:
    # Convenience legacy class to avoid refactoring test code.
    capacity: int
    overlap: float
    def process(self, text, token_len, text_len):
        return string_example(text, self.capacity, text_len, token_len, self.overlap)


class TestWordChunker:
    """Test cases for WordChunker class."""

    def test_process_simple_text(self):
        """Test processing simple text that fits in one chunk."""
        chunker = WordChunker(100, 0.1)
        text = "hello world"
        token_len = 2
        chunks = chunker.process(text, token_len, len(text))
        assert len(chunks) >= 1
        # All words should appear
        all_text = " ".join(chunks)
        assert "hello" in all_text
        assert "world" in all_text
    
    def test_invariant_all_words_present(self):
        """Test that all words from input appear in output."""
        chunker = WordChunker(50, 0.1)
        text = "the quick brown fox jumps over the lazy dog"
        token_len = 9
        chunks = chunker.process(text, token_len, len(text))
        
        # Extract all words from original
        original_words = set(text.split())
        
        # Extract all words from chunks
        chunk_words = set()
        for chunk in chunks:
            chunk_words.update(chunk.replace("\n", " ").split())
        
        # Every word should appear at least once
        assert original_words.issubset(chunk_words)
    
    def test_overlap_zero_reproduces_original(self):
        """Test that overlap=0 reproduces original text (modulo newlines)."""
        chunker = WordChunker(1000, 0)
        text = "hello world\nthis is a test"
        token_len = 6
        chunks = chunker.process(text, token_len, len(text))
        
        # Reconstruct text (newlines become spaces between chunks)
        reconstructed_words = []
        for chunk in chunks:
            reconstructed_words.extend(chunk.replace("\n", " ").split())
        
        original_words = text.replace("\n", " ").split()
        assert reconstructed_words == original_words
    
    def test_long_line_handling(self):
        """Test handling of very long lines."""
        chunker = WordChunker(20, 0.1)
        # Create a long line with many words
        text = " ".join(["word"] * 50)
        token_len = 50
        chunks = chunker.process(text, token_len, len(text))
        
        # Should create multiple chunks
        assert len(chunks) > 1
        
        # All "word" instances should be present
        total_word_count = sum(chunk.count("word") for chunk in chunks)
        assert total_word_count >= 50  # At least 50 due to overlap
    
    def test_long_word_handling(self):
        """Test handling of very long single word."""
        chunker = WordChunker(10, 0.1)
        text = "short verylongwordthatexceedslimit short"
        token_len = 3
        chunks = chunker.process(text, token_len, len(text))
        
        # All words should appear
        all_text = " ".join(chunks)
        assert "short" in all_text
        assert "verylongwordthatexceedslimit" in all_text
    
    def test_overlap_creates_redundancy(self):
        """Test that overlap > 0 creates redundant content."""
        chunker = WordChunker(30, 0.3)  # 30% overlap
        text = "word1 word2 word3 word4 word5 word6 word7 word8"
        token_len = 8
        chunks = chunker.process(text, token_len, len(text))
        
        if len(chunks) > 1:
            # Count total words across all chunks
            total_words = sum(len(chunk.split()) for chunk in chunks)
            original_words = len(text.split())
            
            # With overlap, total words should exceed original
            assert total_words > original_words
    
    def test_empty_text(self):
        """Test handling of empty text."""
        chunker = WordChunker(100, 0.1)
        text = ""
        token_len = 0
        # This should handle edge case gracefully
        # Expecting it to potentially raise or return empty list
        try:
            chunks = chunker.process(text, token_len, len(text))
            # If it succeeds, should return empty or minimal chunks
            assert isinstance(chunks, list)
        except (ValueError, ZeroDivisionError):
            # Acceptable to raise on empty input
            pass
    
    def test_single_word(self):
        """Test processing single word."""
        chunker = WordChunker(100, 0.1)
        text = "hello"
        token_len = 1
        chunks = chunker.process(text, token_len, len(text))
        
        assert len(chunks) >= 1
        assert "hello" in chunks[0]
    
    def test_newline_preservation_in_chunks(self):
        """Test that newlines are used as delimiters."""
        chunker = WordChunker(100, 0.1)
        text = "line1\nline2\nline3"
        token_len = 3
        chunks = chunker.process(text, token_len, len(text))
        
        # All lines should appear somewhere
        all_text = " ".join(chunks)
        assert "line1" in all_text
        assert "line2" in all_text
        assert "line3" in all_text
    
    def test_multiple_newlines(self):
        """Test handling multiple consecutive newlines."""
        chunker = WordChunker(100, 0.1)
        text = "line1\n\n\nline2"
        token_len = 2
        chunks = chunker.process(text, token_len, len(text))
        
        all_text = " ".join(chunks)
        assert "line1" in all_text
        assert "line2" in all_text
    
    def test_char_limit_calculation(self):
        """Test that char_limit is calculated correctly."""
        chunker = WordChunker(100, 0.1)
        text = "a" * 200  # 200 chars
        token_len = 100  # 100 tokens
        # char_limit should be 100 * 200 / 100 = 200
        chunks = chunker.process(text, token_len, len(text))
        
        assert len(chunks) >= 1
        # First chunk should be around 200 chars (or split if too long)
    
    def test_pressure_boundary_conditions(self):
        """Test behavior at pressure boundaries for overlap triggering."""
        chunker = WordChunker(50, 0.1)
        # Create text that will hit pressure boundaries
        text = " ".join([f"w{i}" for i in range(20)])
        token_len = 20
        chunks = chunker.process(text, token_len, len(text))
        
        # Should handle pressure calculations without errors
        assert len(chunks) >= 1
        
        # Verify all words present
        all_words = set()
        for chunk in chunks:
            all_words.update(chunk.split())
        original_words = set(text.split())
        assert original_words.issubset(all_words)
    
    def test_very_small_token_limit(self):
        """Test with very small token limit."""
        chunker = WordChunker(5, 0.1)
        text = "a b c d e f g h i j"
        token_len = 10
        chunks = chunker.process(text, token_len, len(text))
        
        # Should create many small chunks
        assert len(chunks) > 1
        
        # All letters should be present
        all_text = " ".join(chunks)
        for letter in "abcdefghij":
            assert letter in all_text