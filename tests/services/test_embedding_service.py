import pytest
import numpy as np
from src.services.embedding import EmbeddingService
from src.adapters.embedding.fake_client import FakeEmbeddingAdapter


@pytest.fixture
def fake_embedder():
    return FakeEmbeddingAdapter(dimension=384)


@pytest.fixture
def embedding_service(fake_embedder):
    return EmbeddingService(fake_embedder)


class TestEmbeddingService:
    """Test cases for EmbeddingService."""
    
    def test_embed_single_text(self, embedding_service):
        """Test embedding a single text."""
        text = "This is a test sentence."
        embedding = embedding_service.embed(text)
        
        assert embedding is not None
        assert isinstance(embedding, np.ndarray)
        assert embedding.dtype == np.float32
        assert embedding.shape == (384,)
    
    def test_embed_empty_text(self, embedding_service):
        """Test that empty text raises ValueError."""
        with pytest.raises(ValueError, match="Cannot embed empty text"):
            embedding_service.embed("")
    
    def test_embed_whitespace_only(self, embedding_service):
        """Test that whitespace-only text raises ValueError."""
        with pytest.raises(ValueError, match="Cannot embed empty text"):
            embedding_service.embed("   ")
    
    def test_embed_long_text(self, embedding_service):
        """Test that very long text is truncated with warning."""
        long_text = "word " * 1000  # Very long text
        embedding = embedding_service.embed(long_text)
        
        assert embedding is not None
        assert embedding.shape == (384,)
    
    def test_embed_batch(self, embedding_service):
        """Test embedding multiple texts."""
        texts = [
            "First sentence.",
            "Second sentence.",
            "Third sentence."
        ]
        embeddings = embedding_service.embed_batch(texts)
        
        assert embeddings is not None
        assert isinstance(embeddings, np.ndarray)
        assert embeddings.dtype == np.float32
        assert embeddings.shape == (3, 384)
    
    def test_embed_batch_empty_list(self, embedding_service):
        """Test that empty batch raises ValueError."""
        with pytest.raises(ValueError, match="Cannot embed empty list"):
            embedding_service.embed_batch([])
    
    def test_embed_batch_with_empty_text(self, embedding_service):
        """Test that batch with empty text raises ValueError."""
        texts = ["Valid text", "", "Another valid text"]
        with pytest.raises(ValueError, match="Cannot embed empty text in batch"):
            embedding_service.embed_batch(texts)
    
    def test_get_dimension(self, embedding_service):
        """Test getting embedding dimension."""
        dim = embedding_service.get_dimension()
        assert dim == 384
    
    def test_deterministic_embeddings(self, embedding_service):
        """Test that same text produces same embedding."""
        text = "Deterministic test"
        embedding1 = embedding_service.embed(text)
        embedding2 = embedding_service.embed(text)
        
        np.testing.assert_array_equal(embedding1, embedding2)
    
    def test_different_texts_different_embeddings(self, embedding_service):
        """Test that different texts produce different embeddings."""
        text1 = "First text"
        text2 = "Second text"
        
        embedding1 = embedding_service.embed(text1)
        embedding2 = embedding_service.embed(text2)
        
        # Embeddings should not be identical
        assert not np.array_equal(embedding1, embedding2)
    
    def test_compute_similarity_identical(self, embedding_service):
        """Test similarity of identical embeddings."""
        embedding = embedding_service.embed("Test text")
        similarity = embedding_service.compute_similarity(embedding, embedding)
        
        assert 0.99 <= similarity <= 1.01  # Should be very close to 1
    
    def test_compute_similarity_different(self, embedding_service):
        """Test similarity of different embeddings."""
        embedding1 = embedding_service.embed("Cat")
        embedding2 = embedding_service.embed("Dog")
        
        similarity = embedding_service.compute_similarity(embedding1, embedding2)
        
        # Should be between -1 and 1, but not exactly 1
        assert -1 <= similarity <= 1
        assert similarity < 0.99
    
    def test_compute_similarity_zero_vector(self, embedding_service):
        """Test similarity with zero vector."""
        embedding = embedding_service.embed("Test")
        zero_vector = np.zeros(384, dtype=np.float32)
        
        similarity = embedding_service.compute_similarity(embedding, zero_vector)
        assert similarity == 0.0


class TestFakeEmbeddingAdapter:
    """Test cases specifically for FakeEmbeddingAdapter."""
    
    def test_fake_adapter_init(self):
        """Test FakeEmbeddingAdapter initialization."""
        adapter = FakeEmbeddingAdapter(dimension=128)
        assert adapter.get_dimension() == 128
    
    def test_fake_adapter_default_dimension(self):
        """Test FakeEmbeddingAdapter default dimension."""
        adapter = FakeEmbeddingAdapter()
        assert adapter.get_dimension() == 384
    
    def test_fake_adapter_embed(self):
        """Test FakeEmbeddingAdapter embed method."""
        adapter = FakeEmbeddingAdapter(dimension=256)
        embedding = adapter.embed("Test text")
        
        assert embedding.shape == (256,)
        assert embedding.dtype == np.float32
    
    def test_fake_adapter_normalized_output(self):
        """Test that fake adapter produces normalized vectors."""
        adapter = FakeEmbeddingAdapter()
        embedding = adapter.embed("Normalization test")
        
        norm = np.linalg.norm(embedding)
        assert 0.99 <= norm <= 1.01  # Should be unit vector
