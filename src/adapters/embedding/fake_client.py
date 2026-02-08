import hashlib
import numpy as np
import numpy.typing as npt


class FakeEmbeddingAdapter:
    """
    Fake embedding adapter for testing.
    Generates deterministic embeddings based on text hash.
    """
    
    def __init__(self, dimension: int = 384):
        """
        Initialize fake adapter with specified dimension.
        
        Args:
            dimension: Embedding vector dimension (default 384 matches all-MiniLM-L6-v2)
        """
        self._dimension = dimension
    
    def embed(self, text: str) -> npt.NDArray[np.float32]:
        """Generate deterministic fake embedding for a single text."""
        if not text or not text.strip():
            raise ValueError("Cannot embed empty text")
        
        # Use hash to generate deterministic but pseudo-random embedding
        hash_obj = hashlib.md5(text.encode('utf-8'))
        seed = int(hash_obj.hexdigest(), 16) % (2**32)
        rng = np.random.RandomState(seed)
        
        # Generate random vector and normalize
        embedding = rng.randn(self._dimension).astype(np.float32)
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm
        
        return embedding
    
    def embed_batch(self, texts: list[str]) -> npt.NDArray[np.float32]:
        """Generate deterministic fake embeddings for multiple texts."""
        if not texts:
            raise ValueError("Cannot embed empty list of texts")
        
        if any(not t or not t.strip() for t in texts):
            raise ValueError("Cannot embed empty text in batch")
        
        return np.array([self.embed(text) for text in texts])
    
    def get_dimension(self) -> int:
        """Get the dimensionality of the embedding vectors."""
        return self._dimension
