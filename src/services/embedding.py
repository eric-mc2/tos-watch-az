import logging
from dataclasses import dataclass
import numpy.typing as npt
import numpy as np

from src.adapters.embedding.protocol import EmbeddingProtocol
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)


@dataclass
class EmbeddingService:
    """
    Service layer for embeddings following hexagonal architecture.
    Wraps embedding adapter with additional validation and logging.
    """
    adapter: EmbeddingProtocol
    
    def embed(self, text: str) -> npt.NDArray[np.float32]:
        """
        Generate embedding vector for a single text with validation.
        
        Args:
            text: Input text to embed
            
        Returns:
            Numpy array of float32 representing the embedding vector
            
        Raises:
            ValueError: If text is empty or invalid
        """
        if not text or not text.strip():
            raise ValueError("Cannot embed empty text")
        
        # Truncate very long texts (model-dependent, but 512 tokens is reasonable)
        max_chars = 2000  # Approximate character limit
        if len(text) > max_chars:
            logger.warning(f"Text length {len(text)} exceeds {max_chars}, truncating")
            text = text[:max_chars]
        
        try:
            embedding = self.adapter.embed(text)
            return embedding
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            raise
    
    def embed_batch(self, texts: list[str]) -> npt.NDArray[np.float32]:
        """
        Generate embedding vectors for multiple texts with validation.
        
        Args:
            texts: List of input texts to embed
            
        Returns:
            2D numpy array where each row is an embedding vector
            
        Raises:
            ValueError: If texts list is empty or contains invalid entries
        """
        if not texts:
            raise ValueError("Cannot embed empty list of texts")
        
        if any(not t or not t.strip() for t in texts):
            raise ValueError("Cannot embed empty text in batch")
        
        # Truncate very long texts
        max_chars = 2000
        truncated_texts = []
        for i, text in enumerate(texts):
            if len(text) > max_chars:
                logger.warning(f"Text {i} length {len(text)} exceeds {max_chars}, truncating")
                truncated_texts.append(text[:max_chars])
            else:
                truncated_texts.append(text)
        
        try:
            embeddings = self.adapter.embed_batch(truncated_texts)
            return embeddings
        except Exception as e:
            logger.error(f"Batch embedding generation failed: {e}")
            raise
    
    def get_dimension(self) -> int:
        """
        Get the dimensionality of the embedding vectors.
        
        Returns:
            Integer dimension of embedding vectors
        """
        return self.adapter.get_dimension()

