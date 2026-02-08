from typing import Protocol
import numpy as np
import numpy.typing as npt


class EmbeddingProtocol(Protocol):
    """Protocol for embedding adapters following hexagonal architecture."""
    
    def embed(self, text: str) -> npt.NDArray[np.float32]:
        """
        Generate embedding vector for a single text.
        
        Args:
            text: Input text to embed
            
        Returns:
            Numpy array of float32 representing the embedding vector
        """
        ...
    
    def embed_batch(self, texts: list[str]) -> npt.NDArray[np.float32]:
        """
        Generate embedding vectors for multiple texts.
        
        Args:
            texts: List of input texts to embed
            
        Returns:
            2D numpy array where each row is an embedding vector
        """
        ...
    
    def get_dimension(self) -> int:
        """
        Get the dimensionality of the embedding vectors.
        
        Returns:
            Integer dimension of embedding vectors
        """
        ...
