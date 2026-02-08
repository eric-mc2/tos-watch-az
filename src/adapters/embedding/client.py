import logging
import os
from typing import Optional
import numpy as np
import numpy.typing as npt
from sentence_transformers import SentenceTransformer # TODO: Cover import in integration test

from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

# Global model cache - persists across function invocations in Azure Functions
_model: Optional[any] = None


class SentenceTransformerAdapter:
    """
    Adapter for sentence-transformers embedding models.
    Uses 'all-MiniLM-L6-v2' model (~80MB) which is small enough to bundle
    or cache in Azure Functions environment.
    """
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._dimension: Optional[int] = None
    
    def _get_model(self):
        """Lazy load model with global caching for Azure Functions."""
        global _model
        if _model is None:
            logger.info(f"Loading embedding model: {self.model_name}")
            try:
                # Set cache dir to persistent storage in Azure Functions if available
                cache_dir = os.environ.get('SENTENCE_TRANSFORMERS_HOME', None)
                _model = SentenceTransformer(self.model_name, cache_folder=cache_dir)
                logger.info(f"Model loaded successfully. Dimension: {_model.get_sentence_embedding_dimension()}")
            except Exception as e:
                logger.error(f"Failed to load embedding model: {e}")
                raise
        return _model
    
    def embed(self, text: str) -> npt.NDArray[np.float32]:
        """Generate embedding vector for a single text."""
        if not text or not text.strip():
            raise ValueError("Cannot embed empty text")
        
        model = self._get_model()
        embedding = model.encode(text, convert_to_numpy=True, show_progress_bar=False)
        return embedding.astype(np.float32)
    
    def embed_batch(self, texts: list[str]) -> npt.NDArray[np.float32]:
        """Generate embedding vectors for multiple texts."""
        if not texts:
            raise ValueError("Cannot embed empty list of texts")
        
        if any(not t or not t.strip() for t in texts):
            raise ValueError("Cannot embed empty text in batch")
        
        model = self._get_model()
        embeddings = model.encode(
            texts, 
            convert_to_numpy=True, 
            show_progress_bar=False,
            batch_size=32
        )
        return embeddings.astype(np.float32)
    
    def get_dimension(self) -> int:
        """Get the dimensionality of the embedding vectors."""
        if self._dimension is None:
            model = self._get_model()
            self._dimension = model.get_sentence_embedding_dimension()
        return self._dimension
