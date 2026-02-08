import logging
from dataclasses import dataclass, field
from typing import Optional
import faiss
import numpy as np

from src.services.blob import BlobService
from src.services.embedding import EmbeddingService
from src.transforms.differ import DiffDoc, DiffSection
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)


@dataclass
class Indexer:
    """
    FAISS-based vector search indexer for document similarity search.
    Builds an in-memory vector database from diff documents and performs KNN search.
    """
    storage: BlobService
    embedder: EmbeddingService
    _index: Optional[faiss.Index] = None
    _metadata: dict[int, dict[str, str]] = field(default_factory=dict)
    _dimension: Optional[int] = None
    k: int = 5  # Number of nearest neighbors to return

    def build(self, blob_name: str) -> None:
        """
        Build FAISS index from a DiffDoc stored in blob storage.
        
        Args:
            blob_name: Name of blob containing DiffDoc JSON
        """
        logger.info(f"Building FAISS index from {blob_name}")
        
        # Load and parse diffs
        diffs_txt = self.storage.load_text_blob(blob_name)
        diffs = DiffDoc.model_validate_json(diffs_txt)
        
        if not diffs.diffs:
            logger.warning(f"No diffs found in {blob_name}")
            return
        
        # Get embedding dimension
        self._dimension = self.embedder.get_dimension()
        
        # Initialize FAISS index (using L2 distance)
        self._index = faiss.IndexFlatL2(self._dimension)
        
        # Collect all texts to embed
        texts_to_embed = []
        metadata_entries = []
        
        for diff in diffs.diffs:
            # Add "before" text
            if diff.before and diff.before.strip():
                texts_to_embed.append(diff.before)
                metadata_entries.append({
                    'diff_index': diff.index,
                    'category': 'before',
                    'text': diff.before
                })
            
            # Add "after" text
            if diff.after and diff.after.strip():
                texts_to_embed.append(diff.after)
                metadata_entries.append({
                    'diff_index': diff.index,
                    'category': 'after',
                    'text': diff.after
                })
        
        if not texts_to_embed:
            logger.warning("No valid text found to embed")
            return
        
        # Generate embeddings in batch
        logger.debug(f"Generating embeddings for {len(texts_to_embed)} text chunks")
        embeddings = self.embedder.embed_batch(texts_to_embed)
        
        # Add to FAISS index
        self._index.add(embeddings)
        
        # Store metadata with sequential IDs
        for idx, metadata in enumerate(metadata_entries):
            self._metadata[idx] = metadata
        
        logger.info(f"FAISS index built with {len(self._metadata)} entries")

    def search(self, query: str, k: Optional[int] = None) -> DiffDoc:
        """
        Search for most similar diffs using KNN.
        
        Args:
            query: Query text to search for
            k: Number of results to return (defaults to self.k)
            
        Returns:
            DiffDoc containing the most relevant diff sections
        """
        if self._index is None:
            raise ValueError("Index not built. Call build() first.")
        
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        k = k or self.k
        k = min(k, len(self._metadata))  # Don't request more than available
        
        if k == 0:
            logger.warning("No entries in index")
            return DiffDoc(diffs=[])
        
        # Generate query embedding
        query_vec = self.embedder.embed(query)
        query_vec = query_vec.reshape(1, -1)  # FAISS expects 2D array
        
        # Search FAISS index
        distances, indices = self._index.search(query_vec, k)
        
        # Collect unique diff indices with their texts
        diff_map: dict[int, dict[str, str]] = {}
        
        for idx in indices[0]:
            if idx == -1:  # FAISS returns -1 for missing results
                continue
            
            metadata = self._metadata.get(int(idx))
            if not metadata:
                continue
            
            diff_idx = metadata['diff_index']
            category = metadata['category']
            text = metadata['text']
            
            if diff_idx not in diff_map:
                diff_map[diff_idx] = {'before': '', 'after': ''}
            
            diff_map[diff_idx][category] = text
        
        # Create DiffDoc from results
        diff_sections = [
            DiffSection(
                index=diff_idx,
                before=texts.get('before', ''),
                after=texts.get('after', '')
            )
            for diff_idx, texts in sorted(diff_map.items())
        ]
        
        result = DiffDoc(diffs=diff_sections)
        logger.debug(f"Search returned {len(diff_sections)} diff sections")
        
        return result
    
    def is_built(self) -> bool:
        """Check if index has been built."""
        return self._index is not None and len(self._metadata) > 0
    
    def get_index_size(self) -> int:
        """Get number of entries in the index."""
        return len(self._metadata)

