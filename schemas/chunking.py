"""Generic wrapper for chunked LLM responses.

This module implements the Envelope Pattern for separating chunking logic
(technical constraint) from business schema logic. Chunking happens due to
LLM token limits, not business requirements, so it should be transparent
to business schemas.

Design: Chunks are stored as raw dicts (not validated models) because Python
generics are erased at runtime. Validation happens at merge-time when the
actual schema class is known.
"""

from typing import Any, Callable
from pydantic import BaseModel, Field
from functools import reduce

from schemas.base import SchemaBase


class ChunkedResponse(BaseModel):
    """Wrapper for chunked LLM responses with deferred validation.
    
    Stores chunks as raw dicts and validates them when merge() is called
    with the actual schema class. This avoids Python's generic type erasure
    issue where runtime JSON parsing can't know the concrete type parameter.
    
    Example:
        ```python
        from schemas.summary.v4 import Summary
        from schemas.chunking import ChunkedResponse
        
        # Parse from JSON (chunks stored as raw dicts)
        chunked = ChunkedResponse.model_validate_json(json_text)
        
        # Merge with schema - validates and merges in one step
        # Auto-discovers Summary.merge classmethod
        result: Summary = chunked.merge(Summary)
        
        # Or with explicit merge function
        result = chunked.merge(Summary, merge_fn=custom_merge)
        ```
    """
    
    chunks: list[dict[str, Any]] = Field(..., min_length=1)
    
    @property
    def is_chunked(self) -> bool:
        """Returns True if this response contains multiple chunks."""
        return len(self.chunks) > 1
    
    def single[T: SchemaBase](self, schema: type[T]) -> T:
        """Validate and return the single chunk.
        
        Args:
            schema: Pydantic model class to validate against
            
        Raises:
            ValueError: If there are multiple chunks
        
        Returns:
            The validated single chunk
        """
        if len(self.chunks) != 1:
            raise ValueError(
                f"Expected single chunk but got {len(self.chunks)}. "
                f"Use merge() or access chunks directly."
            )
        return schema.model_validate(self.chunks[0])
    
    def merge[T: SchemaBase](
        self, 
        schema: type[T], 
        merge_fn: Callable[[T, T], T] | None = None
    ) -> T:
        """Validate chunks and merge to single item.
        
        Validates each chunk against the schema, then merges them using either:
        1. The provided merge_fn, or
        2. The schema's classmethod `merge(cls, a, b)` if it exists
        
        Args:
            schema: Pydantic model class to validate chunks against
            merge_fn: Optional custom merge function (a, b) -> merged.
                     If None, looks for schema.merge classmethod.

        Returns:
            Merged result of type T
            
        Raises:
            ValueError: If multiple chunks exist but no merge strategy available
        """
        # Validate all chunks against the actual schema
        validated: list[T] = [schema.model_validate(c) for c in self.chunks]
        
        if len(validated) == 1:
            return validated[0]
        
        # Determine merge function
        if merge_fn is None:
            # Auto-discover schema.merge classmethod
            if hasattr(schema, 'merge') and callable(getattr(schema, 'merge')):
                merge_fn = getattr(schema, 'merge')
            else:
                raise ValueError(
                    f"Multiple chunks ({len(self.chunks)}) require a merge strategy. "
                    f"Either provide merge_fn or add a merge(cls, a, b) classmethod to {schema.__name__}."
                )
        
        return reduce(merge_fn, validated)
