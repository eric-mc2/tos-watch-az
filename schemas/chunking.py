"""Generic wrapper for chunked LLM responses.

This module implements the Envelope Pattern for separating chunking logic
(technical constraint) from business schema logic. Chunking happens due to
LLM token limits, not business requirements, so it should be transparent
to business schemas.
"""

from typing import Type, TypeVar, Generic, List, Callable, Optional, cast
from pydantic import BaseModel, Field
from functools import reduce

T = TypeVar('T', bound=BaseModel)


class ChunkedResponse(BaseModel, Generic[T]):
    """Generic wrapper for chunked LLM responses.
    
    This wrapper allows any Pydantic schema to be stored in chunked format
    without contaminating the business schema with chunking concerns.
    
    Example:
        ```python
        from schemas.summary.v2 import Summary
        from schemas.chunking import ChunkedResponse
        
        # Wrap multiple chunks
        chunked = ChunkedResponse[Summary](chunks=[
            Summary(...),
            Summary(...),
        ])
        
        # Access chunks
        for chunk in chunked.chunks:
            print(chunk.practically_substantive)
        ```
    """
    
    chunks: List[T] = Field(..., min_length=1)
    
    @property
    def is_chunked(self) -> bool:
        """Returns True if this response contains multiple chunks."""
        return len(self.chunks) > 1
    
    @property
    def single(self) -> T:
        """Get the single item if there's only one chunk.
        
        Raises:
            ValueError: If there are multiple chunks
        
        Returns:
            The single chunk item
        """
        if len(self.chunks) != 1:
            raise ValueError(
                f"Expected single chunk but got {len(self.chunks)}. "
                f"Use merge() or access chunks directly."
            )
        return self.chunks[0]
    
    # TODO: THIS BREAKS BECAUSE IT CANT DYNAMICALY TYPE THE INPUTS
    def merge(self, schema: Type[BaseModel], merge_fn: Optional[Callable[[T, T], T]] = None) -> T:
        """Merge chunks back to single item.
        
        Args:
            merge_fn: Optional custom merge function that takes a list of chunks
                     and returns a merged result. If None and there's only one
                     chunk, returns that chunk. Otherwise raises an error.

        Returns:
            Merged result of type T
            
        Raises:
            ValueError: If multiple chunks exist but no merge_fn provided
        """
        if len(self.chunks) == 1:
            return self.chunks[0]
        
        if merge_fn is None:
            raise ValueError(
                f"Multiple chunks ({len(self.chunks)}) require merge_fn. "
                f"Provide a function to merge chunks or access them directly."
            )
        
        # TODO: Trying to pass the actual schema and use it to cast value inside this func.
        #       Otherwise I can try casting inside the merge func, which knows its proper type statically.
        _chunks = [schema.model_validate(x) for x in self.chunks]
        return reduce(merge_fn, _chunks)
