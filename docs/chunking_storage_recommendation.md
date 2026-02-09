# Recommendations: Separating Chunking Logic from Schema Logic

## Current Architecture Analysis

### How Chunking Currently Works

1. **Chunking Layer** (`PromptChunker`):
   - Input: `DiffDoc` (business data model)
   - Output: `List[DiffDoc]` (chunks in same format)
   - Logic: Token-limit-based splitting

2. **LLM Execution Layer** (`LLMTransform.execute_prompts`):
   - Input: `Iterable[PromptMessages]`
   - Output: `{"chunks": [response_dicts]}`
   - Creates wrapper format with `chunks` array

3. **Schema/Validation Layer**:
   - **Summary v3**: `Summary(chunks: List[SummaryV2])`
   - **Claims v1**: `Claims(claims: List[str])`
   - **FactCheck v1**: `FactCheck(claims: List[str])`
   - **Judge v1**: `Judgement(practically_substantive: Substantive)`

### The Core Issue

The chunking wrapper `{"chunks": [...]}` is **storage format**, not business logic. It leaks into:
- Schema definitions (Summary.chunks)
- Downstream processing (claim_extractor filters chunks)
- Validation logic (schemas must validate wrapped format)

This violates separation of concerns: **business schemas shouldn't know about technical constraints**.

---

## Recommended Solutions

### **Option 1: Envelope Pattern with Generic Wrapper** ⭐ (Recommended)

Create a generic chunking envelope that wraps any schema without contaminating it.

#### Implementation

```python
# schemas/chunking.py
from typing import TypeVar, Generic, List
from pydantic import BaseModel, Field

T = TypeVar('T', bound=BaseModel)

class ChunkedResponse(BaseModel, Generic[T]):
    """Generic wrapper for chunked LLM responses."""
    chunks: List[T] = Field(..., min_length=1)
    
    @property
    def is_chunked(self) -> bool:
        return len(self.chunks) > 1
    
    def merge(self, merge_fn=None) -> T:
        """Merge chunks back to single item.
        
        Args:
            merge_fn: Optional custom merge function.
                     Default: return first chunk if single, else error.
        """
        if len(self.chunks) == 1:
            return self.chunks[0]
        
        if merge_fn is None:
            raise ValueError("Multiple chunks require merge_fn")
        
        return merge_fn(self.chunks)


# Usage in schemas/summary/v4.py
from schemas.summary.v2 import Summary as SummaryV2

VERSION = "v4"

@register(MODULE, VERSION)
class Summary(SummaryV2):
    """Business schema - no chunking concern"""
    pass


# Chunked variant (only for storage/parsing layer)
ChunkedSummary = ChunkedResponse[SummaryV2]
```

#### Changes to `LLMTransform`

```python
def execute_prompts(
    self, 
    prompts: Iterable[PromptMessages], 
    schema_version: str, 
    module_name: str,
    prompt_version: str,
) -> tuple[str, dict]:
    """Execute prompts and return chunked OR single response."""
    responses = []
    for message in prompts:
        txt = self.llm.call_unsafe(message.system, message.history + [message.current])
        parsed = self.llm.extract_json_from_response(txt)
        if parsed['success']:
            responses.append(parsed['data'])
        else:
            logger.warning(f"Failed to parse response: {parsed['error']}")
            responses.append({"error": parsed['error'], "raw": txt})

    # Only wrap if actually chunked
    if len(responses) == 1:
        response = json.dumps(responses[0])
    else:
        response = json.dumps(dict(chunks=responses))
    
    metadata = dict(
        run_id=ulid.ulid(),
        schema_version=schema_version,
        prompt_version=prompt_version,
        is_chunked=len(responses) > 1,  # Track in metadata!
    )
    return response, metadata
```

#### Changes to Parser

```python
def create_llm_parser(storage: BlobService, llm: LLMService, 
                     module_name: str, output_stage: str) -> Callable:
    def parser(input_blob) -> None:
        in_path = storage.parse_blob_path(input_blob.name)
        txt = input_blob.read().decode()
        metadata = storage.adapter.load_metadata(input_blob.name)
        
        # Get business schema
        schema = SCHEMA_REGISTRY[module_name][metadata['schema_version']]
        
        # Detect chunking from metadata or data structure
        is_chunked = metadata.get('is_chunked', False)
        if not is_chunked:
            # Try to detect from structure
            parsed = json.loads(txt)
            is_chunked = isinstance(parsed, dict) and 'chunks' in parsed
        
        if is_chunked:
            # Validate each chunk against business schema
            wrapper = ChunkedResponse[schema].model_validate_json(txt)
            # Store as chunked
            cleaned_txt = wrapper.model_dump_json()
        else:
            # Validate as single item
            cleaned_txt = llm.validate_output(txt, schema)
        
        # Save versioned output
        out_path = os.path.join(output_stage, in_path.company, in_path.policy, 
                               in_path.timestamp, f"{metadata['run_id']}.json")
        storage.upload_json_blob(cleaned_txt, out_path, metadata=metadata)
        
        # Save latest output
        latest_path = os.path.join(output_stage, in_path.company, in_path.policy,
                                  in_path.timestamp, "latest.json")
        storage.upload_json_blob(cleaned_txt, latest_path, metadata=metadata)
        
        logger.info(f"Successfully validated {module_name} blob: {input_blob.name}")
    
    return parser
```

#### Benefits
- ✅ Business schemas remain pure (no `chunks` field)
- ✅ Backward compatible (can detect chunked vs non-chunked)
- ✅ Type-safe with generics
- ✅ Pydantic validation works on each chunk
- ✅ Metadata tracks chunking status
- ✅ Clear merge strategy for downstream consumers

#### Migration Path
1. Create `ChunkedResponse[T]` generic
2. Update `execute_prompts` to include `is_chunked` in metadata
3. Update parser to detect and validate both formats
4. Update downstream code to check `is_chunked` and unwrap
5. Eventually: Create v4 schemas without hardcoded chunks field

---

### **Option 2: Dual Schema Pattern**

Separate storage schemas from business schemas.

```python
# Business schema (pure)
class SummaryBusiness(SummaryBase):
    practically_substantive: Substantive

# Storage schema (with chunking)
class SummaryStorage(SummaryBase):
    chunks: List[SummaryBusiness]

# Converter
def to_business(storage: SummaryStorage) -> List[SummaryBusiness]:
    return storage.chunks

def to_storage(items: List[SummaryBusiness]) -> SummaryStorage:
    return SummaryStorage(chunks=items)
```

#### Benefits
- ✅ Clear separation of concerns
- ✅ Business logic works with clean schemas

#### Drawbacks
- ⚠️ Duplication of schema definitions
- ⚠️ Need conversion layer everywhere
- ⚠️ More complex type annotations

---

### **Option 3: Discriminated Union Pattern**

Use Pydantic discriminated unions to handle both formats.

```python
from pydantic import Field, Discriminator

class SingleSummary(SummaryBase):
    format: Literal["single"] = "single"
    data: SummaryV2

class ChunkedSummary(SummaryBase):
    format: Literal["chunked"] = "chunked"
    chunks: List[SummaryV2]

Summary = Annotated[
    Union[SingleSummary, ChunkedSummary],
    Field(discriminator='format')
]
```

#### Benefits
- ✅ Pydantic handles format detection
- ✅ Type-safe pattern matching

#### Drawbacks
- ⚠️ More verbose schema definitions
- ⚠️ Downstream code needs to match on union type

---

### **Option 4: Middleware/Adapter Pattern**

Keep chunking entirely outside schemas, handle in adapter layer.

```python
class ChunkingAdapter:
    """Handles chunked storage format transparently."""
    
    def save(self, items: List[T], metadata: dict) -> None:
        """Save items, wrapping in chunks if multiple."""
        if len(items) == 1:
            data = items[0].model_dump_json()
            metadata['chunked'] = False
        else:
            data = json.dumps({"chunks": [i.model_dump() for i in items]})
            metadata['chunked'] = True
        self.storage.upload_json_blob(data, path, metadata)
    
    def load(self, path: str, schema: Type[T]) -> List[T]:
        """Load items, unwrapping chunks if present."""
        txt = self.storage.load_text_blob(path)
        metadata = self.storage.adapter.load_metadata(path)
        
        if metadata.get('chunked'):
            data = json.loads(txt)
            return [schema.model_validate(chunk) for chunk in data['chunks']]
        else:
            return [schema.model_validate_json(txt)]
```

#### Benefits
- ✅ Schemas completely unaware of chunking
- ✅ Centralized chunking logic
- ✅ Easy to test

#### Drawbacks
- ⚠️ Another abstraction layer
- ⚠️ Need to refactor all storage access points

---

## Comparison Matrix

| Option | Schema Purity | Backward Compat | Complexity | Type Safety |
|--------|---------------|-----------------|------------|-------------|
| **1. Generic Envelope** ⭐ | ✅ Excellent | ✅ Yes | ⭐ Low | ✅ Excellent |
| 2. Dual Schema | ✅ Excellent | ⚠️ Partial | ⚠️ Medium | ✅ Good |
| 3. Discriminated Union | ⚠️ Good | ✅ Yes | ⚠️ Medium | ✅ Excellent |
| 4. Middleware Adapter | ✅ Excellent | ✅ Yes | ⚠️ Medium | ⚠️ Good |

---

## Recommendation: Option 1 - Generic Envelope Pattern

### Why Option 1?

1. **Minimal changes**: Schemas can evolve at your pace
2. **Backward compatible**: Works with existing chunked data
3. **Type-safe**: Leverages Python generics and Pydantic
4. **Clear semantics**: `ChunkedResponse[T]` is self-documenting
5. **Pydantic-friendly**: Full validation on each chunk
6. **Metadata tracking**: `is_chunked` flag for easy detection

### Implementation Phases

#### Phase 1: Add Infrastructure (No Breaking Changes)
- Create `schemas/chunking.py` with `ChunkedResponse[T]`
- Add `is_chunked` to metadata in `execute_prompts`
- Update parser to detect and handle both formats

#### Phase 2: Update Downstream Consumers
- Modify `claim_extractor.py` to check `is_chunked` metadata
- Add helper functions for unwrapping chunks
- Update tests

#### Phase 3: Schema Evolution (When Ready)
- Create v4 schemas without hardcoded `chunks` field
- Use `ChunkedResponse[SummaryV4]` for storage
- Migrate historical data (optional)

### Example Downstream Usage

```python
# In claim_extractor.py
def build_prompt(self, blob_name: str) -> Iterator[PromptMessages]:
    summary_text = self.storage.load_text_blob(blob_name)
    metadata = self.storage.adapter.load_metadata(blob_name)
    schema = SCHEMA_REGISTRY[SUMMARY_MODULE][metadata['schema_version']]
    
    # Handle both chunked and non-chunked
    if metadata.get('is_chunked'):
        wrapped = ChunkedResponse[schema].model_validate_json(summary_text)
        summaries = wrapped.chunks
    else:
        summaries = [schema.model_validate_json(summary_text)]
    
    # Rest of logic works with List[Summary] regardless of storage format
    substantive = [
        x.practically_substantive 
        for x in summaries 
        if x.practically_substantive.rating
    ]
    # ...
```

---

## Additional Considerations

### Historical Data Migration (Optional)

Since blob storage already has mixed formats, you don't need strict migration. The parser can handle both:

```python
def is_chunked_format(data: dict) -> bool:
    """Detect if data is in chunked format."""
    return (
        isinstance(data, dict) and 
        'chunks' in data and 
        isinstance(data['chunks'], list)
    )
```

### Future Optimizations

Once Option 1 is stable, you could:
- Add `merge()` strategies for different schemas
- Implement chunk-level caching
- Add chunk-level retry logic (if needed)
- Create views that auto-flatten chunks for reporting

### Testing Strategy

```python
def test_backward_compatibility():
    # Old format (chunked, embedded in schema)
    old_data = Summary(chunks=[SummaryV2(...), SummaryV2(...)])
    
    # New format (chunked, wrapped)
    new_data = ChunkedResponse[SummaryV2](chunks=[SummaryV2(...), SummaryV2(...)])
    
    # Parser should handle both
    assert parse(old_data.model_dump_json()) == parse(new_data.model_dump_json())
```

---

## Conclusion

The **Generic Envelope Pattern (Option 1)** provides the cleanest separation of concerns while maintaining backward compatibility and leveraging Pydantic's powerful validation. It's a gradual, low-risk refactoring that doesn't require big-bang migrations.

The key insight: **Chunking is a transport/storage concern, not a business logic concern**. By wrapping business schemas in a generic `ChunkedResponse[T]` only at storage boundaries, you keep your schemas focused on domain logic while transparently handling the technical constraint of LLM context windows.
