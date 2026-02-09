import pytest
import json
from src.services.embedding import EmbeddingService
from src.services.blob import BlobService
from src.adapters.embedding.fake_client import FakeEmbeddingAdapter
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.transforms.factcheck.vector_search import Indexer
from src.transforms.differ import DiffDoc, DiffSection


@pytest.fixture
def fake_embedder():
    return FakeEmbeddingAdapter(dimension=384)


@pytest.fixture
def embedding_service(fake_embedder):
    return EmbeddingService(fake_embedder)


@pytest.fixture
def fake_storage():
    return BlobService(FakeStorageAdapter())


@pytest.fixture
def sample_diff_doc():
    """Create a sample DiffDoc for testing."""
    return DiffDoc(diffs=[
        DiffSection(
            index=0,
            before="Users must be 18 years or older.",
            after="Users must be 21 years or older."
        ),
        DiffSection(
            index=1,
            before="We collect your email address.",
            after="We collect your email, phone, and location data."
        ),
        DiffSection(
            index=2,
            before="Data is stored for 30 days.",
            after="Data is stored for 90 days."
        ),
    ])


@pytest.fixture
def indexer_with_data(fake_storage, embedding_service, sample_diff_doc):
    """Create an indexer with sample data."""
    # Store the sample diff doc in fake storage
    blob_name = "test_diffs.json"
    diff_json = sample_diff_doc.model_dump_json()
    fake_storage.upload_text_blob(diff_json, blob_name)
    
    # Create and build indexer
    indexer = Indexer(storage=fake_storage, embedder=embedding_service)
    indexer.build(blob_name)
    
    return indexer


class TestVectorSearch:
    """Test cases for FAISS-based vector search."""
    
    def test_indexer_build(self, fake_storage, embedding_service, sample_diff_doc):
        """Test that indexer builds successfully."""
        blob_name = "test_diffs.json"
        diff_json = sample_diff_doc.model_dump_json()
        fake_storage.upload_text_blob(diff_json, blob_name)

        indexer = Indexer(storage=fake_storage, embedder=embedding_service)
        indexer.build(blob_name)
        
        assert indexer.is_built()
        # 3 diffs * 2 (before + after) = 6 entries
        assert indexer.get_index_size() == 6
    
    def test_indexer_search(self, indexer_with_data):
        """Test basic search functionality."""
        query = "age requirement"
        result = indexer_with_data.search(query)
        
        assert isinstance(result, DiffDoc)
        assert len(result.diffs) > 0
        assert len(result.diffs) <= indexer_with_data.k
    
    def test_indexer_search_relevant_results(self, indexer_with_data):
        """Test that search returns semantically relevant results."""
        # Query about age should return the age-related diff
        query = "minimum age for users"
        result = indexer_with_data.search(query, k=1)
        
        assert len(result.diffs) > 0
        # The first result should contain "18" or "21" related to age
        first_diff = result.diffs[0]
        combined_text = (first_diff.before + " " + first_diff.after).lower()
        assert any(word in combined_text for word in ["18", "21", "age", "older"])
    
    def test_indexer_search_empty_query(self, indexer_with_data):
        """Test that empty query raises ValueError."""
        with pytest.raises(ValueError, match="Query cannot be empty"):
            indexer_with_data.search("")
    
    def test_indexer_search_before_build(self, fake_storage, embedding_service):
        """Test that search before build raises ValueError."""
        indexer = Indexer(storage=fake_storage, embedder=embedding_service)
        
        with pytest.raises(ValueError, match="Index not built"):
            indexer.search("test query")
    
    def test_indexer_build_empty_diff(self, fake_storage, embedding_service):
        """Test building index with empty diff list."""
        empty_diff = DiffDoc(diffs=[])
        blob_name = "empty_diffs.json"
        diff_json = empty_diff.model_dump_json()
        fake_storage.upload_text_blob(diff_json, blob_name)

        indexer = Indexer(storage=fake_storage, embedder=embedding_service)
        indexer.build(blob_name)
        
        assert indexer.get_index_size() == 0
    
    def test_indexer_custom_k(self, indexer_with_data):
        """Test search with custom k parameter."""
        result = indexer_with_data.search("data collection", k=2)
        
        assert len(result.diffs) <= 2
    
    def test_indexer_k_exceeds_available(self, indexer_with_data):
        """Test that k is capped at available entries."""
        # Request more results than available unique diffs
        result = indexer_with_data.search("test", k=100)
        
        # Should return at most the number of unique diffs (3 in sample data)
        assert len(result.diffs) <= 3
    
    def test_indexer_metadata_preservation(self, indexer_with_data):
        """Test that metadata is correctly preserved."""
        result = indexer_with_data.search("data storage", k=1)
        
        assert len(result.diffs) > 0
        diff = result.diffs[0]
        # Check that before and after texts are preserved
        assert diff.before or diff.after  # At least one should be non-empty
        assert isinstance(diff.index, int)
    
    def test_indexer_deterministic_results(self, indexer_with_data):
        """Test that same query produces same results."""
        query = "age requirement"
        
        result1 = indexer_with_data.search(query, k=2)
        result2 = indexer_with_data.search(query, k=2)
        
        # Should get same results
        assert len(result1.diffs) == len(result2.diffs)
        for d1, d2 in zip(result1.diffs, result2.diffs):
            assert d1.index == d2.index
            assert d1.before == d2.before
            assert d1.after == d2.after
