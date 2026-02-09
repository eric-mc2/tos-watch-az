import pytest
import json

from schemas.claim.v1 import Claims as ClaimsV1, VERSION as CLAIM_VERSION
from schemas.summary.v3 import Summary as SummaryV3, VERSION as SUMMARY_VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive
from src.transforms.factcheck.claim_checker import ClaimCheckerBuilder, ClaimChecker
from src.transforms.differ import DiffDoc, DiffSection
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.adapters.embedding.fake_client import FakeEmbeddingAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.services.embedding import EmbeddingService
from src.transforms.llm_transform import LLMTransform


@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter()
    adapter.create_container()
    service = BlobService(adapter)
    return service


@pytest.fixture
def fake_llm():
    return FakeLLMAdapter()


@pytest.fixture
def fake_embedder():
    return FakeEmbeddingAdapter(dimension=384)


@pytest.fixture
def llm_service(fake_llm):
    return LLMService(fake_llm)


@pytest.fixture
def embedding_service(fake_embedder):
    return EmbeddingService(fake_embedder)


@pytest.fixture
def llm_transform(fake_storage, llm_service):
    return LLMTransform(fake_storage, llm_service)


@pytest.fixture
def sample_claims():
    """Sample claims for testing."""
    return ClaimsV1(claims=[
        "The document mentions age restrictions changed from 12+ to 15+",
        "The document mentions new data collection practices",
        "The document mentions pricing changes"
    ])


@pytest.fixture
def sample_diffs():
    """Sample diff document for testing."""
    return DiffDoc(diffs=[
        DiffSection(
            index=0,
            before="Users must be 12 years or older to use the service.",
            after="Users must be 15 years or older to use the service."
        ),
        DiffSection(
            index=1,
            before="We collect your email address and username.",
            after="We collect your email, username, location data, and device information."
        ),
        DiffSection(
            index=2,
            before="Service is free for all users.",
            after="Service costs $9.99/month after a 30-day trial."
        ),
    ])


class TestClaimCheckerBuilder:
    """Unit tests for ClaimCheckerBuilder using fake adapters."""
    
    def test_build_prompt_basic(self, fake_storage, embedding_service, sample_claims, sample_diffs):
        """Test that builder creates prompts for each claim."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service)
        
        claims_blob = "claims.json"
        diffs_blob = "diffs.json"
        
        fake_storage.upload_text_blob(
            sample_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        # Act
        prompts = list(builder.build_prompt(claims_blob, diffs_blob))
        
        # Assert
        assert len(prompts) == 3  # One prompt per claim
        for prompt in prompts:
            assert prompt.system is not None
            assert prompt.current.role == "user"
            # Verify prompt contains claim and document
            content = json.loads(prompt.current.content)
            assert "claim" in content
            assert "document" in content
    
    def test_build_prompt_with_empty_claims(self, fake_storage, embedding_service, sample_diffs):
        """Test handling of empty claims list."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service)
        
        empty_claims = ClaimsV1(claims=[])
        claims_blob = "empty_claims.json"
        diffs_blob = "diffs.json"
        
        fake_storage.upload_text_blob(
            empty_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        # Act
        prompts = list(builder.build_prompt(claims_blob, diffs_blob))
        
        # Assert
        assert len(prompts) == 0
    
    def test_format_diffs(self):
        """Test diff formatting for LLM context."""
        # Arrange
        diff_doc = DiffDoc(diffs=[
            DiffSection(index=0, before="Old text", after="New text"),
            DiffSection(index=1, before="Another old", after="Another new")
        ])
        
        # Act
        formatted = ClaimCheckerBuilder._format_diffs(diff_doc)
        
        # Assert
        assert "Section 1" in formatted
        assert "Section 2" in formatted
        assert "Before: Old text" in formatted
        assert "After: New text" in formatted
    
    def test_format_diffs_empty(self):
        """Test formatting of empty diff doc."""
        # Arrange
        empty_diff_doc = DiffDoc(diffs=[])
        
        # Act
        formatted = ClaimCheckerBuilder._format_diffs(empty_diff_doc)
        
        # Assert
        assert "No relevant document sections found" in formatted
    
    def test_rag_indexer_builds(self, fake_storage, embedding_service, sample_claims, sample_diffs):
        """Test that RAG indexer is built during prompt building."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service)
        
        claims_blob = "claims.json"
        diffs_blob = "diffs.json"
        
        fake_storage.upload_text_blob(
            sample_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        # Act - consume all prompts to trigger indexer build
        prompts = list(builder.build_prompt(claims_blob, diffs_blob))
        
        # Assert - just verify it doesn't crash and produces prompts
        assert len(prompts) > 0


class TestClaimChecker:
    """Unit tests for ClaimChecker using fake adapters."""
    
    def test_check_claim_basic(self, fake_storage, llm_service, llm_transform,
                                embedding_service, sample_claims, sample_diffs):
        """Test basic claim checking workflow."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )
        
        claims_blob = "claims.json"
        diffs_blob = "diffs.json"
        
        fake_storage.upload_text_blob(
            sample_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        # Configure fake LLM to return valid fact-check responses
        llm_service.adapter.set_response('{"veracity": true, "reason": "The claim is supported by the document."}')
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        
        # Verify result structure
        result = json.loads(result_json)
        assert "chunks" in result
        assert len(result["chunks"]) == 3  # One per claim
    
    def test_check_claim_validates_responses(self, fake_storage, llm_service, llm_transform,
                                              embedding_service, sample_claims, sample_diffs):
        """Test that responses are validated (smoke test - actual validation in LLMService)."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )
        
        claims_blob = "claims.json"
        diffs_blob = "diffs.json"
        
        fake_storage.upload_text_blob(
            sample_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        # Set response that should be parseable
        llm_service.adapter.set_response('{"veracity": false, "reason": "Not supported."}')
        
        # Act - should not raise
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert
        assert result_json is not None


class TestClaimCheckerIntegration:
    """Integration-style tests using fake adapters but testing full workflow."""
    
    def test_end_to_end_claim_checking(self, fake_storage, llm_service, llm_transform,
                                        embedding_service):
        """Test complete claim checking workflow from start to finish."""
        # Arrange - create realistic test data
        claims = ClaimsV1(claims=[
            "The minimum age requirement increased from 12 to 15 years"
        ])
        
        diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Users must be at least 12 years old.",
                after="Users must be at least 15 years old."
            ),
            DiffSection(
                index=1,
                before="Privacy policy applies to all users.",
                after="Privacy policy applies to all users over 15."
            )
        ])
        
        claims_blob = "e2e_claims.json"
        diffs_blob = "e2e_diffs.json"
        
        fake_storage.upload_text_blob(
            claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )
        
        # Configure LLM response
        llm_service.adapter.set_response(
            '{"veracity": true, "reason": "The document shows age requirement changed from 12 to 15."}'
        )
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert
        result = json.loads(result_json)
        assert len(result["chunks"]) == 1
        assert result["chunks"][0]["veracity"] is True
        assert "12 to 15" in result["chunks"][0]["reason"] or "age" in result["chunks"][0]["reason"].lower()
