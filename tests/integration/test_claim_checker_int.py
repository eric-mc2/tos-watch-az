import os
import pytest
import json

from schemas.claim.v1 import Claims as ClaimsV1, VERSION as CLAIM_VERSION
from schemas.factcheck.v1 import FactCheck
from src.transforms.factcheck.claim_checker import ClaimChecker
from src.transforms.differ import DiffDoc, DiffSection
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.client import ClaudeAdapter
from src.adapters.embedding.client import SentenceTransformerAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.services.embedding import EmbeddingService
from src.transforms.llm_transform import LLMTransform
from src.utils.app_utils import load_env_vars

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "PROD")


@pytest.fixture
def fake_storage():
    """Use fake storage for faster tests and isolation."""
    adapter = FakeStorageAdapter()
    adapter.create_container()
    service = BlobService(adapter)
    return service


@pytest.fixture(scope='module')
def llm_adapter():
    """Create real LLM adapter for integration tests."""
    load_env_vars()
    adapter = ClaudeAdapter()
    yield adapter
    adapter.close()


@pytest.fixture(scope='module')
def embedding_adapter():
    """Create real embedding adapter for integration tests."""
    # Uses cached model for faster subsequent tests
    adapter = SentenceTransformerAdapter(model_name="all-MiniLM-L6-v2")
    return adapter


@pytest.fixture
def llm_service(llm_adapter):
    return LLMService(llm_adapter)


@pytest.fixture
def embedding_service(embedding_adapter):
    return EmbeddingService(embedding_adapter)


@pytest.fixture
def llm_transform(fake_storage, llm_service):
    return LLMTransform(fake_storage, llm_service)


@pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip integration tests in CI")
class TestClaimCheckerIntegration:
    """Integration tests using real LLM and embedding adapters with fake storage."""
    
    def test_check_obvious_true_claim(self, fake_storage, llm_service, llm_transform, 
                                       embedding_service):
        """Test fact-checking an obviously true claim."""
        # Arrange - create data where claim is clearly supported
        claims = ClaimsV1(claims=[
            "The minimum age requirement changed from 13 to 18 years"
        ])
        
        diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Users must be at least 13 years old to create an account.",
                after="Users must be at least 18 years old to create an account."
            ),
            DiffSection(
                index=1,
                before="We respect your privacy.",
                after="We take your privacy seriously."
            )
        ])
        
        claims_blob = "obvious_true_claims.json"
        diffs_blob = "obvious_true_diffs.json"
        
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
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert
        # Since we send one prompt, we get un-chunked response
        result = FactCheck.model_validate_json(result_json)
        
        # Smoke test: LLM should recognize this is true
        assert result.veracity

    def test_check_obvious_false_claim(self, fake_storage, llm_service, llm_transform,
                                        embedding_service):
        """Test fact-checking an obviously false claim."""
        # Arrange - create data where claim is clearly NOT supported
        claims = ClaimsV1(claims=[
            "The service is now completely free with no advertisements"
        ])
        
        diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Monthly subscription: $9.99",
                after="Monthly subscription: $14.99"
            ),
            DiffSection(
                index=1,
                before="Ads may appear on free tier.",
                after="Ads will appear on all tiers to support development."
            )
        ])
        
        claims_blob = "obvious_false_claims.json"
        diffs_blob = "obvious_false_diffs.json"
        
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
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert
        result = FactCheck.model_validate_json(result_json)
        assert not result.veracity
    
    def test_rag_retrieves_relevant_context(self, fake_storage, llm_service, llm_transform,
                                             embedding_service):
        """Test that RAG retrieves semantically relevant diffs."""
        # Arrange - multiple diffs, only some relevant
        claims = ClaimsV1(claims=[
            "Data collection practices have expanded to include biometric data"
        ])
        
        diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="We collect email and username.",
                after="We collect email, username, fingerprint, and facial recognition data."
            ),
            DiffSection(
                index=1,
                before="Service available in 50 countries.",
                after="Service available in 75 countries."
            ),
            DiffSection(
                index=2,
                before="Support hours: 9am-5pm EST",
                after="Support hours: 24/7"
            ),
            DiffSection(
                index=3,
                before="Logo design by Example Corp.",
                after="Logo design by New Design LLC."
            )
        ])
        
        claims_blob = "rag_claims.json"
        diffs_blob = "rag_diffs.json"
        
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
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert - just verify it completes successfully
        result = FactCheck.model_validate_json(result_json)
        assert "biometric" in result.reason or "facial" in result.reason or "fingerprint" in result.reason
    
    def test_multiple_positive_claims(self, fake_storage, llm_service, llm_transform,
                                         embedding_service):
        """Test processing multiple claims in one batch."""
        # Arrange
        claims = ClaimsV1(claims=[
            "Age restriction increased",
            "New payment options added",
            "Privacy policy simplified"
        ])
        
        diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Minimum age: 13",
                after="Minimum age: 16"
            ),
            DiffSection(
                index=1,
                before="Payment: Credit card only",
                after="Payment: Credit card, PayPal, cryptocurrency"
            ),
            DiffSection(
                index=2,
                before="[Long complex privacy text...]",
                after="[Shorter, clearer privacy text...]"
            )
        ])
        
        claims_blob = "multi_claims.json"
        diffs_blob = "multi_diffs.json"
        
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
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)
        
        # Assert
        result_list = json.loads(result_json)['chunks']
        results = [FactCheck.model_validate(x) for x in result_list]
        assert all(x.veracity for x in results)
        assert len(results) == 3

    def test_positive_negative_claims(self, fake_storage, llm_service, llm_transform,
                                         embedding_service):
        """Test processing multiple claims in one batch."""
        # Arrange
        claims = ClaimsV1(claims=[
            "Age restriction increased",
            "New payment options added",
        ])

        diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Minimum age: 21",
                after="Minimum age: 13"
            ),
            DiffSection(
                index=1,
                before="Payment: Credit card only",
                after="Payment: Credit card, PayPal, cryptocurrency"
            ),
        ])

        claims_blob = "multi_claims.json"
        diffs_blob = "multi_diffs.json"

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

        # Act
        result_json, metadata = checker.check_claim(claims_blob, diffs_blob)

        # Assert
        result_list = json.loads(result_json)['chunks']
        results = [FactCheck.model_validate(x) for x in result_list]
        assert len(results) == 2
        assert any(x.veracity for x in results)
        assert not all(x.veracity for x in results)
