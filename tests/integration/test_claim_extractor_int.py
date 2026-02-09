import os
import pytest
import json

from schemas.summary.v3 import Summary as SummaryV3, VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive, VERSION as OLD_VERSION
from src.adapters.llm.client import ClaudeAdapter
from src.services.llm import LLMService
from src.transforms.factcheck.claim_extractor import ClaimExtractorBuilder, ClaimExtractor
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform
from src.utils.app_utils import load_env_vars

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "PROD")

@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter()
    adapter.create_container()
    service = BlobService(adapter)
    return service


@pytest.fixture(scope='module')
def llm():
    """Create a fresh storage adapter with a test container"""
    load_env_vars()
    adapter = ClaudeAdapter()

    yield adapter

    # Teardown
    adapter.close()


@pytest.fixture
def llm_service(llm):
    return LLMService(llm)


@pytest.fixture
def llm_transform(fake_storage, llm_service):
    return LLMTransform(fake_storage, llm_service)

@pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip for CI")
class TestClaimExtractor:
    """Integration tests using real LLM adapter with fake storage."""

    def test_positive(self, fake_storage, llm_transform):
        """Test extracting claims from substantive change (original test)."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(rating=True,
                                                          reason="Changes age restrictions from 12+ to 15+"))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("test.json")

        # Assert
        print(response)
        result = json.loads(response)
        assert "chunks" in result

    def test_extract_from_multiple_substantive_changes(self, fake_storage, llm_transform):
        """Test extracting claims from multiple substantive changes."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="New data collection includes biometric information and location tracking"
            )),
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Subscription model changed from annual to monthly with price increase"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "multi_test.json", metadata={"schema_version": VERSION})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("multi_test.json")

        # Assert
        result = json.loads(response)
        claims = result["chunks"][0]["claims"]
        print(f"Extracted claims from multiple changes: {claims}")
        
        # Should extract claims about both data collection and pricing
        # Smoke test: should have multiple claims for multiple changes
        assert len(claims) >= 2

    def test_extract_mixed_substantive_nonsubstantive(self, fake_storage, llm_transform):
        """Test extracting claims when some changes are substantive and others aren't."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Users can now be banned permanently without appeal"
            )),
            SummaryV2(practically_substantive=Substantive(
                rating=False,
                reason="Contact address formatting was updated"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "mixed_test.json", metadata={"schema_version": VERSION})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("mixed_test.json")

        # Assert
        result = json.loads(response)
        claims = result["chunks"][0]["claims"]
        print(f"Claims from mixed changes: {claims}")
        
        # Should focus on substantive aspects
        assert len(claims) >= 1

    def test_extract_from_nonsubstantive_only(self, fake_storage, llm_transform):
        """Test extracting claims when all changes are non-substantive."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=False,
                reason="Privacy policy sections were reordered alphabetically"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "nonsubstantive_test.json", metadata={"schema_version": VERSION})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("nonsubstantive_test.json")

        # Assert
        result = json.loads(response)
        print(f"Response for non-substantive: {result}")
        # Should still return claims structure even if empty or minimal
        assert "chunks" in result

    def test_extract_realistic_privacy_change(self, fake_storage, llm_transform):
        """Test with realistic privacy policy change scenario."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Privacy policy now allows sharing user data with third-party advertisers and data brokers for marketing purposes"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "privacy_test.json", metadata={"schema_version": VERSION})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("privacy_test.json")

        # Assert
        result = json.loads(response)
        claims = result["chunks"][0]["claims"]
        print(f"Privacy change claims: {claims}")
        
        # Should extract specific claims about data sharing
        assert len(claims) >= 1

    def test_metadata_preservation(self, fake_storage, llm_transform):
        """Test that metadata is properly preserved through extraction."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Service terms now include forced arbitration"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "metadata_test.json", metadata={"schema_version": VERSION})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("metadata_test.json")

        # Assert metadata
        assert "schema_version" in metadata
        assert "run_id" in metadata
        print(f"Preserved metadata: {metadata}")
        
        # Assert response structure
        result = json.loads(response)
        assert "chunks" in result