import os
import pytest
import json

from schemas.llmerror.v1 import LLMError
from schemas.summary.v3 import Summary as SummaryV3, VERSION as VERSIONV3
from schemas.summary.v4 import Summary as SummaryV4, VERSION as VERSIONV4
from schemas.summary.v2 import Summary as SummaryV2, Substantive, VERSION as VERSIONV2
from schemas.fact.v1 import Claims
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

# TODO: Uncomment
# @pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip for CI")
class TestClaimExtractor:
    """Integration tests using real LLM adapter with fake storage."""

    def test_positive(self, fake_storage, llm_transform):
        """Test extracting claims from substantive change (original test)."""
        # Arrange
        data = SummaryV4(practically_substantive=Substantive(rating=True, reason="Changes age restrictions from 12+ to 15+"))

        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSIONV4})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("test.json")

        # Assert
        result = Claims.model_validate_json(response)
        assert isinstance(result, Claims)
        assert len(result.claims) > 0
        assert any("15" in c for c in result.claims) or any("fifteen" in c for c in result.claims)

    def test_multiple_positive(self, fake_storage, llm_transform):
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
        fake_storage.upload_text_blob(data_serialized, "multi_test.json", metadata={"schema_version": VERSIONV3})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("multi_test.json")

        # Assert
        result = Claims.model_validate_json(response)
        assert len(result.claims) >= 2

    def test_positive_negative(self, fake_storage, llm_transform):
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
        fake_storage.upload_text_blob(data_serialized, "mixed_test.json", metadata={"schema_version": VERSIONV3})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("mixed_test.json")

        # Assert
        result = Claims.model_validate_json(response)
        assert len(result.claims) >= 1
        assert not (any("address" in c for c in result.claims) or any("formatting" in c for c in result.claims))

    def test_only_negative(self, fake_storage, llm_transform):
        """Test extracting claims when all changes are non-substantive."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=False,
                reason="Privacy policy sections were reordered alphabetically"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "nonsubstantive_test.json", metadata={"schema_version": VERSIONV3})

        # Act
        extractor = ClaimExtractor(fake_storage, llm_transform)
        response, metadata = extractor.extract_claims("nonsubstantive_test.json")

        # Assert
        LLMError.model_validate_json(response)
