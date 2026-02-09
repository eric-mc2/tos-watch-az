import pytest
import json

from schemas.summary.v3 import Summary as SummaryV3, VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive, VERSION as OLD_VERSION
from schemas.claim.v1 import Claims
from src.transforms.factcheck.claim_extractor import ClaimExtractorBuilder, ClaimExtractor
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService
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
def llm_service(fake_llm):
    return LLMService(fake_llm)


@pytest.fixture
def llm_transform(fake_storage, llm_service):
    return LLMTransform(fake_storage, llm_service)


class TestClaimExtractorBuilder:
    """Unit tests for ClaimExtractorBuilder using fake adapters."""

    def test_positive(self, fake_storage):
        """Test extracting claims from positive (substantive) analysis."""
        # Arrange
        builder = ClaimExtractorBuilder(fake_storage)
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Reason"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})
        
        # Act
        prompts = list(builder.build_prompt("test.json"))
        prompt = prompts[0]

        # Assert
        assert len(prompts) == 1
        assert prompt.system is not None
        assert prompt.current.role == "user"
        assert "Reason" in prompt.current.content

    def test_negative(self, fake_storage):
        """Test extracting claims from negative (non-substantive) analysis."""
        # Arrange
        builder = ClaimExtractorBuilder(fake_storage)
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=False,
                reason="Irrelevant"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})
        
        # Act
        prompts = list(builder.build_prompt("test.json"))
        
        # Assert
        # Should not create prompt for pure negative cases
        assert len(prompts) == 0

    def test_positive_and_negative(self, fake_storage):
        """Test extracting claims from mixed substantive and non-substantive chunks."""
        # Arrange
        builder = ClaimExtractorBuilder(fake_storage)
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Substantive"
            )),
            SummaryV2(practically_substantive=Substantive(
                rating=False,
                reason="Irrelevant"
            )),
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})
        
        # Act
        prompts = list(builder.build_prompt("test.json"))
        prompt_content = prompts[0].current.content

        # Assert
        assert len(prompts) == 1
        # Only positive reasons should be included
        assert "Substantive" in prompt_content
        assert "Irrelevant" not in prompt_content

    def test_migration(self, fake_storage):
        # Arrange
        builder = ClaimExtractorBuilder(fake_storage)
        data = SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Reason"
            ))
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized,
                                        "test.json",
                                      metadata={"schema_version": OLD_VERSION})

        # Act
        prompts = list(builder.build_prompt("test.json"))
        prompt = prompts[0]

        # Assert
        assert len(prompts) == 1
        assert "Reason" in prompt.current.content

class TestClaimExtractor:
    """Unit tests for ClaimExtractor using fake adapters."""
    
    def test_extract_claims_basic(self, fake_storage, llm_transform, llm_service):
        """Test basic claim extraction workflow."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Something"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})
        
        # Configure fake LLM response
        response = Claims(claims=["a claim"])
        llm_service.adapter.set_response(response.model_dump_json())
        
        extractor = ClaimExtractor(fake_storage, llm_transform)
        
        # Act
        result_json, metadata = extractor.extract_claims("test.json")
        
        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata
        
        result = Claims.model_validate_json(result_json)
        assert result == response
    
    def test_extract_multiple_claims(self, fake_storage, llm_transform, llm_service):
        """Test extracting multiple claims from analysis."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Something"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})
        
        # Configure fake LLM to return multiple claims
        response = Claims(claims=["claim 1", "claim 2"])
        llm_service.adapter.set_response(response.model_dump_json())
        
        extractor = ClaimExtractor(fake_storage, llm_transform)
        
        # Act
        result_json, metadata = extractor.extract_claims("test.json")
        
        # Assert
        result = Claims.model_validate_json(result_json)
        assert result == response
    
    def test_extract_claims_empty_response(self, fake_storage, llm_transform, llm_service):
        """Test handling when LLM returns no claims."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Something"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})
        
        # Configure fake LLM to return empty claims list
        response = Claims(claims=[])
        llm_service.adapter.set_response(response.model_dump_json())
        
        extractor = ClaimExtractor(fake_storage, llm_transform)
        
        # Act
        result_json, metadata = extractor.extract_claims("test.json")

        # Assert
        # This is actually OK in this step because claim_extractor does not validate output!!
        result = Claims.model_validate_json(result_json)
        assert result == response

    def test_extraneous_llm_text(self, fake_storage, llm_transform, llm_service):
        """Test basic claim extraction workflow."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Something"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})

        # Configure fake LLM response
        response = Claims(claims=["a claim"])
        llm_service.adapter.set_response(
            "OK here is your answer: " + \
            response.model_dump_json() + \
            " Is there anything else I can assist with?")


        extractor = ClaimExtractor(fake_storage, llm_transform)

        # Act
        result_json, metadata = extractor.extract_claims("test.json")

        # Assert
        result = Claims.model_validate_json(result_json)
        assert result == response

    def test_invalid_json_llm(self, fake_storage, llm_transform, llm_service):
        """Test basic claim extraction workflow."""
        # Arrange
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=Substantive(
                rating=True,
                reason="Something"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, "test.json", metadata={"schema_version": VERSION})

        # Configure fake LLM response
        response = Claims(claims=["a claim"])
        llm_service.adapter.set_response("what is json lol")

        extractor = ClaimExtractor(fake_storage, llm_transform)

        # Act
        result_json, metadata = extractor.extract_claims("test.json")

        # Assert
        result = json.loads(result_json)
        assert "error" in result
        assert "raw" in result
