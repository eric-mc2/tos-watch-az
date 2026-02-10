import json
from collections import namedtuple
from typing import NamedTuple

import pytest
from transformers.models.bloom.modeling_bloom import bloom_gelu_back

from schemas.claim.v1 import Claims as ClaimsV1, VERSION as CLAIM_VERSION
from schemas.factcheck.v1 import FactCheck
from schemas.llmerror.v1 import LLMError
from src.stages import Stage
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
def sample_claim():
    """Sample single claim for testing."""
    return ClaimsV1(claims=[
        "The document mentions age restrictions changed from 12+ to 15+",
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

BlobNames = namedtuple("blob_names", ["single_claim_blob", "multi_claims_blob", "diffs_blob"])

@pytest.fixture
def blob_names() -> BlobNames:
    """Sample blob names for testing."""
    return BlobNames("claim.json", "claims.json", "diff.json")

@pytest.fixture
def upload_test_data(fake_storage, sample_claims, sample_claim, sample_diffs, blob_names):

    fake_storage.upload_text_blob(
        sample_claims.model_dump_json(),
        blob_names.multi_claims_blob,
        metadata={"schema_version": CLAIM_VERSION}
    )
    fake_storage.upload_text_blob(
        sample_claim.model_dump_json(),
        blob_names.single_claim_blob,
        metadata={"schema_version": CLAIM_VERSION}
    )
    fake_storage.upload_text_blob(
        sample_diffs.model_dump_json(),
        blob_names.diffs_blob,
        metadata={}
    )


class TestClaimCheckerBuilder:
    """Unit tests for ClaimCheckerBuilder using fake adapters."""
    
    def test_single_claim(self, fake_storage, embedding_service, upload_test_data, blob_names):
        """Test that builder creates prompts for each claim."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service)
        
        # Act
        prompts = list(builder.build_prompt(blob_names.single_claim_blob, blob_names.diffs_blob))
        
        # Assert
        assert len(prompts) == 1  # One prompt per claim

    def test_multiple_claims(self, fake_storage, embedding_service, upload_test_data, blob_names):
        """Test that builder creates prompts for each claim."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service)

        # Act
        prompts = list(builder.build_prompt(blob_names.multi_claims_blob, blob_names.diffs_blob))

        # Assert
        assert len(prompts) == 3  # One prompt per claim

    def test_empty_claims(self, fake_storage, embedding_service, upload_test_data, blob_names):
        """Test handling of empty claims list."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service)
        
        empty_claims = ClaimsV1(claims=[])
        claims_blob = "empty_claims.json"

        fake_storage.upload_text_blob(
            empty_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )

        # Act
        prompts = list(builder.build_prompt(claims_blob, blob_names.diffs_blob))
        
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

    @pytest.mark.skip
    def test_empty_rag(self, fake_storage, embedding_service, upload_test_data):
        pass # TODO: Need to test FAISS failure modes like when k > diffs

    @pytest.mark.skip
    def test_migration(self, fake_storage, embedding_service):
        pass # TODO: test when there is ClaimsV2

class TestClaimChecker:
    """Unit tests for ClaimChecker using fake adapters."""

    def test_multiple_claims(self, fake_storage, llm_service, llm_transform,
                                embedding_service, upload_test_data, blob_names):
        """Test basic claim checking workflow."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )

        # Configure fake LLM to return valid fact-check responses
        # Fake LLM is prompted and gives sames response every time.
        llm_service.adapter.set_response(
            FactCheck(veracity=True, reason="because").model_dump_json()
        )
        
        # Act
        result_json, metadata = checker.check_claim(blob_names.multi_claims_blob, blob_names.diffs_blob)
        
        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # Verify result structure
        results = [FactCheck.model_validate(x) for x in json.loads(result_json)['chunks']]
        assert len(results) == 3

    def test_single_claim(self, fake_storage, llm_service, llm_transform,
                                embedding_service, upload_test_data, blob_names):
        """Test basic claim checking workflow."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )

        # Configure fake LLM to return valid fact-check responses
        # Fake LLM is prompted and gives sames response every time.
        llm_service.adapter.set_response(
            FactCheck(veracity=True, reason="because").model_dump_json()
        )

        # Act
        result_json, metadata = checker.check_claim(blob_names.single_claim_blob, blob_names.diffs_blob)

        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # Verify result structure
        # Since we only passed one claim, the result is stored as non-chunked.
        FactCheck.model_validate_json(result_json)


    def test_extraneous_llm_text(self, fake_storage,
                                 llm_transform,
                                 llm_service,
                                 embedding_service,
                                 upload_test_data,
                                 blob_names):
        """Test basic claim extraction workflow."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )

        # Configure fake LLM
        check = FactCheck(veracity=True, reason="because")
        llm_service.adapter.set_response(
            "I can help with that \n" + \
            check.model_dump_json() + \
            "Would you like more help?"
        )

        # Act
        result_json, metadata = checker.check_claim(blob_names.multi_claims_blob, blob_names.diffs_blob)

        # Assert
        results = [FactCheck.model_validate(x) for x in json.loads(result_json)['chunks']]
        assert results[0] == check

    def test_invalid_json_llm(self, fake_storage,
                              llm_transform,
                              llm_service,
                              embedding_service,
                              upload_test_data,
                              blob_names):
        """Test basic claim extraction workflow."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )

        # Configure fake LLM
        llm_service.adapter.set_response("{'foo'")

        # Act
        result_json, metadata = checker.check_claim(blob_names.multi_claims_blob, blob_names.diffs_blob)

        # Assert
        [LLMError.model_validate(x) for x in json.loads(result_json)['chunks']]
