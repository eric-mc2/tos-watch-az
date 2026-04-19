import json
from collections import namedtuple
import pytest

from schemas.fact.v1 import Claims as ClaimsV1, CLAIMS_VERSION as CLAIM_VERSION, Fact, Proof, merge_facts, FACT_MODULE
from schemas.fact.v1 import Fact
from schemas.llmerror.v1 import LLMError
from src.stages import Stage
from src.transforms.factcheck.claim_checker import ClaimCheckerBuilder, ClaimChecker
from src.transforms.differ import DiffDoc, DiffSection
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.adapters.embedding.fake_client import FakeEmbeddingAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService, TOKEN_LIMIT
from src.services.embedding import EmbeddingService
from src.transforms.llm_transform import LLMTransform, create_llm_parser


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

BlobNames = namedtuple("BlobNames", ["single_claim_blob", "multi_claims_blob", "diffs_blob"])

@pytest.fixture
def blob_names(fake_storage) -> BlobNames:
    """Sample blob names for testing."""
    single_name = fake_storage.unparse_blob_path((Stage.CLAIM_RAW.value, "c","p","123456789","claim.json"))
    multi_name = fake_storage.unparse_blob_path((Stage.CLAIM_RAW.value, "c","p","123456789","claims.json"))
    diff_name = fake_storage.unparse_blob_path((Stage.DIFF_CLEAN.value, "c","p","123456789.json"))
    return BlobNames(single_name, multi_name, diff_name)

@pytest.fixture
def upload_test_data(fake_storage, sample_claims, sample_claim, sample_diffs, blob_names):

    fake_storage.upload_text_blob(
        sample_claims.model_dump_json(),
        blob_names.multi_claims_blob,
        metadata={"claim_schema_version": CLAIM_VERSION}
    )
    fake_storage.upload_text_blob(
        sample_claim.model_dump_json(),
        blob_names.single_claim_blob,
        metadata={"claim_schema_version": CLAIM_VERSION}
    )
    fake_storage.upload_text_blob(
        sample_diffs.model_dump_json(),
        blob_names.diffs_blob,
        metadata={}
    )


class TestClaimCheckerBuilder:
    """Unit tests for ClaimCheckerBuilder using fake adapters."""
    
    def test_single_claim(self, fake_storage, embedding_service, upload_test_data, blob_names, llm_service):
        """Test that builder creates prompts for each claim."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service, llm_service)
        
        # Act
        prompts = list(builder.build_prompt(blob_names.single_claim_blob, blob_names.diffs_blob))
        
        # Assert
        assert len(prompts) == 1  # One prompt per claim

    def test_multiple_claims(self, fake_storage, embedding_service, upload_test_data, blob_names, llm_service):
        """Test that builder creates prompts for each claim."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service, llm_service)

        # Act
        prompts = list(builder.build_prompt(blob_names.multi_claims_blob, blob_names.diffs_blob))

        # Assert
        assert len(prompts) == 3  # One prompt per claim

    def test_empty_claims(self, fake_storage, embedding_service, upload_test_data, blob_names, llm_service):
        """Test handling of empty claims list."""
        # Arrange
        builder = ClaimCheckerBuilder(fake_storage, embedding_service, llm_service)
        
        empty_claims = ClaimsV1(claims=[])
        claims_blob = "empty_claims.json"

        fake_storage.upload_text_blob(
            empty_claims.model_dump_json(), 
            claims_blob, 
            metadata={"claim_schema_version": CLAIM_VERSION}
        )

        # Act
        prompts = list(builder.build_prompt(claims_blob, blob_names.diffs_blob))
        
        # Assert
        assert len(prompts) == 0


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
        llm_service.adapter.set_response_static(
            Fact(claim="something", veracity=True, reason="because").model_dump_json()
        )
        
        # Act
        result_json, metadata = checker.check_claim(blob_names.multi_claims_blob)
        
        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # Verify result structure
        results = [Fact.model_validate(x) for x in json.loads(result_json)['chunks']]
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
        llm_service.adapter.set_response_static(
            Fact(claim="something", veracity=True, reason="because").model_dump_json()
        )

        # Act
        result_json, metadata = checker.check_claim(blob_names.single_claim_blob)

        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # Verify result structure
        # Since we only passed one claim, the result is stored as non-chunked.
        Fact.model_validate_json(result_json)


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
        check = Fact(claim="something", veracity=True, reason="because")
        llm_service.adapter.set_response_static(
            "I can help with that \n" + \
            check.model_dump_json() + \
            "Would you like more help?"
        )

        # Act
        result_json, metadata = checker.check_claim(blob_names.multi_claims_blob)

        # Assert
        results = [Fact.model_validate(x) for x in json.loads(result_json)['chunks']]
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
        llm_service.adapter.set_response_static("{'foo'")

        # Act
        result_json, metadata = checker.check_claim(blob_names.multi_claims_blob)

        # Assert
        [LLMError.model_validate(x) for x in json.loads(result_json)['chunks']]

    def test_zero_claims(self, fake_storage, llm_service, llm_transform,
                         embedding_service, blob_names):
        """Test handling of empty claims list."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )
        
        empty_claims = ClaimsV1(claims=[])
        claims_blob = f"{Stage.CLAIM_CLEAN.value}/c/p/123/latest.json"
        diffs_blob = f"{Stage.DIFF_CLEAN.value}/c/p/123.json"
        
        fake_storage.upload_text_blob(
            empty_claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        
        sample_diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Old text.",
                after="New text."
            )
        ])
        fake_storage.upload_text_blob(
            sample_diffs.model_dump_json(),
            diffs_blob,
            metadata={}
        )
        
        # Configure fake LLM (shouldn't be called)
        llm_service.adapter.set_response_static(
            Fact(claim="should not appear", veracity=True, reason="N/A").model_dump_json()
        )
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob)
        
        # Assert - should handle empty claims gracefully
        assert result_json is not None

    def test_long_diffs_chunking(self, fake_storage, llm_service, llm_transform,
                                  embedding_service):
        """Test claim checking with very long diffs that require chunking."""
        # Arrange
        checker = ClaimChecker(
            storage=fake_storage,
            executor=llm_transform,
            embedder=embedding_service
        )
        
        claims = ClaimsV1(claims=["The document has been significantly expanded"])
        
        # Create diffs that exceed TOKEN_LIMIT
        long_diffs = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Old terms. " * (TOKEN_LIMIT // 3),
                after="New expanded terms. " * (TOKEN_LIMIT // 3)
            ),
            DiffSection(
                index=1,
                before="Additional old content. " * (TOKEN_LIMIT // 3),
                after="Additional new content. " * (TOKEN_LIMIT // 3)
            )
        ])
        
        claims_blob = f"{Stage.CLAIM_CLEAN.value}/c/p/456/latest.json"
        diffs_blob = f"{Stage.DIFF_CLEAN.value}/c/p/456.json"
        
        fake_storage.upload_text_blob(
            claims.model_dump_json(), 
            claims_blob, 
            metadata={"schema_version": CLAIM_VERSION}
        )
        fake_storage.upload_text_blob(
            long_diffs.model_dump_json(), 
            diffs_blob, 
            metadata={}
        )
        
        # Configure fake LLM
        llm_service.adapter.set_response_static(
            Fact(claim="The document has been significantly expanded", 
                 veracity=True, 
                 reason="Document size increased").model_dump_json()
        )
        
        # Act
        result_json, metadata = checker.check_claim(claims_blob)
        parser = create_llm_parser(llm_service, FACT_MODULE, merge_facts)
        result_json, metadata = parser(result_json, metadata)
        
        # Assert - should successfully process despite large input
        assert result_json is not None
        result = Proof.model_validate_json(result_json)
        assert len(result.facts) > 0
        assert all(f.claim == "The document has been significantly expanded" for f in result.facts)
        assert all(f.veracity == True for f in result.facts)
