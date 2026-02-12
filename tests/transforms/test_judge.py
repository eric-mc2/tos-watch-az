from collections import namedtuple

import pytest

from schemas.summary.v4 import Summary as SummaryV4, VERSION as SUMMARY_VERSION
from schemas.summary.v3 import Summary as SummaryV3, VERSION as SUMMARY_VERSION_V3
from schemas.summary.v2 import Summary as SummaryV2, Substantive as SummarySubstantive
from schemas.fact.v1 import Fact, Proof, FACT_VERSION
from schemas.judge.v1 import Judgement, Substantive as JudgementSubstantive
from src.transforms.factcheck.judge import JudgeBuilder, Judge
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


@pytest.fixture
def sample_summary():
    """Sample summary for testing."""
    return SummaryV4(
        practically_substantive=SummarySubstantive(
            rating=True,
            reason="because"
        ))

@pytest.fixture
def sample_proof():
    """Sample fact check for testing."""
    return Proof(facts=[Fact(claim="something", veracity=True, reason="because"),
                        Fact(claim="other", veracity=False, reason="because")])


BlobNames = namedtuple("BlobNames", ["summary_blob", "fact_blob"])

@pytest.fixture
def blob_names() -> BlobNames:
    """Sample blob names for testing."""
    return BlobNames("summary.json", "factcheck.json")


@pytest.fixture
def upload_test_data(fake_storage, sample_summary, sample_proof, blob_names):

    fake_storage.upload_text_blob(
        sample_summary.model_dump_json(),
        blob_names.summary_blob,
        metadata={"schema_version": SUMMARY_VERSION}
    )
    fake_storage.upload_text_blob(
        sample_proof.model_dump_json(),
        blob_names.fact_blob,
        metadata={"schema_version": FACT_VERSION}
    )


class TestJudgeBuilder:
    """Unit tests for JudgeBuilder using fake adapters."""
    
    def test_build_prompt_basic(self, fake_storage, upload_test_data, blob_names):
        """Test that builder creates prompts correctly."""
        # Arrange
        builder = JudgeBuilder(fake_storage)

        # Act
        prompts = list(builder.build_prompt(blob_names.fact_blob, blob_names.summary_blob))
        
        # Assert
        assert len(prompts) == 1  # Judge produces single prompt
        prompt = prompts[0]
        assert prompt.system is not None
        assert prompt.current.role == "user"
    
    def test_build_prompt_loads_summary(self, fake_storage, sample_summary, upload_test_data, blob_names):
        """Test that summary is properly loaded and included."""
        # Arrange
        builder = JudgeBuilder(fake_storage)
        
        # Act
        prompts = list(builder.build_prompt(blob_names.fact_blob, blob_names.summary_blob))
        prompt_content = prompts[0].current.content
        
        # Assert
        assert sample_summary.practically_substantive.reason in prompt_content

    def test_build_prompt_loads_proof(self, fake_storage, sample_summary, sample_proof, upload_test_data, blob_names):
        """Test that fact checks are properly loaded and included."""
        # Arrange
        builder = JudgeBuilder(fake_storage)
        
        # Act
        prompts = list(builder.build_prompt(blob_names.fact_blob, blob_names.summary_blob))
        prompt_content = prompts[0].current.content
        
        # Assert
        assert all(x.reason in prompt_content for x in sample_proof.facts)

    def test_summary_migration(self, fake_storage, sample_summary, sample_proof, upload_test_data, blob_names):
        # Arrange
        builder = JudgeBuilder(fake_storage)
        data = SummaryV3(chunks=[
            SummaryV2(practically_substantive=SummarySubstantive(
                rating=True,
                reason="pos")),
            SummaryV2(practically_substantive=SummarySubstantive(
                rating=False,
                reason="neg"
            ))
        ])
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized,
                                      "old_summary.json",
                                      metadata={"schema_version": SUMMARY_VERSION_V3})

        # Act
        prompts = list(builder.build_prompt(blob_names.fact_blob, "old_summary.json"))
        prompt = prompts[0]

        # Assert
        assert len(prompts) == 1
        assert "pos" in prompt.current.content
        assert "neg" not in prompt.current.content


class TestJudge:
    """Unit tests for Judge using fake adapters."""
    
    def test_judge_basic(self, fake_storage,
                         llm_service,
                         llm_transform,
                         sample_summary,
                         upload_test_data,
                         blob_names):
        """Test basic judging workflow."""
        # Arrange
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Configure fake LLM to return valid judgment
        llm_service.adapter.set_response(
            Judgement(practically_substantive=JudgementSubstantive(
                rating=True,
                reason="because")).model_dump_json())
        
        # Act
        result_json, metadata = judge.judge(blob_names.fact_blob, blob_names.summary_blob)
        
        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # Verify result structure
        Judgement.model_validate_json(result_json)
