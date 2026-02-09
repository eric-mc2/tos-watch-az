import pytest
import json

from schemas.summary.v3 import Summary as SummaryV3, VERSION as SUMMARY_VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive
from schemas.factcheck.v1 import FactCheck, VERSION as FACTCHECK_VERSION
from schemas.judge.v1 import VERSION as JUDGE_VERSION
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
    return SummaryV3(chunks=[
        SummaryV2(
            practically_substantive=Substantive(
                rating=True,
                reason="The service now requires users to be 15+ instead of 12+, which is a material change to user eligibility."
            )
        ),
        SummaryV2(
            practically_substantive=Substantive(
                rating=True,
                reason="New data collection includes location tracking and device fingerprinting."
            )
        )
    ])


@pytest.fixture
def sample_factcheck():
    """Sample fact check for testing."""
    return FactCheck(claims=[
        "The minimum age changed from 12 to 15 years",
        "Location data is now collected",
        "Device information is now collected"
    ])


class TestJudgeBuilder:
    """Unit tests for JudgeBuilder using fake adapters."""
    
    def test_build_prompt_basic(self, fake_storage, sample_summary, sample_factcheck):
        """Test that builder creates prompts correctly."""
        # Arrange
        builder = JudgeBuilder(fake_storage)
        
        summary_blob = "summary.json"
        facts_blob = "facts.json"
        
        fake_storage.upload_text_blob(
            sample_summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        # Act
        prompts = list(builder.build_prompt(summary_blob, facts_blob))
        
        # Assert
        assert len(prompts) == 1  # Judge produces single judgment
        prompt = prompts[0]
        assert prompt.system is not None
        assert prompt.current.role == "user"
        
        # Verify prompt contains both summary and facts
        content = json.loads(prompt.current.content)
        assert "summary" in content
        assert "facts" in content
    
    def test_build_prompt_loads_summary(self, fake_storage, sample_summary, sample_factcheck):
        """Test that summary is properly loaded and included."""
        # Arrange
        builder = JudgeBuilder(fake_storage)
        
        summary_blob = "summary.json"
        facts_blob = "facts.json"
        
        fake_storage.upload_text_blob(
            sample_summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        # Act
        prompts = list(builder.build_prompt(summary_blob, facts_blob))
        prompt_content = json.loads(prompts[0].current.content)
        
        # Assert
        assert "chunks" in prompt_content["summary"]
        assert len(prompt_content["summary"]["chunks"]) == 2
    
    def test_build_prompt_loads_factcheck(self, fake_storage, sample_summary, sample_factcheck):
        """Test that fact checks are properly loaded and included."""
        # Arrange
        builder = JudgeBuilder(fake_storage)
        
        summary_blob = "summary.json"
        facts_blob = "facts.json"
        
        fake_storage.upload_text_blob(
            sample_summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        # Act
        prompts = list(builder.build_prompt(summary_blob, facts_blob))
        prompt_content = json.loads(prompts[0].current.content)
        
        # Assert
        assert "claims" in prompt_content["facts"]
        assert len(prompt_content["facts"]["claims"]) == 3


class TestJudge:
    """Unit tests for Judge using fake adapters."""
    
    def test_judge_basic(self, fake_storage, llm_service, llm_transform, 
                         sample_summary, sample_factcheck):
        """Test basic judging workflow."""
        # Arrange
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        summary_blob = "summary.json"
        facts_blob = "facts.json"
        
        fake_storage.upload_text_blob(
            sample_summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        # Configure fake LLM to return valid judgment
        llm_service.adapter.set_response(
            '{"practically_substantive": {"rating": true, "reason": "Age restriction is material."}}'
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        assert result_json is not None
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        
        # Verify result structure
        result = SummaryV3.model_validate_json(result_json)
        assert "chunks" in result
    
    def test_judge_substantive_true(self, fake_storage, llm_service, llm_transform,
                                     sample_summary, sample_factcheck):
        """Test judgment when changes are substantive."""
        # Arrange
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        summary_blob = "summary.json"
        facts_blob = "facts.json"
        
        fake_storage.upload_text_blob(
            sample_summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            sample_factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        # Configure LLM for substantive change
        llm_service.adapter.set_response(
            '{"practically_substantive": {"rating": true, "reason": "Material changes to user age requirements and data collection."}}'
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = SummaryV3.model_validate_json(result_json)
        assert len(result["chunks"]) == 1
        assert result["chunks"][0]["practically_substantive"]["rating"] is True
    
    def test_judge_substantive_false(self, fake_storage, llm_service, llm_transform):
        """Test judgment when changes are not substantive."""
        # Arrange
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Non-substantive summary
        non_substantive_summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=False,
                    reason="Only formatting changes, no material impact."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "Section headers were reorganized",
            "Font size changed in privacy policy"
        ])
        
        summary_blob = "summary.json"
        facts_blob = "facts.json"
        
        fake_storage.upload_text_blob(
            non_substantive_summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        # Configure LLM for non-substantive change
        llm_service.adapter.set_response(
            '{"practically_substantive": {"rating": false, "reason": "Only cosmetic changes with no material impact."}}'
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = SummaryV3.model_validate_json(result_json)
        assert result["chunks"][0]["practically_substantive"]["rating"] is False


class TestJudgeIntegration:
    """Integration-style tests using fake adapters but testing full workflow."""
    
    def test_end_to_end_judging(self, fake_storage, llm_service, llm_transform):
        """Test complete judging workflow from start to finish."""
        # Arrange - create realistic test data
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="Service introduces mandatory arbitration clause, removing users' right to sue."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "New arbitration clause added to terms",
            "Users waive right to class action lawsuits",
            "Disputes must be resolved through binding arbitration"
        ])
        
        summary_blob = "e2e_summary.json"
        facts_blob = "e2e_facts.json"
        
        fake_storage.upload_text_blob(
            summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Configure LLM response
        llm_service.adapter.set_response(
            '{"practically_substantive": {"rating": true, "reason": "Arbitration requirement materially affects dispute resolution rights."}}'
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = SummaryV3.model_validate_json(result_json)
        assert len(result["chunks"]) == 1
        judgment = result["chunks"][0]["practically_substantive"]
        assert judgment["rating"] is True
        assert "arbitration" in judgment["reason"].lower() or "dispute" in judgment["reason"].lower()
    
    def test_judge_with_mixed_evidence(self, fake_storage, llm_service, llm_transform):
        """Test judging when evidence is mixed."""
        # Arrange
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="Terms now require government ID verification for all users."
                )
            ),
            SummaryV2(
                practically_substantive=Substantive(
                    rating=False,
                    reason="Contact email address was updated."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "ID verification is now required",
            "Contact email changed from old@example.com to new@example.com"
        ])
        
        summary_blob = "mixed_summary.json"
        facts_blob = "mixed_facts.json"
        
        fake_storage.upload_text_blob(
            summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            factcheck.model_dump_json(), 
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # LLM should focus on substantive aspects
        llm_service.adapter.set_response(
            '{"practically_substantive": {"rating": true, "reason": "ID verification is substantive; email change is not."}}'
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert - should succeed without errors
        result = SummaryV3.model_validate_json(result_json)
        assert "chunks" in result
