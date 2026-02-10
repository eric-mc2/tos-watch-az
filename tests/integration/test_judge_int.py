import os
import pytest
import json

from schemas.judge.v1 import Judgement
from schemas.summary.v4 import Summary as SummaryV4, VERSION as SUMMARY_VERSION
from schemas.summary.v2 import Substantive
from schemas.factcheck.v1 import FactCheck, VERSION as FACTCHECK_VERSION
from src.transforms.factcheck.judge import Judge
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.client import ClaudeAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService
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


@pytest.fixture
def llm_service(llm_adapter):
    return LLMService(llm_adapter)


@pytest.fixture
def llm_transform(fake_storage, llm_service):
    return LLMTransform(fake_storage, llm_service)


# TODO: uncomment guard
# @pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip integration tests in CI")
class TestJudgeIntegration:
    """Integration tests using real LLM adapter with fake storage."""
    
    def test_judge_all_sustained(self, fake_storage, llm_service, llm_transform):
        """Test judging obviously substantive changes."""
        # Arrange - clear substantive change
        summary = SummaryV4(practically_substantive=Substantive(
                    rating=True,
                    reason="Service now requires government-issued ID for all users, mandatory arbitration added, and users lose right to sue."
                )
            )
        
        factcheck = dict(chunks=[
            FactCheck(claim="Government-issued ID is now required for account creation",
                      veracity=True,
                      reason="Document lists the types of compliant IDs").model_dump(),
            FactCheck(claim="Mandatory arbitration clause has been added to terms",
                      veracity=True,
                      reason="Document describes the terms of arbitration").model_dump(),
            FactCheck(claim="Users waive their right to file lawsuits against the company",
                      veracity=True,
                      reason="Section on right to sue has been removed").model_dump()
        ])
        
        summary_blob = "obviously_substantive_summary.json"
        facts_blob = "obviously_substantive_facts.json"
        
        fake_storage.upload_text_blob(
            summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            json.dumps(factcheck),
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = Judgement.model_validate_json(result_json)
        assert result.practically_substantive.rating

    def test_judge_obviously_nonsubstantive(self, fake_storage, llm_service, llm_transform):
        # Arrange
        summary = SummaryV4(practically_substantive=Substantive(
            rating=True,
            reason="Service now requires government-issued ID for all users, mandatory arbitration added, and users lose right to sue."
        ))

        factcheck = FactCheck(
            claim="Government-issued ID is now required for account creation",
            veracity=False,
            reason="Document does not make any mention of identification requirements.")
        
        summary_blob = "nonsubstantive_summary.json"
        facts_blob = "nonsubstantive_facts.json"
        
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
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = Judgement.model_validate_json(result_json)
        assert not result.practically_substantive.rating
    
    def test_judge_with_conflicting_evidence(self, fake_storage, llm_service, llm_transform):
        """Test judging when initial analysis and facts might conflict."""
        # Arrange - summary says substantive, but facts are weak
        summary = SummaryV4(practically_substantive=Substantive(
                    rating=True,
                    reason="Major privacy policy overhaul with significant data sharing changes."
                )
            )
        
        # But fact check shows claims are about minor things
        factcheck = dict(chunks=[
            FactCheck(claim="Privacy policy was reformatted",
                      veracity=True,
                      reason="Document sections are reordered").model_dump(),
            FactCheck(claim="Paragraphs were renumbered",
                      veracity=True,
                      reason="Before and after numbering is different with no major content changes").model_dump(),
            FactCheck(claim="Data sharing policy was significantly expanded.",
                      veracity=False,
                      reason="Data sharing terms are identical between versions").model_dump(),
        ])
        
        summary_blob = "conflicting_summary.json"
        facts_blob = "conflicting_facts.json"
        
        fake_storage.upload_text_blob(
            summary.model_dump_json(), 
            summary_blob, 
            metadata={"schema_version": SUMMARY_VERSION}
        )
        fake_storage.upload_text_blob(
            json.dumps(factcheck),
            facts_blob, 
            metadata={"schema_version": FACTCHECK_VERSION}
        )
        
        judge = Judge(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert - judge should reconcile the conflict
        result = json.loads(result_json)
        judgment = result["chunks"][0]["practically_substantive"]
        print(f"Judgment with conflicting evidence: {judgment}")
        # Just verify it completes and provides reasoning
        assert "rating" in judgment
        assert "reason" in judgment
        assert len(judgment["reason"]) > 10  # Should have substantive reasoning

