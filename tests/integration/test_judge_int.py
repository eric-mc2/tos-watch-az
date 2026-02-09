import os
import pytest
import json

from schemas.summary.v3 import Summary as SummaryV3, VERSION as SUMMARY_VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive
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


@pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip integration tests in CI")
class TestJudgeIntegration:
    """Integration tests using real LLM adapter with fake storage."""
    
    def test_judge_obviously_substantive(self, fake_storage, llm_service, llm_transform):
        """Test judging obviously substantive changes."""
        # Arrange - clear substantive change
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="Service now requires government-issued ID for all users, mandatory arbitration added, and users lose right to sue."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "Government-issued ID is now required for account creation",
            "Mandatory arbitration clause has been added to terms",
            "Users waive their right to file lawsuits against the company"
        ])
        
        summary_blob = "obviously_substantive_summary.json"
        facts_blob = "obviously_substantive_facts.json"
        
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
            llm=llm_service,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = json.loads(result_json)
        assert "chunks" in result
        assert len(result["chunks"]) == 1
        
        judgment = result["chunks"][0]["practically_substantive"]
        print(f"Judgment for substantive change: {judgment}")
        assert "rating" in judgment
        assert "reason" in judgment
        # Smoke test: should recognize this as substantive
        # (not deeply testing LLM reasoning, just that it functions)
    
    def test_judge_obviously_nonsubstantive(self, fake_storage, llm_service, llm_transform):
        """Test judging obviously non-substantive changes."""
        # Arrange - clear non-substantive change
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=False,
                    reason="Contact email address was updated from support@old.com to support@new.com."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "Support email changed from support@old.com to support@new.com"
        ])
        
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
            llm=llm_service,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = json.loads(result_json)
        judgment = result["chunks"][0]["practically_substantive"]
        print(f"Judgment for non-substantive change: {judgment}")
        assert "rating" in judgment
        assert "reason" in judgment
    
    def test_judge_with_conflicting_evidence(self, fake_storage, llm_service, llm_transform):
        """Test judging when initial analysis and facts might conflict."""
        # Arrange - summary says substantive, but facts are weak
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="Major privacy policy overhaul with significant data sharing changes."
                )
            )
        ])
        
        # But fact check shows claims are about minor things
        factcheck = FactCheck(claims=[
            "Privacy policy was reformatted",
            "Section headings were made bold",
            "Paragraphs were renumbered"
        ])
        
        summary_blob = "conflicting_summary.json"
        facts_blob = "conflicting_facts.json"
        
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
            llm=llm_service,
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
    
    def test_judge_with_mixed_chunks(self, fake_storage, llm_service, llm_transform):
        """Test judging summary with multiple chunks (some substantive, some not)."""
        # Arrange
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="Subscription price increased from $5 to $15 per month."
                )
            ),
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="New clause limits refunds to 7 days instead of 30 days."
                )
            ),
            SummaryV2(
                practically_substantive=Substantive(
                    rating=False,
                    reason="Company logo was refreshed with new colors."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "Monthly subscription cost changed from $5 to $15",
            "Refund window reduced from 30 days to 7 days",
            "Logo design was updated"
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
            llm=llm_service,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert
        result = json.loads(result_json)
        judgment = result["chunks"][0]["practically_substantive"]
        print(f"Judgment with mixed chunks: {judgment}")
        # Judge should weigh substantive vs non-substantivchanges
        assert "rating" in judgment
        assert "reason" in judgment
    
    def test_judge_metadata_preservation(self, fake_storage, llm_service, llm_transform):
        """Test that metadata is properly preserved through judging."""
        # Arrange
        summary = SummaryV3(chunks=[
            SummaryV2(
                practically_substantive=Substantive(
                    rating=True,
                    reason="Terms add data retention period of 10 years."
                )
            )
        ])
        
        factcheck = FactCheck(claims=[
            "User data will be retained for 10 years"
        ])
        
        summary_blob = "metadata_summary.json"
        facts_blob = "metadata_facts.json"
        
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
            llm=llm_service,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = judge.judge(facts_blob, summary_blob)
        
        # Assert metadata
        assert "schema_version" in metadata
        assert "run_id" in metadata
        print(f"Metadata preserved: {metadata}")
        
        # Assert result structure
        result = json.loads(result_json)
        assert "chunks" in result
