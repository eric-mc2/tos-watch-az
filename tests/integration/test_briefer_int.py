import os
import pytest
import json

from schemas.brief.v0 import BRIEF_MODULE
from schemas.brief.v2 import Memo, Brief, merge_memos
from src.adapters.storage.client import AzureStorageAdapter
from src.stages import Stage
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.summary.briefer import Briefer
from src.adapters.llm.client import ClaudeAdapter
from src.services.llm import LLMService
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.transforms.llm_transform import LLMTransform, create_llm_activity_processor, create_llm_parser
from src.utils.app_utils import load_env_vars

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "PROD")


@pytest.fixture
def fake_storage():
    """Use fake storage for faster tests and isolation."""
    adapter = FakeStorageAdapter()
    adapter.create_container()
    service = BlobService(adapter)
    return service

@pytest.fixture
def local_storage():
    """Use fake storage for faster tests and isolation."""
    adapter = AzureStorageAdapter()
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


@pytest.fixture
def llm_transform_local(local_storage, llm_service):
    return LLMTransform(local_storage, llm_service)


# @pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip integration tests in CI")
class TestBrieferIntegration:
    """Integration tests using real LLM adapter with fake storage."""
    
    def test_real_doc(self, local_storage, llm_transform_local):
        blob_name = "05-diffs-clean/linkedin/professional-community-policies/20221215173159.json"
        briefer = Briefer(
            storage=local_storage,
            executor=llm_transform_local
        )

        if not local_storage.check_blob(blob_name):
            return
        
        # Act
        result_json, metadata = briefer.brief(blob_name)
        processor = create_llm_activity_processor(local_storage,
                                              briefer.brief,
                                              Stage.BRIEF_RAW.value,
                                              "briefer")
        
        # Assert -- should not throw error
        Memo.model_validate_json(result_json)
        
        # Assert - should not throw error
        processor(dict(task_id=blob_name))

    @staticmethod
    def real_exceed_limit():
        return ["05-diffs-clean/meta/printable/20240813081129.json",
                "05-diffs-clean/x-ai/privacy-policy/20240801214117.json"]
        
    @pytest.mark.parametrize("blob_name", real_exceed_limit())
    def test_real_exceeds_limit(self, local_storage, llm_transform_local, blob_name):
        briefer = Briefer(
            storage=local_storage,
            executor=llm_transform_local
        )

        if not local_storage.check_blob(blob_name):
            return
        
        processor = create_llm_activity_processor(local_storage,
                                              briefer.brief,
                                              Stage.BRIEF_RAW.value,
                                              "briefer")
        
        # Act
        processor(dict(task_id=blob_name))
        


    def test_substantive_privacy_change(self, fake_storage, llm_transform):
        """Test briefing obviously substantive privacy policy change."""
        # Arrange - clear substantive privacy change
        diff = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="We collect your name and email address to provide the service.",
                after="We collect your name, email address, location data, biometric information, and browsing history to provide personalized advertisements."
            )
        ])
        
        blob_name = "privacy_change.json"
        fake_storage.upload_text_blob(
            diff.model_dump_json(), 
            blob_name
        )
        
        briefer = Briefer(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = briefer.brief(blob_name)
        
        # Assert
        parsed = json.loads(result_json)
        result = Memo.model_validate(parsed)
        
        assert len(result.running_memo) > 10
        assert any(keyword in result.running_memo.lower() for keyword in ["data", "collect", "biometric", "location", "privacy"])

    def test_irrelevant_formatting_change(self, fake_storage, llm_transform):
        """Test briefing non-substantive formatting change."""
        # Arrange - only formatting changes
        diff = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="1. Overview\nThis is our policy.",
                after="1.0 Overview\n\nThis is our policy."
            )
        ])
        
        blob_name = "formatting_change.json"
        fake_storage.upload_text_blob(
            diff.model_dump_json(), 
            blob_name
        )
        
        briefer = Briefer(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = briefer.brief(blob_name)
        
        # Assert
        parsed = json.loads(result_json)
        result = Memo.model_validate(parsed)
        

    def test_mixed_relevant_irrelevant(self, fake_storage, llm_transform):
        """Test briefing document with both relevant and irrelevant changes."""
        # Arrange - mix of substantive and non-substantive changes
        diff = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Contact us at: support@example.com, 333 Underhill Road",
                after="Contact us at: support@example.com, 123 Main Street, Suite 100"
            ),
            DiffSection(
                index=1,
                before="You retain all rights to your content.",
                after="By posting content, you grant us a perpetual, irrevocable, worldwide license to use, modify, and distribute your content for any purpose."
            )
        ])
        
        blob_name = "mixed_change.json"
        fake_storage.upload_text_blob(
            diff.model_dump_json(), 
            blob_name
        )
        
        briefer = Briefer(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = briefer.brief(blob_name)
        
        # Assert
        parsed = json.loads(result_json)
        result = Memo.model_validate(parsed)
        
        # Should flag as relevant due to the license change
        assert any(keyword in result.running_memo.lower() for keyword in ["license", "content", "rights"])

    def test_chunked_multi_page_document(self, fake_storage, llm_transform, monkeypatch):
        """Test briefing with many pages that forces chunking (low TOKEN_LIMIT)."""
        # Arrange - Set TOKEN_LIMIT low to force chunking
        import src.transforms.summary.briefer as briefer_module
        monkeypatch.setattr(briefer_module, "TOKEN_LIMIT", 800)
        
        # Create a large document with multiple substantive changes
        # Each section is designed to be reasonably sized but collectively force chunking
        diff = DiffDoc(diffs=[
            DiffSection(
                index=0,
                before="Section 1: We collect minimal data including your name and email to provide basic service functionality.",
                after="Section 1: We now collect extensive data including your name, email, phone number, physical address, browsing history, device identifiers, IP addresses, location data, and behavioral patterns to enhance our advertising and analytics capabilities."
            ),
            DiffSection(
                index=1,
                before="Section 2: Your subscription is $5.99 per month. You can cancel anytime with no penalty and receive a prorated refund.",
                after="Section 2: Your subscription is now $29.99 per month with an annual commitment. Early cancellation incurs a $200 termination fee and no refunds will be provided under any circumstances."
            ),
            DiffSection(
                index=2,
                before="Section 3: Users maintain full ownership of all content they create and post on our platform. We do not claim any rights to user content.",
                after="Section 3: By posting any content, users grant us perpetual, irrevocable, transferable, worldwide rights to use, reproduce, modify, adapt, publish, translate, distribute, and display such content in any media format for any commercial or non-commercial purpose without compensation."
            ),
            DiffSection(
                index=3,
                before="Section 4: Account suspensions require prior notice and users have the right to appeal any moderation decisions. Permanent bans require multiple violations.",
                after="Section 4: We reserve the right to suspend or permanently terminate accounts immediately without prior notice, warning, or explanation at our sole discretion. Users have no right to appeal and all decisions are final."
            ),
            DiffSection(
                index=4,
                before="Section 5: Disputes may be resolved through negotiation, mediation, or court proceedings in accordance with applicable law. Users retain all legal rights.",
                after="Section 5: All disputes must be resolved exclusively through binding individual arbitration. Users waive all rights to jury trials, class actions, or any form of joint legal action. The arbitrator's decision is final and not subject to appeal."
            )
        ])
        
        blob_name = "large_multi_page.json"
        fake_storage.upload_text_blob(
            diff.model_dump_json(), 
            blob_name
        )
        
        briefer = Briefer(
            storage=fake_storage,
            executor=llm_transform
        )
        
        # Act
        result_json, metadata = briefer.brief(blob_name)
        parser = create_llm_parser(llm_transform.llm, BRIEF_MODULE, merge_memos)
        result_json, metadata = parser(result_json, metadata)
        
        # Assert
        # Validate final memo
        result = Brief.model_validate_json(result_json)
        # Should make a couple chunks
        assert len(result.memos) > 2
        # Running memo should accumulate information across chunks
        final_memo = result.memos[-1]
        assert len(final_memo.running_memo) > 50
        
        # Verify multiple substantive topics are captured
        full_text = result_json.lower()
        # Should mention at least some of the major changes
        topics_found = sum([
            "data" in full_text or "collect" in full_text or "privacy" in full_text,
            "subscription" in full_text or "fee" in full_text or "refund" in full_text,
            "content" in full_text or "license" in full_text or "rights" in full_text,
            "account" in full_text or "suspension" in full_text or "ban" in full_text,
            "arbitration" in full_text or "dispute" in full_text or "waive" in full_text
        ])
        assert topics_found >= 3, "Should capture at least 3 of the 5 major topics"

    