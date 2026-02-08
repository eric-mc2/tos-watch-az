import pytest
import os

from schemas.summary.v3 import Summary
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.prompt_eng import PromptEng
from src.transforms.summarizer import Summarizer
from src.services.llm import LLMService
from src.adapters.llm.client import ClaudeAdapter
from src.services.blob import BlobService
from src.adapters.storage.fake_client import FakeStorageAdapter

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "PROD")

@pytest.fixture
def llm():
    llm_adapter = ClaudeAdapter()
    return LLMService(llm_adapter)
    
@pytest.fixture
def storage():
    adapter = FakeStorageAdapter()
    return BlobService(adapter)

class TestSummarizerInt:

    @pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip for CI")
    def test_summary(self, llm, storage):
        # Arrange
        diff = DiffDoc(diffs=[
            DiffSection(index=0,
                        before="Our policy is to do good.",
                        after="Our policy is to do evil.")
        ])
        storage.upload_json_blob(diff.model_dump_json(), "test.json")

        # Act
        summarizer = Summarizer(storage=storage, llm=llm, prompt_eng=PromptEng(storage))
        txt, meta = summarizer.summarize("test.json")

        # Assert
        resp = Summary.model_validate_json(txt)
        assert resp.chunks[0].practically_substantive
