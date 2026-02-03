import pytest

from schemas.summary.v3 import Summary
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.summarizer import Summarizer
from src.services.llm import LLMService
from src.adapters.llm.client import ClaudeAdapter
from src.services.blob import BlobService
from src.adapters.storage.fake_client import FakeStorageAdapter

@pytest.fixture
def llm():
    llm_adapter = ClaudeAdapter()
    return LLMService(llm_adapter)
    
@pytest.fixture
def storage():
    adapter = FakeStorageAdapter('test-container')
    return BlobService(adapter)

def test_summary(llm, storage):
    # Arrange
    diff = DiffDoc(diffs=[
        DiffSection(index=0, 
                    before="Our policy is to do good.", 
                    after="Our policy is to do evil.")
    ])
    storage.upload_json_blob(diff.model_dump_json(), "test.json")

    # Act
    summarizer = Summarizer(storage=storage, llm=llm)
    txt, meta = summarizer.summarize("test.json")

    # Assert
    resp = Summary.model_validate_json(txt)
    assert resp.chunks[0].practically_substantive
