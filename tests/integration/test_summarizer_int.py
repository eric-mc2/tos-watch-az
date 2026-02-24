import pytest
import os

from schemas.brief.v2 import Memo, Brief
from schemas.summary.v4 import Summary
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.icl import ICLDataLoader
from src.transforms.llm_transform import LLMTransform
from src.transforms.summary.summarizer import Summarizer
from src.services.llm import LLMService
from src.adapters.llm.client import ClaudeAdapter
from src.services.blob import BlobService
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.utils.app_utils import load_env_vars

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "PROD")

@pytest.fixture
def llm():
    load_env_vars()
    llm_adapter = ClaudeAdapter()
    return LLMService(llm_adapter)
    
@pytest.fixture
def storage():
    adapter = FakeStorageAdapter()
    return BlobService(adapter)

@pytest.fixture
def transform(storage, llm):
    return LLMTransform(storage, llm)

@pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip for CI")
class TestSummarizerInt:

    def test_summary(self, transform, storage):
        # Arrange
        data = Brief(memos=[Memo(section_memo="""Policy changed from 'We will never collect or sell your data'
                                                to 'We will sell your interactions with our advertising partners.""",
                          running_memo="The company now monetizes on user behavior.")])
        storage.upload_json_blob(data.model_dump_json(), "test.json")

        # Act
        summarizer = Summarizer(storage, ICLDataLoader(storage), transform)
        txt, meta = summarizer.summarize("test.json")

        # Assert
        resp = Summary.model_validate_json(txt)
        assert resp.practically_substantive.rating
