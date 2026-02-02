import pytest
from src.clients.storage.client import AzureStorageAdapter
from dotenv import load_dotenv
from src.clients.llm.client import ClaudeAdapter


@pytest.fixture
def llm():
    """Create a fresh storage adapter with a test container"""
    load_dotenv()
    adapter = ClaudeAdapter()

    yield adapter

    # Teardown
    adapter.close()

    yield None


def test_hello(llm):
    """Test connection"""
    txt = llm.call("echo: hello world")
    assert txt == "hello world"

