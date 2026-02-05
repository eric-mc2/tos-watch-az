import pytest
from src.adapters.llm.protocol import Message
from src.adapters.storage.client import AzureStorageAdapter
from dotenv import load_dotenv
from src.adapters.llm.client import ClaudeAdapter


@pytest.fixture(scope='module')
def llm():
    """Create a fresh storage adapter with a test container"""
    load_dotenv()
    adapter = ClaudeAdapter()

    yield adapter

    # Teardown
    adapter.close()


def test_hello(llm):
    """Test connection"""
    txt = llm.call("echo means repeat back to me verbatim", 
                   [Message("user", "echo 'hello world'")])
    assert txt == "hello world"

def test_empty_system_instruction(llm):
    """Test connection"""
    with pytest.raises(ValueError):
        llm.call("", [Message("user", "echo 'hello world'")])

