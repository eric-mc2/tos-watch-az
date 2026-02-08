import pytest
import os
from src.adapters.llm.protocol import Message
from src.adapters.llm.client import ClaudeAdapter
from src.utils.app_utils import load_env_vars

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "PROD")

@pytest.fixture(scope='module')
def llm():
    """Create a fresh storage adapter with a test container"""
    load_env_vars()
    adapter = ClaudeAdapter()

    yield adapter

    # Teardown
    adapter.close()

@pytest.mark.skipif(RUNTIME_ENV != "DEV", reason="Skip for CI")
class TestLLMIntegration:

    def test_hello(self, llm):
        """Test connection"""
        txt = llm.call("echo means repeat back to me verbatim",
                       [Message("user", "echo 'hello world'")])
        assert txt == "hello world"

    def test_empty_system_instruction(self, llm):
        """Test connection"""
        with pytest.raises(ValueError):
            llm.call("", [Message("user", "echo 'hello world'")])

