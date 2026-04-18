"""
Pytest configuration for test suite.

Sets up environment variables and test fixtures.
"""
import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    # Fix OpenMP conflict between PyTorch and FAISS on macOS
    os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
