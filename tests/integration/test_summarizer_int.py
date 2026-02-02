import pytest
import json

from src.container import ServiceContainer
from src.services.prompt_builder import SYSTEM_PROMPT
from src.clients.llm.protocol import Message
from schemas.summary.v3 import Summary

@pytest.fixture
def llm():
    container = ServiceContainer.create_production()
    return container.llm

@pytest.fixture
def storage():
    container = ServiceContainer.create_production()
    return container.llm

def test_summary(llm):
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['We are good!'], 'after': ['We are evil.']}]}
    prompt = [Message("user", json.dumps(diff))]
    txt = llm.call_unsafe(SYSTEM_PROMPT, prompt, Summary)
    print(txt)