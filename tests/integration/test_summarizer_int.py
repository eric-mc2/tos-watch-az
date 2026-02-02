import pytest
import json
from src.summarizer import SYSTEM_PROMPT
from src.services.differ import DiffDoc
from src.claude_utils import call_api, Message
from src.clients.storage.blob_utils import load_text_blob, set_connection_key
from dotenv import load_dotenv

@pytest.fixture()
def setup():
    load_dotenv()
    set_connection_key()
            
def test_summary(setup):
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['We are good!'], 'after': ['We are evil.']}]}
    prompt = [Message("user", json.dumps(diff))]
    txt = call_api(SYSTEM_PROMPT, prompt)
    print(txt)

def test_chunks(setup):
    diff = load_text_blob("05-diffs-clean/x-ai/terms-of-service/20250614010956.json")
    diff = DiffDoc.model_validate_json(diff)
    # chunks = _chunk_prompt(diff)
    # print(len(chunks))
    for d in diff.diffs:
        print(len(d.before.splitlines()))
        print(len(d.after.splitlines()))
        # prompt = chunk.model_dump_json()
        # print(len(prompt))
        # assert len(prompt) < TOKEN_LIMIT