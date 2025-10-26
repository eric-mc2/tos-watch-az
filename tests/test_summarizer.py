import os
import pytest
import json
from src.blob_utils import load_json_blob
from src.summarizer import summarize, is_diff, create_prompt

@pytest.fixture()
def setup():
    with open('local.settings.json') as f:
        settings  = json.load(f)
        for key,val in settings['Values'].items():
            os.environ[key] = val
            
# def test_summary(setup):
#     # blob = load_json_blob('documents','diff/google/built-in-protection/20240227211728.json')
#     blob = load_json_blob('documents','diff/google/built-in-protection/20201125192228.json')
#     summary = summarize(json.dumps(blob))
#     print(summary)

def test_is_diff(setup):
    diff = {}
    assert not is_diff(json.dumps(diff))
    diff = {'diffs': []}
    assert not is_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'equal'}]}
    assert not is_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'replace'}]}
    assert is_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'insert'}]}
    assert is_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'delete'}]}
    assert is_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'equal'}, {'tag': 'replace'}]}
    assert is_diff(json.dumps(diff))
    
def test_prompt(setup):
    # blob = load_json_blob('documents','diff/google/built-in-protection/20240227211728.json')
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['OLD'], 'after': ['NEW']}]}
    prompt = create_prompt(json.dumps(diff))
    assert 'UNCHANGED' not in prompt and 'NEW' in prompt and 'OLD' in prompt