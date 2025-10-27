import os
import pytest
import json
from src.summarizer import summarize, is_diff, create_prompt, parse_response_json

@pytest.fixture()
def setup():
    with open('local.settings.json') as f:
        settings  = json.load(f)
        for key,val in settings['Values'].items():
            os.environ[key] = val
            
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

def test_summary(setup):
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['We are good!'], 'after': ['We are evil.']}]}
    prompt = create_prompt(json.dumps(diff))
    summary = summarize(prompt)
    print(summary)

def test_parse(setup):
    with open('data/20240421054440.txt') as f:
        data = json.load(f)
    with open('data/20240421054440.txt') as f:
        data_str = f.read()
    resp = parse_response_json(data_str)
    for key in data:
        assert key in resp
    for key in resp:
        assert key in data
        assert data[key] == resp[key] 