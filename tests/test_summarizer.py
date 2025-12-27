import json
from src.claude_utils import sanitize_response
from src.differ import has_diff, clean_diff
from pathlib import Path

def test_is_diff():
    diff = {}
    assert not has_diff(json.dumps(diff))
    diff = {'diffs': []}
    assert not has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'equal'}]}
    assert not has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'replace'}]}
    assert has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'insert'}]}
    assert has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'delete'}]}
    assert has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'equal'}, {'tag': 'replace'}]}
    assert has_diff(json.dumps(diff))
    
def test_prompt():
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['OLD'], 'after': ['NEW']}]}
    prompt = clean_diff(json.dumps(diff))
    assert 'UNCHANGED' not in prompt and 'NEW' in prompt and 'OLD' in prompt


def test_sanitizer():
    assert "bad" == sanitize_response("bad")
    assert True == sanitize_response(True)
    assert 123 == sanitize_response(123)
    assert ["good", "stuff"] == sanitize_response(["good", "stuff"])
    assert {"good": "stuff"} == sanitize_response({"good":"stuff"})
    assert {"good": {"stuff": "here"}} == sanitize_response({"good":{"stuff":"here"}})
    assert {"good": ["stuff", "here"]} == sanitize_response({"good":["stuff","here"]})