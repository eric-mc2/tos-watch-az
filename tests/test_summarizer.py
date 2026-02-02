import json
from src.claude_utils import sanitize_response
from src.services.differ import DiffService

def test_is_diff():
    differ = DiffService()
    diff = {}
    assert not differ.has_diff(json.dumps(diff))
    diff = {'diffs': []}
    assert not differ.has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'equal'}]}
    assert not differ.has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'replace'}]}
    assert differ.has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'insert'}]}
    assert differ.has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'delete'}]}
    assert differ.has_diff(json.dumps(diff))
    diff = {'diffs': [{'tag': 'equal'}, {'tag': 'replace'}]}
    assert differ.has_diff(json.dumps(diff))
    
def test_prompt():
    differ = DiffService()
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['OLD'], 'after': ['NEW']}]}
    prompt = differ.clean_diff(json.dumps(diff))
    assert all('UNCHANGED' not in x.before and 'UNCHANGED' not in x.after for x in prompt.diffs)
    assert any('OLD' in x.before and 'NEW' in x.after for x in prompt.diffs)


def test_sanitizer():
    assert "bad" == sanitize_response("bad")
    assert True == sanitize_response(True)
    assert 123 == sanitize_response(123)
    assert ["good", "stuff"] == sanitize_response(["good", "stuff"])
    assert {"good": "stuff"} == sanitize_response({"good":"stuff"})
    assert {"good": {"stuff": "here"}} == sanitize_response({"good":{"stuff":"here"}})
    assert {"good": ["stuff", "here"]} == sanitize_response({"good":["stuff","here"]})