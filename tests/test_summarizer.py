import json
from src.summarizer import is_diff, create_prompt, parse_response_json, sanitize_response

def test_is_diff():
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
    
def test_prompt():
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['OLD'], 'after': ['NEW']}]}
    prompt = create_prompt(json.dumps(diff))
    assert 'UNCHANGED' not in prompt and 'NEW' in prompt and 'OLD' in prompt

def test_parse():
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

def test_sanitizer():
    assert "bad" == sanitize_response("bad")
    assert True == sanitize_response(True)
    assert 123 == sanitize_response(123)
    assert ["good", "stuff"] == sanitize_response(["good", "stuff"])
    assert {"good": "stuff"} == sanitize_response({"good":"stuff"})
    assert {"good": {"stuff": "here"}} == sanitize_response({"good":{"stuff":"here"}})
    assert {"good": ["stuff", "here"]} == sanitize_response({"good":["stuff","here"]})