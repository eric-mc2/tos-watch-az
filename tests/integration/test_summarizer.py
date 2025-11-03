import os
import pytest
import json
from src.summarizer import summarize, create_prompt

@pytest.fixture()
def setup():
    with open('local.settings.json') as f:
        settings  = json.load(f)
        for key,val in settings['Values'].items():
            os.environ[key] = val
            
def test_summary(setup):
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['We are good!'], 'after': ['We are evil.']}]}
    prompt = create_prompt(json.dumps(diff))
    summary = summarize(prompt)
    print(summary)
