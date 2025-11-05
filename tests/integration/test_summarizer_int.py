import os
import pytest
import json
from src.summarizer import summarize, create_prompt
from pathlib import Path

@pytest.fixture()
def setup():
    root = Path(__file__).parent.parent.parent.absolute()
    with open(f'{root}/local.settings.json') as f:
        settings  = json.load(f)
        for key,val in settings['Values'].items():
            os.environ[key] = val
            
def test_summary(setup):
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['We are good!'], 'after': ['We are evil.']}]}
    prompt = create_prompt(json.dumps(diff))
    summary = summarize(prompt)
    print(summary)
