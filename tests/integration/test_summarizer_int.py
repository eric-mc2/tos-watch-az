import os
import pytest
import json
from src.summarizer import summarize, create_prompt
from pathlib import Path
from dotenv import load_dotenv

@pytest.fixture()
def setup():
    load_dotenv()
            
def test_summary(setup):
    diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']}, 
                      {'tag': 'replace', 'before': ['We are good!'], 'after': ['We are evil.']}]}
    prompt = create_prompt(json.dumps(diff))
    summary = summarize(prompt)
    print(summary)
