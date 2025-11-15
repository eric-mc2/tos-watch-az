import os
import pytest
import json
from src.stages import Stage
from src.differ import diff_batch

@pytest.fixture()
def setup():
    with open('local.settings.json') as f:
        settings  = json.load(f)
        os.environ['AzureWebJobsStorage'] = settings['Values']['AzureWebJobsStorage']

def test_single(setup):
    diff_batch()