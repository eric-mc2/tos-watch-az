import os
import pytest
import json
from src.stages import Stage
from src.differ import diff_batch

@pytest.fixture()
def setup():
    with open('local.settings.json') as f:
        settings  = json.load(f)
        os.environ['AZURE_STORAGE_CONNECTION_STRING'] = settings['Values']['AZURE_STORAGE_CONNECTION_STRING']

def test_single(setup):
    diff_batch()