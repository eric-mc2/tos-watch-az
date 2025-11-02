import os
import pytest
import json
from src.differ import diff_single
from src.stages import Stage

@pytest.fixture()
def setup():
    with open('local.settings.json') as f:
        settings  = json.load(f)
        os.environ['AZURE_STORAGE_CONNECTION_STRING'] = settings['Values']['AZURE_STORAGE_CONNECTION_STRING']

def test_single(setup):
    diff_single(f'{Stage.DOCCHUNK.value}/google/built-in-protection/20240227211728.json')