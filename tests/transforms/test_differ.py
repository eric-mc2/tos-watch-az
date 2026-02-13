import pytest
import json

from src.adapters.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.transforms.differ import Differ, DiffSection, DiffDoc
from src.stages import Stage
from schemas.docchunk.v1 import DocChunk


@pytest.fixture
def storage():
    """Create test storage with fake storage"""
    return BlobService(FakeStorageAdapter())

@pytest.fixture
def differ(storage):
    return Differ(storage)

@pytest.fixture
def sample_docchunks_v1():
    """Sample document chunks for version 1"""
    return [
        str(DocChunk("C","P","T",0,"Welcome to our service")),
        str(DocChunk("C","P","T",1,"Terms of service apply")),
        str(DocChunk("C","P","T",2,"Contact us for support")),
    ]


@pytest.fixture
def sample_docchunks_v2():
    """Sample document chunks for version 2 (modified)"""
    return [
        str(DocChunk("C","P","T",0,"Welcome to our platform")),
        str(DocChunk("C","P","T",1,"Terms of service apply")),
        str(DocChunk("C","P","T",2,"Contact us for help"))
    ]


@pytest.fixture
def doc_pair(storage, sample_docchunks_v1, sample_docchunks_v2):
    """Setup test documents in storage"""
    company = "testco"
    policy = "privacy"
    
    blob1 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-01-01.json"
    blob2 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-02-01.json"
    
    storage.upload_json_blob(json.dumps(sample_docchunks_v1), blob1)
    storage.upload_json_blob(json.dumps(sample_docchunks_v2), blob2)
    
    return blob1, blob2


class TestFindPeers:

    def test_find_diff_peers_finds_adjacent_files(self, differ, doc_pair):
        """Test that find_diff_peers correctly identifies adjacent document versions"""
        blob1, blob2 = doc_pair

        # Test finding peers for the second blob
        peers = list(differ.find_diff_peers(blob2))
        
        assert len(peers) == 1
        assert peers[0] == (blob1, blob2)


    def test_find_diff_peers_finds_both_neighbors(self, differ, storage, sample_docchunks_v1):
        """Test that middle document finds both before and after neighbors"""
        company = "testco"
        policy = "terms"
        
        blob1 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-01-01.json"
        blob2 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-02-01.json"
        blob3 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-03-01.json"
        
        storage.upload_json_blob(json.dumps(sample_docchunks_v1), blob1)
        storage.upload_json_blob(json.dumps(sample_docchunks_v1), blob2)
        storage.upload_json_blob(json.dumps(sample_docchunks_v1), blob3)
        
        peers = list(differ.find_diff_peers(blob2))
        
        assert len(peers) == 2
        assert (blob1, blob2) in peers
        assert (blob2, blob3) in peers

class TestComputeDiff:
    def test_compute_diff_returns_diff_strings(self, differ, doc_pair):
        """Test that compute_diff returns valid diff strings"""
        blob1, blob2 = doc_pair

        diff, span_diff = differ.compute_diff(blob1, blob2)
        
        # Verify both diffs are valid JSON
        diff_obj = json.loads(diff)
        span_diff_obj = json.loads(span_diff)
        
        assert 'fromfile' in diff_obj
        assert 'tofile' in diff_obj
        assert 'diffs' in diff_obj
        assert diff_obj['fromfile'] == blob1
        assert diff_obj['tofile'] == blob2
        
        assert 'diffs' in span_diff_obj


class TestDiffSave:
    def test_diff_and_save_creates_diff_files(self, differ, storage, doc_pair):
        """Test that diff_and_save creates diff files in storage"""
        blob1, blob2 = doc_pair

        differ.diff_and_save(blob2)
        
        # Check that diff files were created
        expected_diff = blob2.replace(Stage.DOCCHUNK.value, Stage.DIFF_RAW.value)
        expected_span = blob2.replace(Stage.DOCCHUNK.value, Stage.DIFF_SPAN.value)
        
        assert storage.adapter.exists_blob(expected_diff)
        assert storage.adapter.exists_blob(expected_span)

class TestHasDiff:
    def test_has_diff_detects_changes(self, differ, doc_pair):
        """Test that has_diff correctly identifies when documents differ"""
        blob1, blob2 = doc_pair

        diff, _ = differ.compute_diff(blob1, blob2)
        
        assert Differ.has_diff(diff) is True


    def test_has_diff_detects_no_changes(self, differ, storage, sample_docchunks_v1):
        """Test that has_diff returns False for identical documents"""
        company = "testco"
        policy = "privacy"
        
        blob1 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-01-01.json"
        blob2 = f"{Stage.DOCCHUNK.value}/{company}/{policy}/2024-01-02.json"
        
        storage.upload_json_blob(json.dumps(sample_docchunks_v1), blob1)
        storage.upload_json_blob(json.dumps(sample_docchunks_v1), blob2)
        
        diff, _ = differ.compute_diff(blob1, blob2)
        
        assert Differ.has_diff(diff) is False


    def test_has_diff(self, differ):
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


class TestCleanDiff:
    def test_clean_diff_filters_equal_sections(self, differ, doc_pair):
        """Test that clean_diff only returns non-equal diff sections"""
        blob1, blob2 = doc_pair

        diff, _ = differ.compute_diff(blob1, blob2)
        cleaned = Differ.clean_diff(diff)
        
        assert isinstance(cleaned, DiffDoc)
        assert len(cleaned.diffs) > 0
        
        # Verify all returned sections are actual changes
        for section in cleaned.diffs:
            assert isinstance(section, DiffSection)
            assert section.before != section.after or section.before == "" or section.after == ""


    def test_clean_diff_preserves_content(self, differ, doc_pair):
        """Test that clean_diff preserves the actual diff content"""
        blob1, blob2 = doc_pair

        diff, _ = differ.compute_diff(blob1, blob2)
        cleaned = Differ.clean_diff(diff)
        
        # Should have changes since we modified text
        assert len(cleaned.diffs) > 0
        
        # At least one section should contain our changed text
        all_before = ' '.join(s.before for s in cleaned.diffs)
        all_after = ' '.join(s.after for s in cleaned.diffs)
        
        assert 'service' in all_before or 'support' in all_before
        assert 'platform' in all_after or 'help' in all_after


    def test_prompt(self, differ):
        diff = {'diffs': [{'tag': 'equal', 'before': ['UNCHANGED'], 'after': ['UNCHANGED']},
                        {'tag': 'replace', 'before': ['OLD'], 'after': ['NEW']}]}
        prompt = differ.clean_diff(json.dumps(diff))
        assert all('UNCHANGED' not in x.before and 'UNCHANGED' not in x.after for x in prompt.diffs)
        assert any('OLD' in x.before and 'NEW' in x.after for x in prompt.diffs)
