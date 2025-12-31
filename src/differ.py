import logging
import difflib
from itertools import pairwise
import json
from pydantic import BaseModel
from os.path import basename
from src.blob_utils import (load_json_blob, upload_json_blob, list_blobs_nest, check_blob, list_blobs, parse_blob_path)
from src.log_utils import setup_logger
from src.docchunk import DocChunk
from src.stages import Stage


class DiffSection(BaseModel):
    index: int
    before: str
    after: str

class DiffDoc(BaseModel):
    diffs: list[DiffSection]


logger = setup_logger(__name__, logging.INFO)

def diff_single(blob_name_before, blob_name_after) -> str:
    diff = _diff_files(blob_name_before, blob_name_after)
    _set_manifest(blob_name_before, blob_name_after)
    if diff:
        out_name = blob_name_after.replace(Stage.DOCCHUNK.value, Stage.DIFF_RAW.value)
        upload_json_blob(diff, out_name)
    return diff


def _set_manifest(before, after):
    path = parse_blob_path(before)
    path2 = parse_blob_path(after)
    if path.company != path2.company or path.policy != path2.policy:
        raise ValueError("Can't diff unrelated docs.")
    manifest = _get_manifest(path.company, path.policy)
    manifest[basename(after)] = basename(before)
    _store_manifest(manifest, path.company, path.policy)
    

def _get_manifest(company, policy):
    """Retrieve list of computed diffs (and reference points)."""
    manifest_name = f"{Stage.DIFF_RAW.value}/{company}/{policy}/manifest.json"
    if check_blob(manifest_name):
        return load_json_blob(manifest_name)
    else:
        return {}


def _store_manifest(data, company, policy):
    """Upload list of computed diffs (and reference points)."""
    manifest_name = f"{Stage.DIFF_RAW.value}/{company}/{policy}/manifest.json"
    manifest_str = json.dumps(data, indent=2)
    return upload_json_blob(manifest_str, manifest_name)


def _diff_files(filenamea, filenameb) -> str:
    """Compute difference between two DocChunk files (parsed html lines)."""
    if not filenamea.startswith(Stage.DOCCHUNK.value) or not filenameb.startswith(Stage.DOCCHUNK.value):
        raise ValueError(f"Expected {Stage.DOCCHUNK.value} blob path")
    doca = load_json_blob(filenamea)
    docb = load_json_blob(filenameb)
    txta = [DocChunk.from_str(x).text for x in doca]
    txtb = [DocChunk.from_str(x).text for x in docb]
    diff = _diff_sequence(txta, txtb)
    output = dict(fromfile = filenamea,
                    tofile = filenameb,
                    diffs=list(diff))
    return json.dumps(output, indent=2)
    

def _diff_sequence(a, b): 
    """Helper function to diff line-based files."""
    matcher = difflib.SequenceMatcher(lambda x: x.isspace(), a, b)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        matcher = difflib.SequenceMatcher(lambda x: x.isspace(), a[i1:i2], b[j1:j2])
        yield dict(tag=tag, i1=i1, i2=i2, j1=j1, j2=j2,
                    before=a[i1:i2], after=b[j1:j2],
                   sim=matcher.ratio())
        

def has_diff(diff_str: str) -> bool:
    diff_obj = json.loads(diff_str)
    diffs = diff_obj.get('diffs', [])
    return any([d['tag'] != 'equal' for d in diffs])


def clean_diff(diff_str: str) -> DiffDoc:
    diff_obj = json.loads(diff_str)
    output = []
    for i, diff in enumerate(diff_obj['diffs']):
        if diff['tag'] == 'equal':
            continue
        before = ' '.join(diff['before'])
        after = ' '.join(diff['after'])
        output.append(DiffSection(index=i, before=before, after=after))
    return DiffDoc(diffs=output)