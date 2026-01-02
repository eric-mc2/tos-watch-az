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
    doca = load_json_blob(blob_name_before)
    docb = load_json_blob(blob_name_after)
    txta = [DocChunk.from_str(x).text for x in doca]
    txtb = [DocChunk.from_str(x).text for x in docb]
    diff = _diff_byline(blob_name_before, blob_name_after, txta, txtb)
    _set_manifest(blob_name_before, blob_name_after)
    if diff:
        out_name = blob_name_after.replace(Stage.DOCCHUNK.value, Stage.DIFF_RAW.value)
        upload_json_blob(diff, out_name)
        span_diff = _diff_byspan(blob_name_before, blob_name_after, txta, txtb)
        out_name = blob_name_after.replace(Stage.DOCCHUNK.value, Stage.DIFF_SPAN.value)
        upload_json_blob(span_diff, out_name)
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


def _diff_byline(filenamea, filenameb, txta, txtb) -> str:
    """Compute difference between two DocChunk files (parsed html lines)."""
    output = dict(fromfile = filenamea,
                    tofile = filenameb,
                    diffs = _diff_sequence(txta, txtb))
    return json.dumps(output, indent=2)

def _diff_byspan(filenamea, filenameb, txta, txtb) -> str:
    """Compute difference between two DocChunk files (parsed html lines)."""
    output = dict(fromfile = filenamea,
                    tofile = filenameb,
                    diffs = _diff_spans(txta, txtb))
    return json.dumps(output, indent=2)
    

def _diff_sequence(a: list[str], b: list[str]) -> list[dict]: 
    """Helper function to diff line-based files."""
    diffs = []
    # A is a sequence of lines. B is a sequence of lines
    matcher = difflib.SequenceMatcher(lambda x: x.isspace(), a, b)
    # This aligns the two sequences as best as possible.
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        matcher = difflib.SequenceMatcher(lambda x: x.isspace(), a[i1:i2], b[j1:j2])
        diffs.append(dict(tag=tag, i1=i1, i2=i2, j1=j1, j2=j2,
                    before=a[i1:i2], after=b[j1:j2],
                   sim=matcher.ratio()))
    return diffs


def _diff_spans(chunks_a: list[str], chunks_b: list[str]) -> list[dict]: 
    """Helper function to diff line-based files."""
    # A is a sequence of semantic lines. B is a sequence of lines.
    diffs = []
    matcher = difflib.SequenceMatcher(lambda x: x.isspace(), chunks_a, chunks_b)
    for outer_idx, (outer_tag, i1, i2, j1, j2) in enumerate(matcher.get_opcodes()):
        # This aligns the two sequences as best as possible.
        alines, blines = chunks_a[i1:i2], chunks_b[j1:j2]
        # Some of the lines themselves have newlines in them. So let's normalize.
        astr, bstr = '\n'.join(alines), '\n'.join(blines)
        alines, blines = astr.splitlines(), bstr.splitlines()
        numlines = max(len(alines), len(blines))
        for inner_idx in range(numlines):
            line_a = alines[inner_idx] if inner_idx < len(alines) else ""
            line_b = blines[inner_idx] if inner_idx < len(blines) else ""
            # Now we diff a sentence. (Don't ignore whitespace so we can properly recombine word boundaries)
            matcher = difflib.SequenceMatcher(None, line_a, line_b)
            for inner_tag, ii1, ii2, jj1, jj2 in matcher.get_opcodes():
                diffs.append(dict(tag=inner_tag, idx=outer_idx, 
                           i1=i1, i2=i2, j1=j1, j2=j2,
                           ii1=ii1, ii2=ii2, jj1=jj1, jj2=jj2,
                           before=line_a[ii1:ii2], after=line_b[jj1:jj2]))
    return diffs
            

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