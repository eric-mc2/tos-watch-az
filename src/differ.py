import logging
import difflib
from itertools import pairwise
import json
from src.blob_utils import (load_json_blob, upload_json_blob, list_blobs_nest, check_blob)
from src.log_utils import setup_logger
from src.docchunk import DocChunk
from src.stages import Stage

logger = setup_logger(__name__, logging.INFO)

def diff_batch() -> None:
    directory = list_blobs_nest()[Stage.DOCCHUNK.value]
    for company, policies in directory.items():
        for policy, snaps in policies.items():
            pairs = pairwise(sorted(snaps.keys()))
            manifest = _get_manifest(company, policy)
            for before, after in pairs:
                outname = f"{Stage.DIFF.value}/{company}/{policy}/{after}"
                if after in manifest and manifest[after] == before:
                    logger.debug(f"Diff already computed for {outname}")
                    continue
                logger.debug(f"Difffing {company}/{policy} : {before} <-> {after}")
                manifest[after] = before
                output = _diff_files(company, policy, before, after)
                upload_json_blob(output, outname)
                _store_manifest(manifest, company, policy)


def _get_manifest(company, policy):
    """Retrieve list of computed diffs (and reference points)."""
    manifest_name = f"{Stage.DIFF.value}/{company}/{policy}/manifest.json"
    if check_blob(manifest_name):
        return load_json_blob(manifest_name)
    else:
        return {}


def _store_manifest(data, company, policy):
    """Upload list of computed diffs (and reference points)."""
    manifest_name = f"{Stage.DIFF.value}/{company}/{policy}/manifest.json"
    manifest_str = json.dumps(data, indent=2)
    return upload_json_blob(manifest_str, manifest_name)


def _diff_files(company, policy, before, after) -> str:
    """Compute difference between two DocChunk files (parsed html lines)."""
    filenamea = f"{Stage.DOCCHUNK.value}/{company}/{policy}/{before}"
    filenameb = f"{Stage.DOCCHUNK.value}/{company}/{policy}/{after}"
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