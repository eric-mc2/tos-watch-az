import logging
import difflib
from itertools import pairwise
import json
from src.blob_utils import get_blob_service_client, parse_blob_path, load_json_blob, upload_json_blob
from src.log_utils import setup_logger
from src.docchunk import DocChunk

logger = setup_logger(__name__, logging.INFO)

def diff_batch() -> None:
    directory = _list_container()
    for company, policies in directory.items():
        for policy, snaps in policies.items():
            pairs = pairwise(sorted(snaps))
            for pair in pairs:
                output = _diff_files(pair[0], pair[1])
                outpath = parse_blob_path(pair[0])
                outname = f"diff/{outpath.company}/{outpath.policy}/{outpath.timestamp}.json"
                upload_json_blob(output, 'documents', outname)


def diff_single(input_name: str):
    directory = _list_container()
    parts = parse_blob_path(input_name)
    policies = directory[parts.company]
    snaps = sorted(policies[parts.policy])
    idx = snaps.index(input_name)
    if idx >= 1:
        other = snaps[idx - 1]
        output = _diff_files(input_name, other)
        outname = f"diff/{parts.company}/{parts.policy}/{parts.timestamp}.json"
        upload_json_blob(output, 'documents', outname)


def _diff_files(filenamea: str, filenameb: str) -> str:
    doca = load_json_blob('documents', filenamea)
    docb = load_json_blob('documents', filenameb)
    txta = [DocChunk.from_str(x).text for x in doca]
    txtb = [DocChunk.from_str(x).text for x in docb]
    diff = _diff_sequence(txta, txtb)
    output = dict(fromfile = filenamea,
                    tofile = filenameb,
                    diffs=list(diff))
    return json.dumps(output, indent=2)
    

def _diff_sequence(a, b): 
    matcher = difflib.SequenceMatcher(lambda x: x.isspace(), a, b)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        matcher = difflib.SequenceMatcher(lambda x: x.isspace(), a[i1:i2], b[j1:j2])
        yield dict(tag=tag, i1=i1, i2=i2, j1=j1, j2=j2,
                    before=a[i1:i2], after=b[j1:j2],
                   sim=matcher.ratio())


def _list_container():
    client = get_blob_service_client()
    container = client.get_container_client('documents')
    directory = {}
    for name in container.list_blob_names(name_starts_with="annotated/"):
        parts = parse_blob_path(name)
        policies = directory.setdefault(parts.company, {})
        snaps = policies.setdefault(parts.policy, [])
        snaps.append(name)
    return directory