import logging
import difflib
import json
from dataclasses import dataclass
from pydantic import BaseModel
from os.path import basename
from typing import Iterator

from src.utils.log_utils import setup_logger
from schemas.docchunk.v1 import DocChunk
from src.services.blob import BlobService
from src.stages import Stage


class DiffSection(BaseModel):
    index: int
    before: str
    after: str

class DiffDoc(BaseModel):
    diffs: list[DiffSection]


logger = setup_logger(__name__, logging.INFO)

@dataclass
class Differ:
    storage: BlobService

    def diff_and_save(self, blob_name: str):
        refs = self.find_diff_peers(blob_name)
        for before, after in refs:
            diff, span_diff = self.compute_diff(before, after)
            if diff:
                self.save_diff(after, diff, span_diff)

    def find_diff_peers(self, blob_name: str) -> Iterator[tuple[str, str]]:
        blob_name = blob_name.removeprefix(f"{self.storage.container}/")
        path = self.storage.parse_blob_path(blob_name)
        # nb: Trailing slash prevents same-prefixed policy collision like policy-safety vs policy-privacy
        peers = sorted([x for x in self.storage.adapter.list_blobs() if
                        x.startswith(f"{Stage.DOCCHUNK.value}/{path.company}/{path.policy}/")])
        idx = peers.index(blob_name)  # throws if missing
        if idx >= 1:
            yield peers[idx - 1], blob_name
        if idx + 1 < len(peers):
            yield blob_name, peers[idx + 1]

    def compute_diff(self, blob_name_before, blob_name_after) -> tuple[str,str]:
        doca = self.storage.load_json_blob(blob_name_before)
        docb = self.storage.load_json_blob(blob_name_after)
        txta = [DocChunk.from_str(x).text for x in doca]
        txtb = [DocChunk.from_str(x).text for x in docb]
        diff = self._diff_byline(blob_name_before, blob_name_after, txta, txtb)
        span_diff = self._diff_byspan(blob_name_before, blob_name_after, txta, txtb)
        self._set_manifest(blob_name_before, blob_name_after)
        return diff, span_diff

    def save_diff(self, blob_name_after, diff, span_diff):
        out_name = blob_name_after.replace(Stage.DOCCHUNK.value, Stage.DIFF_RAW.value)
        self.storage.upload_json_blob(diff, out_name)
        out_name = blob_name_after.replace(Stage.DOCCHUNK.value, Stage.DIFF_SPAN.value)
        self.storage.upload_json_blob(span_diff, out_name)

    def _set_manifest(self, before, after):
        path = self.storage.parse_blob_path(before)
        path2 = self.storage.parse_blob_path(after)
        if path.company != path2.company or path.policy != path2.policy:
            raise ValueError("Can't diff unrelated docs.")
        manifest = self._get_manifest(path.company, path.policy)
        manifest[basename(after)] = basename(before)
        self._store_manifest(manifest, path.company, path.policy)


    def _get_manifest(self, company, policy):
        """Retrieve list of computed diffs (and reference points)."""
        manifest_name = f"{Stage.DIFF_RAW.value}/{company}/{policy}/manifest.json"
        if self.storage.adapter.exists_blob(manifest_name):
            return self.storage.load_json_blob(manifest_name)
        else:
            return {}


    def _store_manifest(self, data, company, policy):
        """Upload list of computed diffs (and reference points)."""
        manifest_name = f"{Stage.DIFF_RAW.value}/{company}/{policy}/manifest.json"
        manifest_str = json.dumps(data, indent=2)
        return self.storage.upload_json_blob(manifest_str, manifest_name)


    def _diff_byline(self, filenamea, filenameb, txta, txtb) -> str:
        """Compute difference between two DocChunk files (parsed html lines)."""
        output = dict(fromfile = filenamea,
                        tofile = filenameb,
                        diffs = self._diff_sequence(txta, txtb))
        return json.dumps(output, indent=2)

    def _diff_byspan(self, filenamea, filenameb, txta, txtb) -> str:
        """Compute difference between two DocChunk files (parsed html lines)."""
        output = dict(fromfile = filenamea,
                        tofile = filenameb,
                        diffs = self._diff_spans(txta, txtb))
        return json.dumps(output, indent=2)
    

    @staticmethod
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


    @staticmethod
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
            

    @staticmethod
    def has_diff(diff_str: str) -> bool:
        diff_obj = json.loads(diff_str)
        diffs = diff_obj.get('diffs', [])
        return any([d['tag'] != 'equal' for d in diffs])


    @staticmethod
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