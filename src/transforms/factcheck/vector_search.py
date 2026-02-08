import logging
from dataclasses import dataclass

from src.services.blob import BlobService
from src.transforms.differ import DiffDoc, DiffSection
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

@dataclass
class Indexer:
    storage: BlobService
    embedder: EmbeddingService
    _index = None # FAISS OBJECT
    _metadata = None # INDEX PROVINENCE


    def build(self, blob_name):
        diffs_txt = self.storage.load_text_blob(blob_name)
        diffs = DiffDoc.model_validate_json(diffs_txt)
        for diff in diffs.diffs:
            before_vector = self.embedder.embed(diff.before)
            after_vector = self.embedder.embed(diff.after)
            # Not sure if FAISS adds metadata like Elastic Search e.g. id, category
            # If it's purely index based, then for simplicity might just have two indexes.
            self._index.add(dict(id = diff.index, category = "before", text = diff.before, vector = before_vector))
            self._index.add(dict(id = diff.index, category = "after", text = diff.after, vector = after_vector))


    def search(self, query: str) -> DiffDoc:
        query_vec = self.embedder.embed(query)
        # Search KNN from index.
        ids = self._index.search(query_vec)
        # Return before and after text for each diff
        doc = DiffDoc(diffs = [DiffSection(
            index = x,
            before = self._metadata.get(x, "before"),
            after = self._metadata.get(x, "after")
        ) for x in ids])
        return doc

