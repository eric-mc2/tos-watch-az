import os
import logging
import json
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from src.utils.log_utils import setup_logger
from src.adapters.storage.protocol import BlobStorageProtocol

logger = setup_logger(__name__, logging.INFO)

class BlobService:
    adapter: BlobStorageProtocol
    container: str

    def __init__(self, adapter: BlobStorageProtocol):
        self.adapter = adapter
        self.container = adapter.container
        if not self.adapter.exists_container():
            self.adapter.create_container()

    # Domain Specific Parsing
    def parse_blob_path(self, path: str):
        path = path.removeprefix(f"{self.container}/")
        blob_path = Path(path)
        if len(blob_path.parts) == 4:
            BlobPath = namedtuple("BlobPath", ['stage', 'company', 'policy', 'timestamp'])
            return BlobPath(
                blob_path.parts[0],
                blob_path.parts[1],
                blob_path.parts[2],
                blob_path.stem)
        elif len(blob_path.parts) == 5:
            RunBlobPath = namedtuple("RunBlobPath", ['stage', 'company', 'policy', 'timestamp', 'run_id'])
            return RunBlobPath(
                blob_path.parts[0],
                blob_path.parts[1],
                blob_path.parts[2],
                blob_path.parts[3],
                blob_path.stem)
        else:
            raise ValueError(f"Invalid path {path}")


    # Domain Specific Queries
    def list_blobs_nest(self) -> dict:
        """Represent container as dictionary."""
        directory = {}
        for name in self.adapter.list_blobs():
            namepath = Path(name)
            subdir = directory
            for i, part in enumerate(namepath.parts):
                if i == len(namepath.parts) - 1:
                    leaf = subdir.setdefault(part, None)
                else:
                    subdir = subdir.setdefault(part, {})
        return directory


    # Domain Specific Operations
    def check_blob(self, blob_name: str, touch: bool=False) -> bool:
        blob_name = blob_name.removeprefix(f"{self.container}/")
        exists = self.adapter.exists_blob(blob_name)
        if exists and touch:
            metadata = self.adapter.load_metadata(blob_name)
            metadata["touched"] = datetime.now(timezone.utc).isoformat()
            self.adapter.upload_metadata(metadata, blob_name)
        return exists


    def touch_blobs(self, stage, company=None, policy=None, timestamp=None, run=None) -> None:
        blobs = self.list_blobs_nest()
        for c, policies in blobs[stage].items():
            if company and company != c:
                continue
            for p, timestamps in policies.items():
                if policy and policy != p:
                    continue
                for t, runs in timestamps.items():
                    if timestamp and not t.startswith(timestamp):
                        continue
                    if not runs:
                        path = os.path.join(stage, c, p, t)
                        self.check_blob(path, touch=True)
                        continue
                    for r in runs:
                        if run and run != r:
                            continue
                        path = os.path.join(stage, c, p, t, r)
                        self.check_blob(path, touch=True)


    def ensure_container(self) -> None:
        if self.adapter.exists_container():
            logger.debug(f"Output container {self.container} already exists.")
        else:
            self.adapter.create_container()
            logger.info(f"Created output container: {self.container}")


    # Convenience Methods
    def load_json_blob(self, name: str) -> dict:
        name = name.removeprefix(f"{self.container}/")
        logger.debug(f"Downloading blob: {self.container}/{name}")
        data = self.adapter.load_blob(name)
        try:
            json_data = json.loads(data.decode('utf-8'))
            return json_data
        except Exception as e:
            logger.error(f"Invalid json blob {name}:\n{e}")
            raise


    def load_text_blob(self, name: str) -> str:
        name = name.removeprefix(f"{self.container}/")
        logger.debug(f"Downloading blob: {self.container}/{name}")
        data = self.adapter.load_blob(name)
        try:
            txt = data.decode('utf-8')
            return txt
        except Exception as e:
            logger.error(f"Error decoding text blob {name}:\n{e}")
            raise


    def upload_blob(self, data: Any, blob_name: str, content_type: str, metadata: Optional[dict]) -> None:
        logger.debug(f"Uploading blob to {self.container}/{blob_name}")
        self.adapter.upload_blob(data, blob_name, content_type, metadata)


    def upload_text_blob(self, data: str, blob_name: str, metadata: Optional[dict]) -> None:
        data_bytes = data.encode('utf-8')
        content_type = 'text/plain; charset=utf-8'
        self.upload_blob(data_bytes, blob_name, content_type, metadata)


    def upload_json_blob(self, data: str, blob_name: str, metadata: Optional[dict]) -> None:
        data_bytes = data.encode('utf-8')
        content_type = 'application/json; charset=utf-8'
        self.upload_blob(data_bytes, blob_name, content_type, metadata)


    def upload_html_blob(self, cleaned_html: str, blob_name: str, metadata: Optional[dict]) -> None:
        html_bytes = cleaned_html.encode('utf-8')
        content_type = 'text/html; charset=utf-8'
        self.upload_blob(html_bytes, blob_name, content_type, metadata)

    def upload_metadata(self, data: dict, blob_name: str) -> None:
        if not self.adapter.exists_blob(blob_name):
            return
        logger.debug(f"Uploading metadata to {self.container}/{blob_name}")
        self.adapter.upload_metadata(data, blob_name)

    def remove_blob(self, blob_name: str) -> None:
        if not self.adapter.exists_blob(blob_name):
            return
        logger.debug(f"Deleting blob {blob_name}")
        self.adapter.remove_blob(blob_name)

