from dataclasses import dataclass
from typing import Literal

from src.adapters.http.protocol import HttpProtocol
from src.adapters.llm.client import ClaudeAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.adapters.http.client import RequestsAdapter
from src.adapters.http.fake_client import FakeHttpAdapter
from src.adapters.storage.client import AzureStorageAdapter
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.transforms.differ import Differ
from src.services.llm import LLMService
from src.transforms.metadata_scraper import MetadataScraper
from src.transforms.prompt_eng import PromptEng
from src.transforms.snapshot_scraper import SnapshotScraper
from src.transforms.summarizer import Summarizer


TEnv = Literal["DEV", "PROD"]

@dataclass
class ServiceContainer:
    """Dependency injection container"""

    # Adapters (infrastructure)
    storage: BlobService
    llm: LLMService

    # Transforms (business logic)
    differ_transform: Differ
    wayback_transform: MetadataScraper
    snapshot_transform: SnapshotScraper
    summarizer_transform: Summarizer
    prompt_transform: PromptEng

    @classmethod
    def create(cls, env: TEnv):
        if env == "PROD":
            return cls.create_production()
        else:
            return cls.create_dev()

    @classmethod
    def create_production(cls) -> 'ServiceContainer':
        """Create container with production dependencies"""
        blob_storage = BlobService(AzureStorageAdapter("documents", "APP_BLOB_CONNECTION_STRING"))
        http_client = RequestsAdapter()
        llm_client = LLMService(ClaudeAdapter())
        return cls.create_container(blob_storage, http_client, llm_client)

    @classmethod
    def create_dev(cls) -> 'ServiceContainer':
        """Create container with test doubles"""
        blob_storage = BlobService(AzureStorageAdapter("documents"))
        http_client = RequestsAdapter()
        llm_client = LLMService(ClaudeAdapter())
        return cls.create_container(blob_storage, http_client, llm_client)

    
    @classmethod
    def create_container(cls, blob_storage: BlobService, http_client: HttpProtocol, llm_client: LLMService):
        prompt_eng = PromptEng(blob_storage)
        return cls(
            storage=blob_storage,
            llm=llm_client,
            differ_transform=Differ(blob_storage),
            wayback_transform=MetadataScraper(blob_storage, http_client),
            snapshot_transform=SnapshotScraper(blob_storage, http_client),
            summarizer_transform=Summarizer(blob_storage, llm_client, prompt_eng),
            prompt_transform=prompt_eng,
        )