from dataclasses import dataclass
from socket import create_connection

from src.clients.http.protocol import HttpProtocol
from src.clients.llm.client import ClaudeAdapter
from src.clients.llm.fake_client import FakeLLMAdapter
from src.clients.http.client import RequestsAdapter
from src.clients.http.fake_client import FakeHttpClient
from src.clients.storage.client import AzureStorageAdapter
from src.clients.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.services.differ import DiffService
from src.services.llm import LLMService
from src.services.metadata_scraper import MetadataScraper
from src.services.seeder import Seeder
from src.services.snapshot_scraper import SnapshotScraper
from src.services.summarizer import Summarizer


@dataclass
class ServiceContainer:
    """Dependency injection container"""

    # Adapters (infrastructure)
    storage: BlobService
    llm: LLMService

    # Services (business logic)
    seeder_service: Seeder
    differ_service: DiffService
    wayback_service: MetadataScraper
    snapshot_service: SnapshotScraper
    summarizer_service: Summarizer

    @classmethod
    def create_production(cls) -> 'ServiceContainer':
        """Create container with production dependencies"""
        blob_storage = BlobService(AzureStorageAdapter("documents"))
        http_client = RequestsAdapter()
        llm_client = LLMService(ClaudeAdapter())
        return cls.create_container(blob_storage, http_client, llm_client)

    @classmethod
    def create_dev(cls) -> 'ServiceContainer':
        """Create container with test doubles"""
        blob_storage = BlobService(FakeStorageAdapter('test-integration'))
        http_client = FakeHttpClient()
        llm_client = LLMService(FakeLLMAdapter())
        return cls.create_container(blob_storage, http_client, llm_client)

    @classmethod
    def create_container(cls, blob_storage: BlobService, http_client: HttpProtocol, llm_client: LLMService):
        return cls(
            storage=blob_storage,
            llm=llm_client,
            seeder_service=Seeder(blob_storage),
            differ_service=DiffService(blob_storage),
            wayback_service=MetadataScraper(blob_storage, http_client),
            snapshot_service=SnapshotScraper(blob_storage, http_client),
            summarizer_service=Summarizer(blob_storage, llm_client),
        )