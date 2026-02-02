from dataclasses import dataclass
from src.clients.llm.protocol import LLMClient
from src.services.blob import BlobService
from src.services.differ import DiffService
from src.services.metadata_scraper import MetadataScraper
from src.clients.http.client import RequestsAdapter
from src.snapshot_scraper import SnapshotScraper


@dataclass
class ServiceContainer:
    """Dependency injection container"""

    # Adapters (infrastructure)
    storage: BlobService
    llm: LLMClient

    # Services (business logic)
    differ_service: DiffService
    wayback_service: MetadataScraper
    snapshot_service: SnapshotScraper

    @classmethod
    def create_production(cls) -> 'ServiceContainer':
        from src.clients.storage.client import AzureStorageAdapter
        """Create container with production dependencies"""
        blob_storage = BlobService(AzureStorageAdapter())
        llm_client = None #ClaudeAdapter(os.environ['ANTHROPIC_API_KEY'])
        http_client = RequestsAdapter()

        return cls(
            storage=blob_storage,
            llm=llm_client,
            differ_service=DiffService(blob_storage),
            wayback_service=MetadataScraper(blob_storage, http_client),
            snapshot_service=SnapshotScraper(blob_storage, http_client)
        )

    @classmethod
    def create_dev(cls) -> 'ServiceContainer':
        """Create container with test doubles"""
        from src.clients.storage.fake_client import FakeStorageAdapter

        blob_storage = BlobService(FakeStorageAdapter('test-integration'))
        llm_client = None #overrides.get('llm_client', FakeLLMClient())
        http_client = RequestsAdapter()

        return cls(
            storage=blob_storage,
            llm=llm_client,
            differ_service=DiffService(blob_storage),
            wayback_service=MetadataScraper(blob_storage, http_client),
            snapshot_service=SnapshotScraper(blob_storage, http_client)
        )