from dataclasses import dataclass
import os
from src.clients.llm.protocol import LLMClient
from src.clients.storage.protocol import BlobStorageProtocol
from src.services.blob import BlobService
from src.services.differ import DiffService

@dataclass
class ServiceContainer:
    """Dependency injection container"""

    # Adapters (infrastructure)
    storage: BlobService
    llm: LLMClient

    # Services (business logic)
    differ_service: DiffService

    @classmethod
    def create_production(cls) -> 'ServiceContainer':
        from src.clients.storage.azure import AzureStorageAdapter
        """Create container with production dependencies"""
        blob_storage = BlobService(AzureStorageAdapter())
        llm_client = None #ClaudeAdapter(os.environ['ANTHROPIC_API_KEY'])

        return cls(
            storage=blob_storage,
            llm=llm_client,
            differ_service=DiffService(blob_storage),
        )

    @classmethod
    def create_dev(cls, **overrides) -> 'ServiceContainer':
        """Create container with test doubles"""
        from tests.clients.storage.fake_azure import FakeStorageAdapter

        blob_storage = overrides.get('blob_storage', BlobService(FakeStorageAdapter()))
        llm_client = None #overrides.get('llm_client', FakeLLMClient())

        return cls(
            storage=blob_storage,
            llm=llm_client,
            differ_service=DiffService(blob_storage),
        )