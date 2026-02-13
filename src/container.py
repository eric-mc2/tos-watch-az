from dataclasses import dataclass
import os

from src.adapters.embedding.client import SentenceTransformerAdapter
from src.adapters.embedding.fake_client import FakeEmbeddingAdapter
from src.adapters.http.protocol import HttpProtocol
from src.adapters.llm.client import ClaudeAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.adapters.http.client import RequestsAdapter
from src.adapters.http.fake_client import FakeHttpAdapter
from src.adapters.storage.client import AzureStorageAdapter
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.services.blob import BlobService
from src.services.embedding import EmbeddingService
from src.transforms.differ import Differ
from src.services.llm import LLMService
from src.transforms.icl import ICL
from src.transforms.llm_transform import LLMTransform
from src.transforms.metadata_scraper import MetadataScraper
from src.transforms.snapshot_scraper import SnapshotScraper
from src.transforms.summary.summarizer import Summarizer
from src.transforms.factcheck.claim_extractor import ClaimExtractor
from src.transforms.factcheck.claim_checker import ClaimChecker
from src.transforms.factcheck.judge import Judge

@dataclass
class ServiceContainer:
    """Dependency injection container"""

    # Adapters (infrastructure)
    storage: BlobService
    llm: LLMService

    # Transforms (business logic)
    llm_executor: LLMTransform
    differ_transform: Differ
    wayback_transform: MetadataScraper
    snapshot_transform: SnapshotScraper
    summarizer_transform: Summarizer
    claim_extractor_transform: ClaimExtractor
    claim_checker_transform: ClaimChecker
    judge_transform: Judge


    @classmethod
    def create(cls):
        target_env = os.environ.get("TARGET_ENV", "DEV")
        if target_env == "PROD" or target_env == "STAGE":
            return cls.create_real()
        else:
            return cls.create_fake()


    @classmethod
    def create_real(cls) -> 'ServiceContainer':
        """Create container with production dependencies"""
        blob_storage = BlobService(AzureStorageAdapter())
        http_client = RequestsAdapter()
        llm_client = LLMService(ClaudeAdapter())
        embedding_client = EmbeddingService(SentenceTransformerAdapter())
        return cls.create_container(blob_storage, http_client, llm_client, embedding_client)


    @classmethod
    def create_fake(cls) -> 'ServiceContainer':
        """Create container with test doubles"""
        blob_storage = BlobService(FakeStorageAdapter())
        http_client = FakeHttpAdapter()
        llm_client = LLMService(FakeLLMAdapter())
        embedding_client = EmbeddingService(FakeEmbeddingAdapter())
        return cls.create_container(blob_storage, http_client, llm_client, embedding_client)

    
    @classmethod
    def create_container(cls,
                         blob_storage: BlobService,
                         http_client: HttpProtocol,
                         llm_client: LLMService,
                         embedding_client: EmbeddingService):
        llm_executor = LLMTransform(blob_storage, llm_client)
        return cls(
            storage=blob_storage,
            llm=llm_client,
            llm_executor=llm_executor,
            differ_transform=Differ(blob_storage),
            wayback_transform=MetadataScraper(blob_storage, http_client),
            snapshot_transform=SnapshotScraper(blob_storage, http_client),
            summarizer_transform=Summarizer(blob_storage, ICL(blob_storage), llm_executor),
            claim_extractor_transform=ClaimExtractor(blob_storage, llm_executor),
            claim_checker_transform=ClaimChecker(blob_storage, llm_executor, embedding_client),
            judge_transform=Judge(blob_storage, llm_executor),
        )