import logging
import os
from dataclasses import dataclass
from itertools import chain
from typing import Iterable

import numpy as np

from schemas.claim.v1 import Claims
from schemas.summary.v2 import Summary as SummaryV2, Substantive
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService
from src.transforms.differ import DiffDoc, DiffSection
from src.services.llm import TOKEN_LIMIT, LLMService
from src.transforms.factcheck.claim_checker import ClaimChecker
from src.transforms.factcheck.claim_extractor import ClaimExtractor, ClaimExtractorBuilder
from src.transforms.factcheck.vector_search import Indexer
from src.transforms.summary.prompt_chunker import PromptChunker
from src.stages import Stage
from src.transforms.prompt_eng import PromptEng
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

@dataclass
class FactChecker:
    storage: BlobService
    llm: LLMService
    embedder: EmbeddingService

    def check(self, summary_blob_name: str, diff_blob_name: str):
        claim_extractor = ClaimExtractor(self.storage, self.llm)
        claim_checker = ClaimChecker(self.storage, self.llm)
        indexer = Indexer(self.storage, self.embedder)

        # Extract verifiable facts
        claims_txt, claims_meta = claim_extractor.extract_claims(summary_blob_name)
        claims = Claims.model_validate_json(claims_txt)

        # RAG
        indexer.build(diff_blob_name)
        for claim in claims.claims:
            doc = indexer.search(claim)
            claim_checker.check_claim(claim, doc)

