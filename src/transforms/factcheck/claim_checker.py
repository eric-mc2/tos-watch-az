import json
import logging
from dataclasses import dataclass
from typing import Iterator

from schemas.registry import SCHEMA_REGISTRY
from schemas.fact.v0 import CLAIMS_MODULE
from schemas.fact.v1 import Claims as ClaimsV1, FACT_VERSION as FACT_SCHEMA_VERSION, FACT_MODULE
from src.adapters.llm.protocol import Message, PromptMessages
from src.services.blob import BlobService
from src.services.embedding import EmbeddingService
from src.services.llm import TOKEN_LIMIT
from src.transforms.factcheck.vector_search import Indexer
from src.transforms.llm_transform import LLMTransform
from src.transforms.summary.prompt_chunker import PromptChunker
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)

PROMPT_VERSION = "v1"
N_ICL = 3
SYSTEM_PROMPT = """
Your role is the expert fact checker. Your task is to 
verify whether a document entails a specific claim. 
You should respond with valid json.

INPUT FORMAT:
{"claim": "abcde ...", "document": "defgh ...."}


OUTPUT FORMAT:
{"claim": "The verbatim input claim.", 
"veracity": bool, 
"reason": "One sentence describing why claim is true or not."}  
"""

@dataclass
class ClaimCheckerBuilder:
    storage: BlobService
    embedder: EmbeddingService

    def build_prompt(self, blob_name: str, diff_blob_name: str) -> Iterator[PromptMessages]:
        examples: list = []  # self.read_examples() for future ICL
        claims_text = self.storage.load_text_blob(blob_name)
        metadata = self.storage.adapter.load_metadata(blob_name)
        schema = SCHEMA_REGISTRY[CLAIMS_MODULE][metadata['schema_version']]
        claims = schema.model_validate_json(claims_text)
        assert isinstance(claims, ClaimsV1)

        if not claims.claims:
            # No actual claims found.
            return

        # Build RAG index once for all claims
        indexer = Indexer(storage=self.storage, embedder=self.embedder)
        indexer.build(diff_blob_name)
        logger.info(f"Built index with {indexer.get_index_size()} entries")

        # For each claim, retrieve relevant diffs and create prompt
        for claim in claims.claims:
            # Use RAG to find relevant document sections
            relevant_diffs = indexer.search(claim)
            
            chunker = PromptChunker(TOKEN_LIMIT)
            chunks = chunker.chunk_prompt(relevant_diffs)

            for chunk in chunks:

                # Format the retrieved diffs as context
                doc_context = self._format_diffs(chunk)
                
                prompt_data = dict(
                    claim=claim,
                    document=doc_context,
                )
                prompt = Message("user", json.dumps(prompt_data))
                yield PromptMessages(
                    system=SYSTEM_PROMPT,
                    history=examples,
                    current=prompt
                )
    
    @staticmethod
    def _format_diffs(diff_doc) -> str:
        """Format DiffDoc into readable context for the LLM."""
        if not diff_doc.diffs:
            return "No relevant document sections found."
        
        formatted_sections = []
        for i, diff in enumerate(diff_doc.diffs, 1):
            section = f"Section {i}:\n"
            if diff.before:
                section += f"Before: {diff.before}\n"
            if diff.after:
                section += f"After: {diff.after}\n"
            formatted_sections.append(section)
        
        return "\n".join(formatted_sections)


@dataclass
class ClaimChecker:
    storage: BlobService
    executor: LLMTransform
    embedder: EmbeddingService

    def check_claim(self, blob_name: str, other_blob_name: str) -> tuple[str, dict]:
        logger.debug(f"Checking claims from {blob_name}")
        prompter = ClaimCheckerBuilder(self.storage, self.embedder)
        messages = prompter.build_prompt(blob_name, other_blob_name)
        # Always annotate this as a FACT because the parser needs to validate the individual items, which are always facts.
        # It becomes a PROOF when the parser merges the FACTS / chunks together.
        return self.executor.execute_prompts(messages, FACT_MODULE, FACT_SCHEMA_VERSION, PROMPT_VERSION)

