import json
import logging
import os
from dataclasses import dataclass
from typing import Callable, Iterable

import ulid  # type: ignore

from schemas.registry import SCHEMA_REGISTRY
from src.adapters.llm.protocol import PromptMessages
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.stages import Stage
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__, logging.DEBUG)


@dataclass
class LLMTransform:
    """Utility class for executing LLM prompts with consistent error handling and response aggregation."""
    storage: BlobService
    llm: LLMService

    def execute_prompts(
        self, 
        prompts: Iterable[PromptMessages], 
        schema_version: str, 
        module_name: str,
        prompt_version: str | None = None
    ) -> tuple[str, dict]:
        """
        Execute a sequence of prompts against the LLM and aggregate responses.
        
        Args:
            prompts: Iterable of PromptMessages to execute
            schema_version: Schema version for metadata
            module_name: Module name for metadata (e.g., "summary", "claim", "factcheck", "judge")
            prompt_version: Optional prompt version for metadata tracking
            
        Returns:
            Tuple of (json_string, metadata_dict)
            - json_string: {"chunks": [response_dicts]}
            - metadata_dict: {"run_id": str, "schema_version": str, "prompt_version": str (optional)}
        """
        responses = []
        for message in prompts:
            txt = self.llm.call_unsafe(message.system, message.history + [message.current])
            parsed = self.llm.extract_json_from_response(txt)
            if parsed['success']:
                responses.append(parsed['data'])
            else:
                logger.warning(f"Failed to parse response: {parsed['error']}")
                responses.append({"error": parsed['error'], "raw": txt})

        response = json.dumps(dict(chunks=responses))
        metadata = dict(
            run_id=ulid.ulid(),
            schema_version=schema_version,
        )
        if prompt_version is not None:
            metadata['prompt_version'] = prompt_version
        return response, metadata


def create_llm_activity_processor(storage: BlobService, transform_fn: Callable, output_stage: str, workflow_name: str,
                                  paired_input_stage: str | None = None) -> Callable[[dict], None]:
    """
    Factory function to create a generic LLM activity processor.
    
    Args:
        storage: BlobService instance for path parsing and blob uploads
        transform_fn: Transform method that returns (output_text, metadata)
        output_stage: Stage enum value for RAW output (e.g., Stage.SUMMARY_RAW.value)
        workflow_name: Name for logging (e.g., "summarization", "claim extraction")
        paired_input_stage: Optional stage path for loading summary blob (judge workflow only)
        
    Returns:
        Activity processor function compatible with Azure Functions @activity_trigger
    """
    def processor(input_data: dict) -> None:
        blob_name = input_data['task_id']
        in_path = storage.parse_blob_path(blob_name)
        
        # Judge needs summary blob from earlier stage
        if paired_input_stage == Stage.DIFF_CLEAN.value:
            paired_blob_name = f"{paired_input_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}.json"
            output, metadata = transform_fn(blob_name, paired_blob_name)
        elif paired_input_stage is not None:
            paired_blob_name = f"{paired_input_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.json"
            output, metadata = transform_fn(blob_name, paired_blob_name)
        else:
            output, metadata = transform_fn(blob_name)
        
        # Save versioned output
        out_path = f"{output_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/{metadata['run_id']}.txt"
        storage.upload_text_blob(output, out_path, metadata=metadata)
        
        # Save latest output
        latest_path = f"{output_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.txt"
        storage.upload_text_blob(output, latest_path, metadata=metadata)
        
        logger.info(f"Successfully completed {workflow_name} for blob: {blob_name}")
    
    return processor


def create_llm_parser(storage: BlobService, llm: LLMService, module_name: str, output_stage: str) -> Callable:
    """
    Factory function to create a generic LLM output parser/validator.
    
    Args:
        storage: BlobService instance for path parsing and blob uploads
        llm: LLMService instance with validate_output method
        module_name: Module key in schema registry (e.g., "summary", "claim", "factcheck", "judge")
        output_stage: Stage enum value for CLEAN output (e.g., Stage.SUMMARY_CLEAN.value)
        
    Returns:
        Parser function compatible with Azure Functions @blob_trigger
    """
    def parser(input_blob) -> None:
        in_path = storage.parse_blob_path(input_blob.name)
        txt = input_blob.read().decode()
        metadata = storage.adapter.load_metadata(input_blob.name)
        
        # Get schema from registry
        schema = SCHEMA_REGISTRY[module_name][metadata['schema_version']]
        
        # Validate and clean output
        cleaned_txt = llm.validate_output(txt, schema)
        
        # Save versioned output
        out_path = os.path.join(output_stage, in_path.company, in_path.policy, in_path.timestamp, f"{metadata['run_id']}.json")
        storage.upload_json_blob(cleaned_txt, out_path, metadata=metadata)
        
        # Save latest output
        out_path = os.path.join(output_stage, in_path.company, in_path.policy, in_path.timestamp, "latest.json")
        storage.upload_json_blob(cleaned_txt, out_path, metadata=metadata)
        
        logger.info(f"Successfully validated {module_name} blob: {input_blob.name}")
    
    return parser
