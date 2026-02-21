import json
import logging
import os
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, List

import ulid  # type: ignore

from schemas.base import SchemaBase
from schemas.brief.v0 import BRIEF_MODULE
from schemas.chunking import ChunkedResponse
from schemas.llmerror.v1 import LLMError
from schemas.registry import load_schema
from schemas.fact.v0 import FACT_MODULE, PROOF_MODULE
from src.adapters.llm.protocol import PromptMessages, Message
from src.services.blob import BlobService
from src.services.llm import LLMService
from src.stages import Stage
from src.utils.log_utils import setup_logger
from src.utils.metadata_utils import merge_lineage, prefix_metadata, extract_stage_metadata, is_lineage_data

logger = setup_logger(__name__, logging.DEBUG)


@dataclass
class LLMTransform:
    """Utility class for executing LLM prompts with consistent error handling and response aggregation."""
    storage: BlobService
    llm: LLMService

    def execute_prompts(self,
                        prompts: Iterable[PromptMessages],
                        module_name: str,
                        schema_version: str,
                        prompt_version: str) -> tuple[str, dict]:
        """
        Execute a sequence of prompts against the LLM and aggregate responses.
        
        Args:
            prompts: Iterable of PromptMessages to execute
            schema_version: Schema version for metadata
            prompt_version: Prompt version for metadata tracking
            
        Returns:
            Tuple of (json_string, metadata_dict)
            - json_string: If single response, returns the response JSON directly.
                          If multiple responses, returns {"chunks": [response_dicts]}
            - metadata_dict: {"run_id": str, "schema_version": str, "prompt_version": str, 
                             "is_chunked": bool}
        """
        responses = []
        error_flag = None
        for message in prompts:
            txt = self.llm.call_unsafe(message.system, message.history + [message.current])
            parsed = self.llm.extract_json_from_response(txt)
            if parsed.success:
                responses.append(parsed.data)
            else:
                logger.warning(f"Failed to parse response: {parsed.error}")
                error_flag = LLMError.VERSION()  # Raise error flag
                responses.append(LLMError(error=parsed.error, raw=txt).model_dump())

        # Only wrap in chunks array if actually chunked (>1 response)
        # Use un-typed json.dumps here instead of model_dump_json because we haven't validated yet.
        if len(responses) == 0:
            response = ""
            is_chunked = False
        elif len(responses) == 1:
            response = json.dumps(responses[0])
            is_chunked = False
        else:
            response = json.dumps(dict(chunks=responses))
            is_chunked = True
        
        metadata = dict(
            run_id=ulid.ulid(),
            module_name=module_name,
            schema_version=schema_version,
            model_version=self.llm.adapter.get_model_version(),
            prompt_version=prompt_version,
            is_chunked=is_chunked,
            error_flag=error_flag,
        )
        return response, metadata
    
    def execute_prompts_serial(self,
                               prompts: Iterable[PromptMessages],
                               module_name: str,
                               schema_version: str,
                               prompt_version: str,
                               initial_state: Optional[str] = None) -> tuple[str, dict]:
        """
        Execute a sequence of prompts sequentially with validation and state passing.
        
        Args:
            prompts: Iterable of PromptMessages to execute sequentially
            module_name: Module name for metadata
            schema_version: Schema version for metadata
            prompt_version: Prompt version for metadata tracking
            validator: Function (text, metadata) -> (validated_text, metadata)
                      Called after each prompt to validate before passing to next.
                      Should return ("", {}) on validation failure.
            saver: Optional function to save intermediate results
            initial_state: Optional seed response for first prompt's history
            
        Returns:
            List of validated_json, metadata tuple
            Returns empty list if any prompt fails validation or execution.
        """
        responses : List[dict] = []
        previous_message = Message("assistant", initial_state or "")

        parser = create_llm_parser(self.llm, module_name)
        
        metadata = dict(
                run_id=ulid.ulid(),
                module_name=module_name,
                schema_version=schema_version,
                prompt_version=prompt_version,
                is_chunked=False, # for sequential parsing
                error_flag=None
            )
        
        for prompt_msg in prompts:
            # Inject previous validated response into prompt's history
            if previous_message.content:
                prompt_msg.history = [previous_message]
            
            # Execute single prompt
            txt = self.llm.call_unsafe(prompt_msg.system,
                                       prompt_msg.history + [prompt_msg.current])
            parsed = self.llm.extract_json_from_response(txt)
            
            if not parsed.success:
                logger.warning(f"Failed to parse response: {parsed.error}")
                metadata['error_flag'] = LLMError.VERSION()
                response = LLMError(error=parsed.error, raw=txt).model_dump()
            else:
                response_json, metadata = parser(json.dumps(parsed.data), metadata)
                response = json.loads(response_json)
                previous_message = Message("assistant", response_json)
                
            responses.append(response)

            if not parsed.success:
                break
                                
        if len(responses) > 1:
            out_txt = json.dumps(dict(chunks=responses))
            metadata['is_chunked'] = True
        else:
            out_txt = json.dumps(responses[0])
        
        return out_txt, metadata


def create_llm_activity_processor(storage: BlobService,
                                  transform_fn: Callable,
                                  output_stage: str,
                                  workflow_name: str,
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
        upstream_metadata = storage.adapter.load_metadata(blob_name)
        
        # Judge needs summary blob from earlier stage
        if paired_input_stage == Stage.DIFF_CLEAN.value:
            paired_blob_name = f"{paired_input_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}.json"
            output, metadata = transform_fn(blob_name, paired_blob_name)
        elif paired_input_stage is not None:
            paired_blob_name = f"{paired_input_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.json"
            output, metadata = transform_fn(blob_name, paired_blob_name)
        else:
            output, metadata = transform_fn(blob_name)

        run_id = metadata['run_id']
        metadata = merge_lineage(upstream_metadata, metadata, output_stage)

        if output:
            # Save versioned output
            out_path = f"{output_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/{run_id}.txt"
            storage.upload_text_blob(output, out_path, metadata=metadata)
            
            # Save latest output
            latest_path = f"{output_stage}/{in_path.company}/{in_path.policy}/{in_path.timestamp}/latest.txt"
            storage.upload_text_blob(output, latest_path, metadata=metadata)
        
        logger.info(f"Successfully completed {workflow_name} for blob: {blob_name}")
    
    return processor


def create_llm_parser_saver[T: SchemaBase](storage: BlobService,
                                           llm: LLMService,
                                           module_name: str,
                                           output_stage: str,
                                           merge_fn: Optional[Callable[[T, T], T]] = None) -> Callable:
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
    def parser_saver(input_blob) -> tuple[str, dict]:
        txt = input_blob.read().decode()
        metadata = storage.adapter.load_metadata(input_blob.name)
        parser = create_llm_parser(llm, module_name, merge_fn)
        cleaned_txt, metadata = parser(txt, metadata)
        if not cleaned_txt:
            return "", {}
        logger.info(
            (f"Successfully validated {module_name} blob: {input_blob.name} "
             f"(chunked={metadata['is_chunked']})"))
        saver = create_llm_saver(storage, output_stage)
        saver(input_blob.name, cleaned_txt, metadata)
        return cleaned_txt, metadata
    return parser_saver


def create_llm_parser[T: SchemaBase](llm: LLMService,
                                     module_name: str,
                                     merge_fn: Optional[Callable[[T, T], T]] = None) -> Callable:
    """
    Factory function to create a generic LLM output parser/validator.

    Handles both chunked and non-chunked formats for backward compatibility:
    - Detects chunking from metadata['is_chunked'] flag (new format)
    - Falls back to structure detection ({"chunks": [...]}) for historical data
    - Validates each chunk individually against the business schema
    - Stores in the same format it was received (preserves chunking)

    Args:
        storage: BlobService instance for path parsing and blob uploads
        llm: LLMService instance with validate_output method
        module_name: Module key in schema registry (e.g., "summary", "claim", "factcheck", "judge")
        output_stage: Stage enum value for CLEAN output (e.g., Stage.SUMMARY_CLEAN.value)

    Returns:
        Parser function
    """

    def parser(txt: str, metadata: dict) -> tuple[str, dict]:

        # When this is called in the context of serial_execute it receives non-lineage data.
        # But in the context of function_app.parse it receives lineage data.
        # In the first case it should be a no-op. In the second case it should
        # extract the stage-specific parts for easier handling.
        stage_metadata = extract_stage_metadata(metadata, tag=module_name)
        schema_version = stage_metadata['schema_version']
        module_key = stage_metadata.get('module_name', module_name)
        schema = load_schema(module_name, schema_version, module_key)

        # Check for errors
        error_flag = stage_metadata.get('error_flag')

        # New format: detect chunking from metadata or structure
        is_chunked = stage_metadata.get('is_chunked', False)
        if not is_chunked:
            # Try to detect from structure for historical data
            try:
                parsed = json.loads(txt)
                is_chunked = isinstance(parsed, dict) and 'chunks' in parsed and isinstance(parsed.get('chunks'), list)
            except json.JSONDecodeError:
                is_chunked = False

        if error_flag is not None:
            logger.warning("LLM returned structurally invalid output: \n%s", txt)
            return "", {}  # exit early! don't save anything

        if is_chunked:
            # Chunked format: ChunkedResponse stores raw dicts, validates at merge-time
            wrapper = ChunkedResponse.model_validate_json(txt)
            if module_key == FACT_MODULE:
                # Unfortunate custom logic: chunked facts arrive as facts and are turned into proofs.
                stage_metadata['module_name'] = PROOF_MODULE
            elif module_key == BRIEF_MODULE:
                stage_metadata['module_name'] = BRIEF_MODULE
            # Auto-discovers schema.merge if exists
            data = wrapper.merge(schema, merge_fn=merge_fn)  # type: ignore
        else:
            # Single item format: validate directly
            data = schema.model_validate_json(txt)

        cleaned_data = llm.sanitize_response(data.model_dump())
        cleaned_txt = json.dumps(cleaned_data, indent=2)

        # Update metadata with chunking info -- we always merge!
        stage_metadata['is_chunked'] = False

        # In the context of serial_executor, we're dealing with non-prefixed keys.
        # In the context of function_app.parse, we need to re-promote the keys to lineage.
        tag = module_name if is_lineage_data(metadata) else None
        stage_metadata = prefix_metadata(stage_metadata, tag=tag)
        metadata.update(stage_metadata)

        return cleaned_txt, metadata

    return parser


def create_llm_saver(storage: BlobService, output_stage: str) -> Callable:
    def saver(blob_name: str, txt: str, metadata: dict) -> None:
        in_path = storage.parse_blob_path(blob_name)
        stage_metadata = extract_stage_metadata(metadata, output_stage)
        run_id = stage_metadata['run_id']
        # Save versioned output
        out_path = os.path.join(output_stage, in_path.company, in_path.policy, in_path.timestamp, f"{run_id}.json")
        storage.upload_json_blob(txt, out_path, metadata=metadata)

        # Save latest output
        latest_path = os.path.join(output_stage, in_path.company, in_path.policy, in_path.timestamp, "latest.json")
        storage.upload_json_blob(txt, latest_path, metadata=metadata)
    return saver