"""
Metadata utilities for managing stage-prefixed metadata in the pipeline.

This module provides utilities for:
1. Prefixing metadata keys with stage names
2. Merging metadata from upstream stages
3. Unprefixing metadata for backward compatibility
"""
from operator import truediv
from typing import Optional
from src.stages import Stage

# Metadata keys that should be prefixed per stage
PREFIXABLE_KEYS = [
    "schema_version",
    "prompt_version",
    "model_version",
    "run_id",
    "module_name",
]


# Metadata keys that should never be prefixed
GLOBAL_KEYS = [
    "error_flag",
    "is_chunked",
    "touched",  # Blob touch timestamp
]

def prefix_metadata(metadata: dict, stage: Optional[str] = None, tag: Optional[str] = None) -> dict:
    """
    Prefix metadata keys with stage transform name.
    
    Example:
        Input:  {"run_id": "abc123", "schema_version": "v1", "touched": "2024-01-01"}
        Stage:  Stage.BRIEF_RAW.value
        Output: {"briefer_run_id": "abc123", "briefer_schema_version": "v1", "touched": "2024-01-01"}
    
    Args:
        metadata: Original metadata dict
        stage: Stage enum value
        tag: Prefixed stage name

    Returns:
        New dict with prefixed keys (original dict unchanged)
    """
    transform_name = tag if tag else Stage.get_transform_name(stage) if stage else None
    if transform_name is None:
        return metadata.copy()
    
    prefixed = {}
    for key, value in metadata.items():
        if key in GLOBAL_KEYS:
            prefixed[key] = value
        elif key in PREFIXABLE_KEYS:
            prefixed[f"{transform_name}_{key}"] = value
        else:
            # Keep unprefixed keys that aren't in our known sets
            # (These might be from upstream stages)
            prefixed[key] = value
    
    return prefixed


def unprefix_metadata(metadata: dict, stage: str) -> dict:
    """
    Remove stage prefix from metadata keys (for backward compatibility).
    
    Example:
        Input:  {"briefer_run_id": "abc123", "briefer_schema_version": "v1"}
        Stage:  Stage.BRIEF_RAW.value
        Output: {"run_id": "abc123", "schema_version": "v1"}
    
    Args:
        metadata: Prefixed metadata dict
        stage: Stage enum value
        
    Returns:
        New dict with unprefixed keys (original dict unchanged)
    """
    transform_name = Stage.get_transform_name(stage)
    if transform_name is None:
        return metadata.copy()
    
    unprefixed = {}
    prefix = f"{transform_name}_"
    
    for key, value in metadata.items():
        if key.startswith(prefix):
            unprefixed_key = key[len(prefix):]
            unprefixed[unprefixed_key] = value
        else:
            unprefixed[key] = value
    
    return unprefixed


def merge_lineage(upstream_metadata: dict, new_metadata: dict, current_stage: str) -> dict:
    """
    Merge upstream metadata with new stage metadata.
    
    This function:
    1. Preserves all upstream stage metadata (already prefixed)
    2. Prefixes the new stage's metadata
    3. Returns combined dict
    
    Example:
        upstream_metadata = {"briefer_run_id": "abc", "briefer_schema_version": "v1"}
        new_metadata = {"run_id": "xyz", "schema_version": "v2", "prompt_version": "v8"}
        current_stage = Stage.SUMMARY_RAW.value
        
        Returns: {
            "briefer_run_id": "abc",
            "briefer_schema_version": "v1", 
            "summarizer_run_id": "xyz",
            "summarizer_schema_version": "v2",
            "summarizer_prompt_version": "v8"
        }
    
    Args:
        upstream_metadata: Metadata from previous stage(s) - already prefixed
        new_metadata: New metadata for current stage - not yet prefixed
        current_stage: Current stage enum value
        
    Returns:
        Merged metadata dict with all lineage preserved
    """
    # Start with upstream metadata (already prefixed)
    merged = upstream_metadata.copy()
    
    # Prefix and add current stage metadata
    prefixed_new = prefix_metadata(new_metadata, current_stage)
    merged.update(prefixed_new)
    
    return merged


def extract_stage_metadata(metadata: dict, stage: Optional[str] = None, tag: Optional[str] = None) -> dict:
    """
    Extract only the metadata for a specific stage.
    
    Args:
        metadata: Full metadata dict with multiple stages
        stage: Stage enum value to extract
        
    Returns:
        Dict containing only the specified stage's metadata (unprefixed)
    """
    transform_name = tag if tag else Stage.get_transform_name(stage) if stage else None
    if transform_name is None:
        return {}
    
    prefix = f"{transform_name}_"
    stage_metadata = {}
    
    # Run through all keys first and check for explicit stage keys.
    for key, value in metadata.items():
        unprefixed_key = key.removeprefix(prefix)
        if key == prefix + unprefixed_key and unprefixed_key in PREFIXABLE_KEYS:
            stage_metadata[unprefixed_key] = value

    # Now run through and check for non-stage keys to add.
    for key in metadata.keys() - stage_metadata.keys():
        if key in PREFIXABLE_KEYS or key in GLOBAL_KEYS:
            stage_metadata[key] = metadata[key]

    # Unlisted keys belongs to something else. Don't add.
    return stage_metadata

def is_lineage_data(metadata: dict) -> bool:
    for stage in Stage:
        pfx = Stage.get_transform_name(stage.value)
        for key in PREFIXABLE_KEYS:
            if f"{pfx}_{key}" in metadata:
                return True
    return False