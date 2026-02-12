from typing import Any, Callable, Optional, Type
from pydantic import BaseModel
from src.services.blob import BlobService

SCHEMA_REGISTRY : dict[str, dict[str, Type[BaseModel]]] = {}


def register(module_name, name):
    def wrapper(cls):
        SCHEMA_REGISTRY.setdefault(module_name, {})
        SCHEMA_REGISTRY[module_name][name] = cls
        return cls
    return wrapper


def load_data(blob_name: str, module_name: str, storage: BlobService) -> BaseModel:
    # Extract identifiers
    txt = storage.load_text_blob(blob_name)
    metadata = storage.adapter.load_metadata(blob_name)
    schema_version = metadata["schema_version"]  # This is pretty much guaranteed to exist
    metadata_module_name = metadata.get("module_name")   # This might not always exist
    # Find and load schema
    schema = load_schema(module_name, schema_version, metadata_module_name)
    data = schema.model_validate_json(txt)
    # Double-check if
    max_schema = load_max_schema(module_name, metadata_module_name)
    if _version_compare(schema_version, max_schema.VERSION()) < 0 and hasattr(max_schema, "migrate"):
        data = max_schema.migrate(data)
    return data


def load_schema(module_name: str, version: str, metadata_module_name: Optional[str] = None) -> Type[BaseModel]:
    schema_key = _get_schema_key(module_name, metadata_module_name)
    return SCHEMA_REGISTRY[schema_key][version]


def load_max_schema(module_name: str, metadata_module_name: Optional[str] = None) -> Type[BaseModel]:
    schema_key = _get_schema_key(module_name, metadata_module_name)
    max_version = max(map(_parse_version, SCHEMA_REGISTRY[schema_key].keys()))
    return load_schema(module_name, _format_version(max_version), metadata_module_name)


def _get_max_version(module_name: str, metadata_module_name: Optional[str] = None) -> str:
    schema_key = _get_schema_key(module_name, metadata_module_name)
    max_version = max(map(_parse_version, SCHEMA_REGISTRY[schema_key].keys()))
    return _format_version(max_version)

def _get_schema_key(module_name: str, metadata_module_name: Optional[str] = None) -> str:
    # Prioritize name embedded in metadata
    # (Allows flexibility if stages return union types)
    # Otherwise use compile-time name
    if metadata_module_name is not None and metadata_module_name in SCHEMA_REGISTRY:
        return metadata_module_name
    else:
        return module_name

def _parse_version(version: str) -> int:
    return int(version.lstrip('v'))

def _format_version(version: int) -> str:
    return f'v{version}'

def _version_compare(a: str, b: str) -> int:
    if a == b:
        return 0
    elif _parse_version(a) < _parse_version(b):
        return -1
    else:
        return 1