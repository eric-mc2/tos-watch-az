from typing import Any, Callable, Optional, Type

from pydantic import BaseModel

from src.services.blob import BlobService

SCHEMA_REGISTRY : dict[str, dict[str, Type[BaseModel]]] = {}
MIGRATIONS : dict[str, Callable[[BaseModel, str], BaseModel]] = {}

def register_migration(module_name):
    def wrapper(func):
        MIGRATIONS[module_name] = func
        return func
    return wrapper

def register(module_name, name):
    def wrapper(cls):
        SCHEMA_REGISTRY.setdefault(module_name, {})
        SCHEMA_REGISTRY[module_name][name] = cls
        return cls
    return wrapper

def load_schema(module_name: str, version: str, metadata_module_name: Optional[str] = None) -> Type[BaseModel]:
    # Prioritize name embedded in metadata
    # (Allows flexibility if stages return union types)
    schema_key = metadata_module_name or module_name
    # Otherwise use compile-time name
    schema_versions = SCHEMA_REGISTRY.get(schema_key, SCHEMA_REGISTRY[module_name])
    schema = schema_versions[version]
    return schema

def load_data(blob_name: str, module_name: str, storage: BlobService) -> BaseModel:
    # TODO: use this function to replace common load / validate pattern
    txt = storage.load_text_blob(blob_name)
    metadata = storage.adapter.load_metadata(blob_name)
    schema = load_schema(module_name, metadata['schema_version'], metadata.get('module_name'))
    data = schema.model_validate_json(txt)
    if module_name in MIGRATIONS:
        data = MIGRATIONS[module_name](data, metadata['schema_version'])
    return data