from typing import Optional, Type
from schemas.base import SchemaBase

SCHEMA_REGISTRY : dict[str, dict[str, Type[SchemaBase]]] = {}


def register(module_name, name):
    def wrapper(cls):
        SCHEMA_REGISTRY.setdefault(module_name, {})
        SCHEMA_REGISTRY[module_name][name] = cls
        return cls
    return wrapper


def load_schema(module_name: str, version: str, metadata_module_name: Optional[str] = None) -> Type[SchemaBase]:
    schema_key = _get_schema_key(module_name, metadata_module_name)
    return SCHEMA_REGISTRY[schema_key][version]


def load_max_schema(module_name: str, metadata_module_name: Optional[str] = None) -> Type[SchemaBase]:
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

def increment_version(version: str) -> str:
    return _format_version(_parse_version(version) - 1)

def decrement_version(version: str) -> str:
    return _format_version(_parse_version(version) + 1)