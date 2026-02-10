from typing import Any, Type

from pydantic import BaseModel

SCHEMA_REGISTRY : dict[str, dict[str, Type[BaseModel]]] = {}

def register(module_name, name):
    def wrapper(cls):
        SCHEMA_REGISTRY.setdefault(module_name, {})
        SCHEMA_REGISTRY[module_name][name] = cls
        return cls
    return wrapper