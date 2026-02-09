from typing import Any

SCHEMA_REGISTRY : dict[str, dict[str, Any]] = {}

def register(module_name, name):
    def wrapper(cls):
        SCHEMA_REGISTRY.setdefault(module_name, {})
        SCHEMA_REGISTRY[module_name][name] = cls
        return cls
    return wrapper
