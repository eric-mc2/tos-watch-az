SCHEMA_REGISTRY = {}

def register(module_name, name):
    def wrapper(cls):
        SCHEMA_REGISTRY.setdefault(module_name, {})
        SCHEMA_REGISTRY[module_name][name] = cls
        return cls
    return wrapper
