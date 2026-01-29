CLASS_REGISTRY = {}

def register(name):
    def wrapper(cls):
        CLASS_REGISTRY[name] = cls
        return cls
    return wrapper
