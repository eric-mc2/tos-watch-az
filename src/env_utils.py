import os

def dev_only(app_func):
    """
    Do not publish this function in production
    """
    if os.getenv("ENVIRONMENT") == "production":
        return None
    return app_func

