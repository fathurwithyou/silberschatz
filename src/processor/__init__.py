from .exceptions import AbortError

__all__ = ["QueryProcessor", "AbortError"]


def __getattr__(name: str):
    if name == "QueryProcessor":
        from .processor import QueryProcessor as _QueryProcessor
        return _QueryProcessor
    raise AttributeError(f"module 'src.processor' has no attribute '{name}'")
