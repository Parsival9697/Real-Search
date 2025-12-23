# crawler_service/adapters/__init__.py
from .base import SourceAdapter
from .websearch_cse import WebSearchCSEAdapter

# (You can still keep other adapters in the package; only the ones you add
#  to the list in crawler_service/main.py will actually run.)

__all__ = [
    "SourceAdapter",
    "WebSearchCSEAdapter",
]
