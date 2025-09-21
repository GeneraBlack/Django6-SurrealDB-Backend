"""Django SurrealDB Backend Paket (Standalone).

Dieses Paket stellt einen Django-Datenbank-Backend-Treiber für SurrealDB bereit.
"""

# Stelle sicher, dass Unterpakete als Attribute verfügbar sind
from . import base as base  # noqa: F401
from . import management as management  # noqa: F401

__all__ = [
    "base",
    "management",
]
