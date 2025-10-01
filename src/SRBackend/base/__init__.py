"""Django-Datenbank-Backend für SurrealDB.

Dieses Paket stellt die Klasse `DatabaseWrapper` auf Modulebene bereit,
damit Django sie unter ENGINE = 'SRBackend.base' finden kann.
"""

from .base import DatabaseWrapper  # noqa: F401

__all__ = ["DatabaseWrapper"]