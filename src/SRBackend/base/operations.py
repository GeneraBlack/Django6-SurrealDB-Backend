from django.db.backends.base.operations import BaseDatabaseOperations

class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        # SurrealDB benötigt keine speziellen Identifier-Quotes; Rückgabe unverändert
        return name

    def sql_flush(self, style, tables, sequences=(), allow_cascade=False, **kwargs):
        # Liefert Statements, die alle Tabellen leeren. SurrealDB versteht DELETE <table>
        # Sequenzen/allow_cascade werden ignoriert.
        return [f"DELETE {t}" for t in tables]

    def max_name_length(self):
        # Keine harte Grenze erzwingen – Django nutzt None als „keine Begrenzung“
        return None

    def no_limit_value(self):
        return None

    def regex_lookup(self, lookup_type):
        raise NotImplementedError("regex lookups werden von diesem Mock nicht unterstützt.")

    def last_insert_id(self, cursor, table_name, pk_name):
        # Django ruft dies nach einem INSERT auf, wenn kein RETURNING unterstützt wird.
        # Wir geben die vom Cursor gesetzte lastrowid als int zurück.
        lr = getattr(cursor, 'lastrowid', 0)
        try:
            return int(lr)
        except Exception:
            return 0
