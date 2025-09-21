from django.db.backends.base.operations import BaseDatabaseOperations

class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        return name

    def sql_flush(self, style, tables, sequences=(), allow_cascade=False, **kwargs):
        return [f"DELETE {t}" for t in tables]

    def max_name_length(self):
        return None

    def no_limit_value(self):
        return None

    def regex_lookup(self, lookup_type):
        raise NotImplementedError("regex lookups werden nicht unterstützt.")

    def last_insert_id(self, cursor, table_name, pk_name):
        lr = getattr(cursor, 'lastrowid', 0)
        try:
            return int(lr)
        except Exception:
            return 0
