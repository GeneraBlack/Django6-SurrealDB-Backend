# pyright: reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownLambdaType=false
from typing import Any, List, Tuple, Optional, Dict, Sequence, Set, cast
import threading
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.creation import BaseDatabaseCreation
from .operations import DatabaseOperations
from . import metrics as _dbm


COUNT_FUNC = 'count()'


class DatabaseFeatures:
    """Django DatabaseFeatures für SurrealDB (mit konservativen Flags)."""

    def __init__(self, connection: Any):
        self.connection = connection
        self.interprets_empty_strings_as_nulls = False
        self.supports_transactions = False
        self.supports_order_by_nulls_modifier = False
        self.supports_foreign_keys = False
        self.supports_column_check_constraints = False
        self.supports_table_check_constraints = False
        self.supports_index_column_ordering = False
        self.supports_paramstyle_pyformat = True
        self.supports_sequence_reset = False
        self.supports_timezones = False
        self.supports_microsecond_precision = True
        self.supports_json_field = True
        self.supports_partial_indexes = False
        # Django 6: Abfrage für Covering Indexes (INCLUDE-Spalten). SurrealDB unterstützt dies nicht.
        self.supports_covering_indexes = False
        self.supports_expression_indexes = False
        # Django nutzt chunked reads für speichereffiziente Cursors – nicht unterstützt
        self.can_use_chunked_reads = False
        self.supports_functions_in_partial_indexes = False
        self.supports_ignore_conflicts = False
        self.supports_select_for_update = False
        self.supports_select_for_update_with_limit = False
        self.supports_select_for_update_nowait = False
        self.supports_select_for_update_skip_locked = False
        self.supports_subqueries_in_group_by = False
        self.supports_update_conflicts_with_target = False
        self.supports_update_conflicts_with_where = False
        self.supports_update_conflicts_with_constraint = False
        self.supports_update_conflicts_with_index = False
        self.supports_update_conflicts_with_excluded = False
        self.supports_update_conflicts_with_returning = False
        self.truncates_names = False
        # Kein RETURNING – Django bezieht IDs über last_insert_id
        self.can_return_columns_from_insert = False
        # ORM-Flags für Subqueries in DELETE/UPDATE (SurrealQL unterstützt dies so nicht direkt)
        self.delete_can_self_reference_subquery = False
        self.update_can_self_reference_subquery = False
        self.supports_default_keyword_in_bulk_insert = False
        self.can_return_rows_from_bulk_insert = False
        self.has_bulk_insert = False
        self.empty_fetchmany_value = []
        # DDL-Rollback wird nicht unterstützt
        self.can_rollback_ddl = False


class DatabaseIntrospection:
    def __init__(self, connection: Any):
        self.connection = connection

    def table_names(self, _cursor: Any) -> List[str]:
        # Tabellennamen via INFO FOR DB abrufen
        result: Any = self.connection.connection.query("INFO FOR DB")
        names: List[str] = []
        if isinstance(result, list) and result and isinstance(result[0], dict) and 'result' in result[0]:
            dbinfo_any: Any = result[0]['result']
            dbinfo: Dict[str, Any] = dbinfo_any if isinstance(dbinfo_any, dict) else {}
            tb_any: Any = dbinfo.get('tb', {})
            if isinstance(tb_any, dict):
                names = [str(k) for k in tb_any.keys()]  # type: ignore[arg-type]
        # Ergänze 'django_migrations' falls implicit vorhanden
        if 'django_migrations' not in names:
            try:
                _probe = self.connection.connection.query("SELECT * FROM django_migrations LIMIT 1")
                names.append('django_migrations')
            except Exception:
                pass
        return names

    # Wird u. a. von flush() in Tests verwendet
    def django_table_names(self, only_existing: bool = False, include_views: bool = True) -> List[str]:  # noqa: ARG002
        # Versuche zunächst INFO FOR DB
        try:
            names: Set[str] = set(self.table_names(None))
        except Exception:
            names = set()

        # Sicherstellen, dass die zentrale Django-Tabelle 'django_migrations' erkannt wird,
        # auch wenn sie nicht via DEFINE TABLE angelegt wurde (SurrealDB erstellt Tabellen implizit)
        if 'django_migrations' not in names:
            try:
                # "Existenzprobe": wenn Query OK zurückgibt, existiert die Tabelle faktisch
                _probe = self.connection.connection.query("SELECT * FROM django_migrations LIMIT 1")
                # Wenn keine Exception kam, füge sie hinzu
                names.add('django_migrations')
            except Exception:
                # ignorieren – Tabelle existiert ggf. noch nicht
                pass

        return sorted(names)

    def close(self):
        # Kein eigener Ressourcen-Handle hier nötig
        pass


class DatabaseValidation:
    def __init__(self, connection: Any):
        self.connection = connection

    def check(self, **kwargs: Any) -> List[Any]:
        return []

    def check_field(self, _field: Any, **kwargs: Any) -> List[Any]:
        return []

    def alter_field(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class DatabaseWrapper(BaseDatabaseWrapper):
    def get_connection_params(self):
        # Reiche die settings 1:1 weiter
        return self.settings_dict

    class SurrealDBCreation(BaseDatabaseCreation):
        """Minimaler Creation-Adapter für Django-Tests.
        Wir verwenden dieselbe DB/NS und räumen nicht speziell auf.
        """
        def __init__(self, db_wrapper: BaseDatabaseWrapper):
            super().__init__(db_wrapper)

        def create_test_db(self, verbosity: int = 1, autoclobber: bool = False, serialize: bool = False, keepdb: bool = False):
            # Keine separate Test-DB — wir nutzen dieselbe DB/NS
            return self.connection.settings_dict.get('NAME')

        def destroy_test_db(self, old_database_name: Optional[str] = None, verbosity: int = 1, keepdb: bool = False, suffix: Optional[str] = None) -> None:
            # Kein spezielles Destroying nötig
            return

        def test_db_signature(self) -> Tuple[Any, Any, Any, Any]:
            s = self.connection.settings_dict
            return (s.get('NAME'), s.get('NAMESPACE'), s.get('HOST'), s.get('PORT'))

    creation_class = SurrealDBCreation

    class SurrealDBClient:
        def __init__(self, db_wrapper: BaseDatabaseWrapper):
            self.db_wrapper = db_wrapper

        def runshell(self):
            print("SurrealDB bietet keine interaktive Shell.")

    client_class = SurrealDBClient
    vendor = 'SurrealDB'
    display_name = 'SurrealDB'
    ops_class = DatabaseOperations
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    validation_class = DatabaseValidation

    class SurrealDBSchemaEditor:
        """SchemaEditor-Stub für Django-Kompatibilität."""

        atomic_migration = False

        def __init__(self, connection: Any, *_args: Any, **_kwargs: Any):
            self.connection = connection
            self.deferred_sql = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc: Any):
            return False
        def create_model(self, model: Any, *_args: Any, **_kwargs: Any):  # pragma: no cover
            """Sehr vereinfachte Tabellen-Anlage.

            Für SCHEMALESS Nutzung genügt ein einfaches DEFINE TABLE. Felder definieren wir
            bewusst nicht zwingend, da SurrealDB dynamische Felder erlaubt. Für die
            spezielle *django_migrations*-Tabelle legen wir ein paar Felder explizit an,
            damit die Migrationserkennung konsistent bleibt.
            """
            try:  # defensive: Fehler hier dürfen Migration nicht killen
                tbl = getattr(getattr(model, '_meta', None), 'db_table', None)
                if not tbl:
                    return None
                if tbl == 'django_migrations':
                    self.connection.connection.query("DEFINE TABLE IF NOT EXISTS django_migrations SCHEMALESS")  # type: ignore[attr-defined]
                    self.connection.connection.query("DEFINE FIELD IF NOT EXISTS app ON TABLE django_migrations TYPE string")  # type: ignore[attr-defined]
                    self.connection.connection.query("DEFINE FIELD IF NOT EXISTS name ON TABLE django_migrations TYPE string")  # type: ignore[attr-defined]
                    self.connection.connection.query("DEFINE FIELD IF NOT EXISTS applied ON TABLE django_migrations TYPE datetime")  # type: ignore[attr-defined]
                else:
                    self.connection.connection.query(f"DEFINE TABLE IF NOT EXISTS {tbl} SCHEMALESS")  # type: ignore[attr-defined]
            except Exception:
                # Ignorieren – wir wollen nicht abbrechen
                pass
            return None

        # --- Zusätzliche Stub-Methoden für Django Migration Operations ---
        def add_field(self, model: Any, field: Any):  # noqa: D401 pragma: no cover
            """No-Op: Felder werden in SCHEMALESS Tabellen nicht vorab benötigt."""
            # Optional könnten wir hier DEFINE FIELD erzeugen – für jetzt nicht nötig.
            return None

        def add_index(self, model: Any, index: Any):  # noqa: D401 pragma: no cover
            """No-Op: SurrealDB Indizes könnten via DEFINE INDEX erzeugt werden; später ausbauen."""
            try:
                # Minimaler Versuch falls der Index einfache Feldliste hat – optional.
                fields = getattr(index, 'fields', None)
                name = getattr(index, 'name', None)
                tbl = getattr(getattr(model, '_meta', None), 'db_table', None)
                if tbl and fields and name:
                    field_list = ','.join(fields)
                    self.connection.connection.query(f"DEFINE INDEX {name} ON {tbl} FIELDS {field_list} TYPE BTREE")  # type: ignore[attr-defined]
            except Exception:
                pass
            return None

        def remove_field(self, *_args: Any, **_kwargs: Any):  # noqa: D401 pragma: no cover
            return None

        def alter_field(self, *_args: Any, **_kwargs: Any):  # noqa: D401 pragma: no cover
            return None

        def alter_unique_together(self, *_args: Any, **_kwargs: Any):  # noqa: D401 pragma: no cover
            return None

    SchemaEditorClass = SurrealDBSchemaEditor  # type: ignore[assignment]

    # Exception-Klassen wie von Django erwartet
    class Database:
        pass

    DatabaseError = type('DatabaseError', (Exception,), {})
    DataError = type('DataError', (DatabaseError,), {})
    OperationalError = type('OperationalError', (DatabaseError,), {})
    IntegrityError = type('IntegrityError', (DatabaseError,), {})
    InternalError = type('InternalError', (DatabaseError,), {})
    InterfaceError = type('InterfaceError', (DatabaseError,), {})
    ProgrammingError = type('ProgrammingError', (DatabaseError,), {})
    NotSupportedError = type('NotSupportedError', (DatabaseError,), {})
    Error = DatabaseError
    Database.DatabaseError = DatabaseError  # type: ignore[attr-defined]
    Database.DataError = DataError  # type: ignore[attr-defined]
    Database.OperationalError = OperationalError  # type: ignore[attr-defined]
    Database.IntegrityError = IntegrityError  # type: ignore[attr-defined]
    Database.InternalError = InternalError  # type: ignore[attr-defined]
    Database.InterfaceError = InterfaceError  # type: ignore[attr-defined]
    Database.ProgrammingError = ProgrammingError  # type: ignore[attr-defined]
    Database.NotSupportedError = NotSupportedError  # type: ignore[attr-defined]
    Database.Error = Error  # type: ignore[attr-defined]

    def __init__(self, settings_dict: Dict[str, Any], alias: Optional[str] = None):
        super().__init__(settings_dict, alias or "default")
        self.Database = self.__class__.Database
        LIKE = 'LIKE %s'
        self.operators = {
            'exact': '= %s',
            'iexact': '= %s',
            'contains': LIKE,
            'icontains': LIKE,
            'regex': '~ %s',
            'iregex': '~* %s',
            'gt': '> %s',
            'gte': '>= %s',
            'lt': '< %s',
            'lte': '<= %s',
            'startswith': LIKE,
            'endswith': LIKE,
            'istartswith': LIKE,
            'iendswith': LIKE,
            'in': 'IN %s',
        }

    def get_new_connection(self, conn_params: Dict[str, Any]) -> Any:  # type: ignore[override]
        return CustomDBConnection(conn_params)

    def init_connection_state(self):
        return None

    def create_cursor(self, name: Optional[str] = None):
        return self.connection.cursor()

    def close(self):
        if self.connection:
            self.connection.close()

    def rollback(self):
        self.connection.rollback()

    def commit(self):
        self.connection.commit()

    def _set_autocommit(self, _autocommit: bool):
        # SurrealDB autocommit – nichts zu tun
        pass


class CustomDBConnection:
    def __init__(self, settings_dict: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any):
        # Debug-Ausgabe der Settings
        _raw_settings: Dict[str, Any] = {} if settings_dict is None else settings_dict
        self.settings_dict: Dict[str, Any] = dict(_raw_settings)
        # Debug-/Logging-Steuerung über settings.py -> DATABASES[...]['OPTIONS']
        opts: Dict[str, Any] = cast(Dict[str, Any], self.settings_dict.get('OPTIONS') or {})
        self._debug = bool(opts.get('SUR_DEBUG') or opts.get('DEBUG') or False)
        self._log_queries = bool(opts.get('SUR_LOG_QUERIES') or False)
        self._log_responses = bool(opts.get('SUR_LOG_RESPONSES') or False)
        self._profile = bool(opts.get('SUR_PROFILE') or False)
        self._trace_sql = bool(opts.get('SUR_TRACE_SQL') or False)
        # Zusätzliche Optionen für Middleware/Slow-Query-Logging
        try:
            self._slow_ms = float(opts.get('SUR_SLOW_QUERY_MS', 100.0))
        except Exception:
            self._slow_ms = 100.0
        self._log_query_body = bool(opts.get('SUR_LOG_QUERY_BODY', True))
        self._metrics_headers_verbose = bool(opts.get('SUR_METRICS_HEADERS_VERBOSE', False))
        # Erzwinge (wo möglich) Datenkonsistenz wie in relationalen DBs (z.B. unique constraints)
        self._ensure_uniques = bool(opts.get('SUR_ENSURE_UNIQUES', True))
        if self._debug:
            print("[SurrealDB-DEBUG] settings_dict (komplett):", self.settings_dict)
        self.user = str(self.settings_dict.get('USER') or 'root')
        self.password = str(self.settings_dict.get('PASSWORD') or 'root')
        self.host = str(self.settings_dict.get('HOST') or 'localhost')
        self.port = int(self.settings_dict.get('PORT') or 8080)
        self.db_name = str(self.settings_dict.get('NAME') or '')
        self.namespace = str(self.settings_dict.get('NAMESPACE') or '')
        if not self.db_name or not self.namespace:
            raise ValueError("SurrealDB: NAME und NAMESPACE müssen in settings.py gesetzt sein!")
        self.database = self.db_name
        if self._debug:
            print(f"[SurrealDB-DEBUG] USER={self.user} PASSWORD={self.password} HOST={self.host} PORT={self.port} NAME={self.db_name} NAMESPACE={self.namespace}")
        self.connection = None
        # Lokaler Import, um Probleme mit Typing-Fallbacks zu vermeiden
        try:
            from surrealdb import Surreal as _Surreal  # type: ignore
        except Exception as imp_err:  # pragma: no cover
            raise ImportError("Das Paket 'surrealdb' konnte nicht importiert werden. Bitte installieren Sie es (pip install surrealdb).") from imp_err
        # Protokoll optional über OPTIONS steuerbar (http/https/ws/wss)
        scheme = str((opts.get('SUR_PROTOCOL') or 'http')).lower()
        if scheme not in ('http', 'https', 'ws', 'wss'):
            scheme = 'http'
        self.db = cast(Any, _Surreal(f"{scheme}://{self.host}:{self.port}"))
        self.connected = False
        self._insert_counters: Dict[str, int] = {}
        self._pk_counters: Dict[str, int] = {}
        # Einfache In-Memory-Caches zur Beschleunigung von PK↔RID-Lookups
        self._pk_to_rids_cache: Dict[tuple[str, int], list[str]] = {}
        self._rid_to_pk_cache: Dict[str, int] = {}
        self._lock = threading.RLock()
        try:
            self._cache_max_entries = int(opts.get('SUR_CACHE_MAX_ENTRIES') or 5000)
        except Exception:
            self._cache_max_entries = 5000
        # Optionale Cache-Vorwärmung aus Settings
        try:
            cw_any_any: Any = opts.get('SUR_CACHE_WARMUP_TABLES') or []
            if isinstance(cw_any_any, (list, tuple)):
                try:
                    cw_seq: Sequence[Any] = cast(Sequence[Any], cw_any_any)
                    self._cache_warmup_tables = [str(x) for x in cw_seq]
                except Exception:
                    self._cache_warmup_tables = []
            else:
                self._cache_warmup_tables = []
        except Exception:
            self._cache_warmup_tables = []
        try:
            self._log_cache_stats = bool(opts.get('SUR_LOG_CACHE_STATS') or False)
        except Exception:
            self._log_cache_stats = False
        self.connect()

    def query(self, sql: str) -> Any:
        # Serialisiere Abfragen über einen Lock; nutze den (ggf. gewrappten) Client
        with self._lock:
            import time
            t0 = time.perf_counter()
            try:
                res = self.db.query(sql)  # type: ignore[no-any-return]
                return res
            finally:
                try:
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    if self._profile:
                        from . import metrics as _dbm
                        _dbm.record(sql if self._log_query_body else '<redacted>', dt_ms)  # type: ignore
                    if self._profile and self._slow_ms > 0 and dt_ms >= self._slow_ms:
                        body = sql if self._log_query_body else '<redacted>'
                        print(f"[SurrealDB-SLOW ≥{self._slow_ms:.0f}ms] {dt_ms:.2f} ms :: {body}")
                except Exception:
                    pass

    # --- Einfache Cache-Helper für PK↔RID-Mappings ---
    def _cache_evict_if_needed(self) -> None:
        try:
            with self._lock:
                if len(self._pk_to_rids_cache) > self._cache_max_entries or len(self._rid_to_pk_cache) > self._cache_max_entries:
                    # Simple Strategie: kompletter Reset (einfach und sicher)
                    self._pk_to_rids_cache.clear()
                    self._rid_to_pk_cache.clear()
        except Exception:
            pass

    def cache_get_pk_to_rids(self, table: str, pk: int) -> Optional[list[str]]:
        try:
            with self._lock:
                val = self._pk_to_rids_cache.get((table, int(pk)))
            if val is not None:
                try:
                    _dbm.record_cache_hit('pk_to_rids')
                except Exception:
                    pass
            else:
                try:
                    _dbm.record_cache_miss('pk_to_rids')
                except Exception:
                    pass
            return val
        except Exception:
            return None

    def cache_set_pk_to_rids(self, table: str, pk: int, rids: list[str]) -> None:
        try:
            with self._lock:
                self._pk_to_rids_cache[(table, int(pk))] = list(rids)
            self._cache_evict_if_needed()
        except Exception:
            pass

    def cache_get_pk_for_rid(self, rid: str) -> Optional[int]:
        try:
            with self._lock:
                val = self._rid_to_pk_cache.get(rid)
            if val is not None:
                try:
                    _dbm.record_cache_hit('rid_to_pk')
                except Exception:
                    pass
            else:
                try:
                    _dbm.record_cache_miss('rid_to_pk')
                except Exception:
                    pass
            return val
        except Exception:
            return None

    def cache_set_pk_for_rid(self, rid: str, pk: int) -> None:
        try:
            with self._lock:
                self._rid_to_pk_cache[rid] = int(pk)
            self._cache_evict_if_needed()
        except Exception:
            pass

    def commit(self) -> None:
        if self._debug:
            print("Transaction committed.")

    def close(self) -> None:
        if self._debug:
            print("Connection closed.")

    def rollback(self) -> None:
        if self._debug:
            print("Transaction rollback (no-op for SurrealDB).")

    # Öffentliche, sichere Inkrementierung für Fallback-Insert-Zähler
    def add_insert_counter(self, table: str) -> int:
        with self._lock:
            cur = int(self._insert_counters.get(table, 0)) + 1
            self._insert_counters[table] = cur
            return cur

    def connect(self) -> None:
        self.db.signin({"username": self.user, "password": self.password})
        self.db.use(self.namespace, self.database)
        # Monkeypatch: query()-Metriken einfangen, wenn DEBUG/SUR_PROFILE aktiv ist
        try:
            _orig_query = self.db.query  # type: ignore[attr-defined]

            def _wrapped_query(sql: str, *args: Any, **kwargs: Any):
                import time as _t
                active = _dbm.is_active() or getattr(self, '_profile', False)
                if active:
                    t0 = _t.perf_counter()
                    res = _orig_query(sql, *args, **kwargs)
                    dt = (_t.perf_counter() - t0) * 1000.0
                    # Per-Query Profiling-Ausgabe nur wenn SUR_PROFILE aktiv
                    if getattr(self, '_profile', False):
                        try:
                            print(f"[SurrealDB-PROFILE] query: {dt:.2f} ms :: {sql}")
                        except Exception:
                            pass
                    # Thread-lokale Aggregation
                    try:
                        if _dbm.is_active():
                            _dbm.record(sql, dt)
                    except Exception:
                        pass
                    if getattr(self, '_log_responses', False):
                        try:
                            print(f"[SurrealDB-DEBUG] response: {res}")
                        except Exception:
                            pass
                    return res
                else:
                    res = _orig_query(sql, *args, **kwargs)
                    if getattr(self, '_log_responses', False):
                        try:
                            print(f"[SurrealDB-DEBUG] response: {res}")
                        except Exception:
                            pass
                    return res

            # tatsächliches Wrapping (pro Connection-Objekt)
            self.db.query = _wrapped_query  # type: ignore[assignment]
        except Exception:
            # Bei Problemen mit Wrapping: normal weiterarbeiten
            pass
        self.connected = True
        if self._debug:
            print(f"Connected to SurrealDB: {self.host}:{self.port} as {self.user} (NS: {self.namespace}, DB: {self.database})")
        # Optional Caching vorwärmen
        try:
            if getattr(self, '_cache_warmup_tables', None):
                self._warmup_cache()
        except Exception:
            pass
        # Optional: Kern-Constraints sicherstellen (Unique) und offensichtliche Duplikate bereinigen
        try:
            if getattr(self, '_ensure_uniques', True):
                self._ensure_core_constraints_and_cleanup()
        except Exception:
            # Kein harter Fehler bei fehlgeschlagener Bereinigung/Index-Erstellung
            pass

    def check(self, **kwargs: Any) -> Any:
        raise NotImplementedError

    def check_field(self, field: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def cursor(self):
        return CustomDBCursor(self)

    # --- Konsistenz & Unique-Constraints ------------------------------------------------------
    def _flatten_rows(self, res: Any) -> list[Any]:
        rows: list[Any] = []
        try:
            if isinstance(res, list) and res and isinstance(res[0], dict) and ('status' in res[0] or 'result' in res[0]):
                for e in res:
                    if isinstance(e, dict) and 'result' in e and e['result'] is not None:
                        rv = e['result']
                        if isinstance(rv, list):
                            rows.extend(rv)
                        else:
                            rows.append(rv)
            elif isinstance(res, list):
                rows = res
            else:
                rows = [res]
        except Exception:
            pass
        return rows

    def _rid_to_string(self, rid_obj: Any) -> Optional[str]:
        # Unterstützt RecordID-Objekt (mit table_name/id) oder bereits String "table:rid"
        try:
            if isinstance(rid_obj, str) and ':' in rid_obj:
                return rid_obj
            tname = getattr(rid_obj, 'table_name', None)
            rid = getattr(rid_obj, 'id', None)
            if tname and rid:
                return f"{tname}:{rid}"
        except Exception:
            pass
        return None

    def _pk_from_rid(self, table: str, rid_str: str) -> Optional[int]:
        """Liefert den gemappten int-PK zu einer RID-String-ID (z. B. "django_content_type:abc")."""
        try:
            cached = self.cache_get_pk_for_rid(rid_str)
            if cached is not None:
                return int(cached)
        except Exception:
            pass
        map_tbl = f"django_pk_{table}"
        try:
            res = self.db.query(f"SELECT pk FROM {map_tbl} WHERE rid = '{rid_str}'")
            rows = self._flatten_rows(res)
            pks = [r.get('pk') for r in rows if isinstance(r, dict) and r.get('pk') is not None]
            if pks:
                try:
                    pkv = int(max(pks))
                except Exception:
                    pkv = int(pks[0])
                try:
                    self.cache_set_pk_for_rid(rid_str, int(pkv))
                except Exception:
                    pass
                return pkv
        except Exception:
            pass
        return None

    def _ensure_core_constraints_and_cleanup(self) -> None:
        """Sichert zentrale Eindeutigkeiten (z.B. ContentType(app_label, model)).
        - Bereinigt vorhandene Duplikate, behält den ersten Eintrag pro (app_label, model).
        - Definiert UNIQUE-Indizes, damit zukünftige Duplikate verhindert werden.
        """
        # 1) ContentTypes: unique(app_label, model)
        try:
            res = self.db.query("SELECT id, app_label, model FROM django_content_type")
            rows = self._flatten_rows(res)
            groups: dict[tuple[str, str], list[Any]] = {}
            for r in rows:
                if not isinstance(r, dict):
                    continue
                al = str(r.get('app_label', '')).strip()
                mo = str(r.get('model', '')).strip()
                if not al or not mo:
                    continue
                groups.setdefault((al, mo), []).append(r)
            deletes: list[str] = []
            for key, items in groups.items():  # key: (app_label, model)
                if len(items) > 1:
                    # Behalte den ersten, leite Referenzen auf diesen um
                    keep_rid = self._rid_to_string(items[0].get('id')) if isinstance(items[0], dict) else None
                    pk_keep: Optional[int] = self._pk_from_rid('django_content_type', keep_rid) if keep_rid else None
                    for dup in items[1:]:
                        dup_rid = self._rid_to_string(dup.get('id')) if isinstance(dup, dict) else None
                        pk_dup: Optional[int] = self._pk_from_rid('django_content_type', dup_rid) if dup_rid else None
                        if not dup_rid:
                            continue
                        # Referenzen in auth_permission und django_admin_log umbiegen
                        try:
                            if pk_keep is not None and pk_dup is not None and pk_keep != pk_dup:
                                self.db.query(f"UPDATE auth_permission SET content_type_id = {pk_keep} WHERE content_type_id = {pk_dup}")
                        except Exception:
                            pass
                        try:
                            if pk_keep is not None and pk_dup is not None and pk_keep != pk_dup:
                                self.db.query(f"UPDATE django_admin_log SET content_type_id = {pk_keep} WHERE content_type_id = {pk_dup}")
                        except Exception:
                            pass
                        deletes.append(dup_rid)
            # Duplikate löschen
            for rid in deletes:
                try:
                    self.db.query(f"DELETE {rid}")
                    if self._debug:
                        print(f"[SurrealDB-FIX] Deleted duplicate ContentType record: {rid}")
                except Exception:
                    # versuche fallback via WHERE
                    try:
                        self.db.query(f"DELETE FROM django_content_type WHERE id = '{rid}'")
                    except Exception:
                        pass
            # UNIQUE-Index definieren (idempotent – SurrealDB ignoriert Neu-Definition oder wir fangen Fehler ab)
            try:
                self.db.query("DEFINE INDEX uniq_contenttype_app_model ON TABLE django_content_type FIELDS app_label, model UNIQUE")
            except Exception:
                pass
        except Exception:
            pass

    def _warmup_cache(self) -> None:
        """Lädt PK↔RID-Mappings für konfigurierte Tabellen in den In‑Memory‑Cache."""
        tables = getattr(self, '_cache_warmup_tables', []) or []
        total = 0
        for tbl in tables:
            try:
                tname = str(tbl)
                map_tbl = f"django_pk_{tname}"
                res = self.db.query(f"SELECT pk, rid FROM {map_tbl}")
                rows_local: list[Any] = []
                if isinstance(res, list) and res and isinstance(res[0], dict) and ('status' in res[0] or 'result' in res[0]):
                    for e in res:
                        if isinstance(e, dict) and 'result' in e and e['result']:
                            if isinstance(e['result'], list):
                                rows_local.extend(e['result'])
                            else:
                                rows_local.append(e['result'])
                elif isinstance(res, list):
                    rows_local = res
                for r in rows_local:
                    if isinstance(r, dict):
                        pk = r.get('pk')
                        rid = r.get('rid')
                        if isinstance(pk, int) and isinstance(rid, str) and ':' in rid:
                            try:
                                self.cache_set_pk_for_rid(rid, int(pk))
                                self.cache_set_pk_to_rids(tname, int(pk), [rid])
                                total += 1
                            except Exception:
                                pass
            except Exception:
                continue
        if getattr(self, '_log_cache_stats', False):
            try:
                print(f"[SurrealDB-CACHE] warmup loaded: {total} mappings across {len(tables)} table(s)")
            except Exception:
                pass

    # Interner Helfer: liefert nächste PK für eine Mapping-Tabelle
    def next_pk(self, map_tbl: str) -> int:  # NOSONAR - bewusst kompakt, aber leicht verzweigt
        with self._lock:
            cur = self._pk_counters.get(map_tbl)
            if cur is None:
                mx_val = 0
                try:
                    res_mx = self.db.query(f"SELECT pk FROM {map_tbl} ORDER BY pk DESC LIMIT 1")
                    rows_local: list[Any] = []
                    if isinstance(res_mx, list) and res_mx and isinstance(res_mx[0], dict) and ('status' in res_mx[0] or 'result' in res_mx[0]):
                        for e in res_mx:
                            if isinstance(e, dict) and 'result' in e and e['result']:
                                if isinstance(e['result'], list):
                                    rows_local.extend(e['result'])
                                else:
                                    rows_local.append(e['result'])
                    elif isinstance(res_mx, list):
                        rows_local = res_mx
                    if rows_local and isinstance(rows_local[0], dict) and rows_local[0].get('pk') is not None:
                        mx_val = int(rows_local[0]['pk'])
                except Exception:
                    mx_val = 0
                cur = mx_val
            cur = int(cur) + 1
            self._pk_counters[map_tbl] = cur
            return cur


class CustomDBCursor:
    def __init__(self, connection: CustomDBConnection):
        self.connection: CustomDBConnection = connection
        self.lastrowid: Optional[int] = None
        self._results: List[Any] = []
        self._result_index: int = 0
        self.description: Optional[List[Tuple[Any, Any, Any, Any, Any, Any, Any]]] = None
        # DB-API 2.0: Anzahl betroffener Zeilen; für SELECT üblicherweise -1
        self.rowcount = -1

    # --- Interne Helper für Übersetzungen (nur Struktur, Verhalten unverändert) ---
    def _fmt_param(self, p: Any) -> str:
        # Spezialformate zuerst: Datum/Zeit sauber für SurrealQL formatieren
        try:
            from datetime import datetime, date, timezone as _tz
            if isinstance(p, datetime):
                # ISO-8601 in UTC; Surreal-freundlich via time::parse('...')
                if p.tzinfo is None:
                    p = p.replace(tzinfo=_tz.utc)
                else:
                    p = p.astimezone(_tz.utc)
                iso = p.isoformat()
                # Kompakt als Z-Suffix darstellen
                if iso.endswith('+00:00'):
                    iso = iso[:-6] + 'Z'
                return f"time::parse('{iso}')"
            if isinstance(p, date):
                # Als Mitternacht UTC interpretieren
                dt = datetime(p.year, p.month, p.day, tzinfo=_tz.utc)
                iso = dt.isoformat().replace('+00:00', 'Z')
                return f"time::parse('{iso}')"
        except Exception:
            pass
        if isinstance(p, bool):
            return 'true' if p else 'false'
        if p is None:
            return 'NULL'
        if isinstance(p, int) and abs(p) > 2**53:
            return str(p)
        if isinstance(p, (list, tuple)):
            inner = ', '.join(self._fmt_param(x) for x in p)
            return f'[{inner}]'
        if isinstance(p, str):
            return "'" + p.replace("'", "''") + "'"
        return repr(p)

    def _apply_basic_transforms(self, sql: str) -> str:
        import re
        q = sql
        # IN (..)->[..]
        q = re.sub(r'\bIN\s*\(([^()]+)\)', r'IN [\1]', q, flags=re.IGNORECASE)
        # COUNT(*)/COUNT(1)->count()
        q = re.sub(r'\bCOUNT\s*\(\s*\*\s*\)', COUNT_FUNC, q, flags=re.IGNORECASE)
        q = re.sub(r'\bCOUNT\s*\(\s*1\s*\)', COUNT_FUNC, q, flags=re.IGNORECASE)
        # COUNT(<expr>)->count() (vereinheitlichen)
        q = re.sub(r'\bCOUNT\s*\(\s*[^)]*\)', COUNT_FUNC, q, flags=re.IGNORECASE)
        # OFFSET->START
        q = re.sub(r'\bOFFSET\s+(\d+)\b', r'START \1', q, flags=re.IGNORECASE)
        # Backticks entfernen
        q = q.replace('`', '')
        # Doppelte Anführungszeichen (Identifier-Quotes) entfernen
        q = q.replace('"', '')
        # Tabellenqualifizierer entfernen, aber NICHT innerhalb von String-Literalen
        # Wir splitten an einfachen Quotes und bearbeiten nur Segmente außerhalb von Strings
        parts = re.split(r"('(?:''|[^'])*')", q)
        for i in range(0, len(parts), 2):  # nur außerhalb von Strings
            parts[i] = re.sub(r'\b([A-Za-z_][\w]*)\.([A-Za-z_@][\w]*)\b', r'\2', parts[i])
        q = ''.join(parts)
        return q

    def _extract_result_rows(self, raw: Any) -> list[Any]:  # NOSONAR - Struktur orientiert sich an API-Formaten
        rows: list[Any] = []
        if isinstance(raw, list):
            if raw and isinstance(raw[0], dict) and ('status' in raw[0] or 'result' in raw[0]):
                for entry in raw:
                    if isinstance(entry, dict):
                        status = entry.get('status')
                        if status and status != 'OK':
                            detail = entry.get('detail') or entry.get('message') or str(entry)
                            raise DatabaseWrapper.OperationalError(f'SurrealDB-Fehler: {detail}')
                        if 'result' in entry and entry['result'] is not None:
                            rv = entry['result']
                            if isinstance(rv, list):
                                rows.extend(rv)
                            else:
                                rows.append(rv)
            else:
                rows = raw
        else:
            rows = [raw]
        return rows

    def _normalize_select_rows(self, rows: list[Any], distinct_flag: bool, select_cols: Optional[List[str]] = None) -> list[Any]:  # NOSONAR - bewusst detaillierte Normalisierung
        # '@id' nach 'id' spiegeln
        if rows and isinstance(rows[0], dict):
            for _r in rows:
                try:
                    if isinstance(_r, dict) and '@id' in _r and 'id' not in _r:
                        _r['id'] = _r['@id']
                except Exception:
                    pass
            # Spaltenreihenfolge: wenn SELECT-Liste bekannt, diese nutzen; sonst aus erster Zeile ableiten
            cols = select_cols if select_cols else list(rows[0].keys())
            self.description = [(c, None, None, None, None, None, None) for c in cols]

            # Prefetch: sammle alle RID-Strings pro Tabelle, die noch nicht im Cache sind,
            # und hole ihre PKs in einem Schwung aus den Mapping-Tabellen.
            prefetch_map: Dict[str, int] = {}
            table_to_rids: Dict[str, set[str]] = {}
            try:
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    for c in cols:
                        try:
                            v = row.get(c)
                        except Exception:
                            v = None
                        tname = getattr(v, 'table_name', None)
                        rid = getattr(v, 'id', None)
                        if tname and rid:
                            rid_str = f"{tname}:{rid}"
                            # schon im Cache?
                            cached_pk = self.connection.cache_get_pk_for_rid(rid_str)
                            if cached_pk is not None:
                                prefetch_map[rid_str] = int(cached_pk)
                                continue
                            s = table_to_rids.setdefault(str(tname), set())
                            s.add(rid_str)
                # Bulk-Queries je Tabelle
                for tname, ridset in table_to_rids.items():
                    if not ridset:
                        continue
                    map_tbl = f"django_pk_{tname}"
                    # baue Liste 'rid' Literale: ['table:rid'] als Surreal String-Liste
                    values = ', '.join("'" + r.replace("'", "''") + "'" for r in ridset)
                    try:
                        mp = self.connection.db.query(f"SELECT rid, pk FROM {map_tbl} WHERE rid IN [{values}]")
                        rows_local: list[Any] = []
                        if isinstance(mp, list) and mp and isinstance(mp[0], dict) and ('status' in mp[0] or 'result' in mp[0]):
                            for e in mp:
                                if isinstance(e, dict) and 'result' in e and e['result']:
                                    if isinstance(e['result'], list):
                                        rows_local.extend(e['result'])
                                    else:
                                        rows_local.append(e['result'])
                        elif isinstance(mp, list):
                            rows_local = mp
                        for r in rows_local:
                            if isinstance(r, dict):
                                ridv = r.get('rid')
                                pkv = r.get('pk')
                                if isinstance(ridv, str) and isinstance(pkv, int):
                                    prefetch_map[ridv] = pkv
                                    try:
                                        self.connection.cache_set_pk_for_rid(ridv, int(pkv))
                                        self.connection.cache_set_pk_to_rids(tname, int(pkv), [ridv])
                                    except Exception:
                                        pass
                    except Exception:
                        # Prefetch ist nur eine Optimierung – sicher ignorieren
                        pass
            except Exception:
                pass

            def norm(v):
                tname = getattr(v, 'table_name', None)
                rid = getattr(v, 'id', None)
                if tname and rid:
                    rid_str = f"{tname}:{rid}"
                    # Prefetch-Ergebnis zuerst verwenden
                    try:
                        if rid_str in prefetch_map:
                            return int(prefetch_map[rid_str])
                    except Exception:
                        pass
                    # Zuerst Cache prüfen (RID -> PK)
                    try:
                        cached_pk = self.connection.cache_get_pk_for_rid(rid_str)
                        if cached_pk is not None:
                            return int(cached_pk)
                    except Exception:
                        pass
                    map_tbl = f"django_pk_{tname}"
                    try:
                        q = f"SELECT pk FROM {map_tbl} WHERE rid = '{rid_str}'"
                        mp = self.connection.db.query(q)
                        rows_local = []
                        if isinstance(mp, list) and mp and isinstance(mp[0], dict) and ('status' in mp[0] or 'result' in mp[0]):
                            for e in mp:
                                if isinstance(e, dict) and 'result' in e and e['result']:
                                    if isinstance(e['result'], list):
                                        rows_local.extend(e['result'])
                                    else:
                                        rows_local.append(e['result'])
                        elif isinstance(mp, list):
                            rows_local = mp
                        if rows_local and isinstance(rows_local[0], dict):
                            pks = [r.get('pk') for r in rows_local if isinstance(r, dict) and r.get('pk') is not None]
                            if pks:
                                try:
                                    pk_val = int(max(pks))
                                    # Cache auffrischen (beide Richtungen)
                                    try:
                                        self.connection.cache_set_pk_for_rid(rid_str, pk_val)
                                        self.connection.cache_set_pk_to_rids(tname, pk_val, [rid_str])
                                    except Exception:
                                        pass
                                    return pk_val
                                except Exception:
                                    return pks[0]
                            new_id = self.connection.next_pk(map_tbl)
                            try:
                                self.connection.db.query(f"UPDATE {map_tbl} SET pk = {new_id} WHERE rid = '{rid_str}'")
                            except Exception:
                                pass
                            try:
                                self.connection.cache_set_pk_for_rid(rid_str, int(new_id))
                                self.connection.cache_set_pk_to_rids(tname, int(new_id), [rid_str])
                            except Exception:
                                pass
                            return new_id
                        new_id = self.connection.next_pk(map_tbl)
                        try:
                            self.connection.db.query(f"UPDATE {map_tbl} SET pk = {new_id} WHERE rid = '{rid_str}'")
                        except Exception:
                            pass
                        try:
                            self.connection.db.query(f"CREATE {map_tbl} CONTENT {{ rid: '{rid_str}', pk: {new_id} }}")
                        except Exception:
                            pass
                        try:
                            self.connection.cache_set_pk_for_rid(rid_str, int(new_id))
                            self.connection.cache_set_pk_to_rids(tname, int(new_id), [rid_str])
                        except Exception:
                            pass
                        return new_id
                    except Exception:
                        return rid_str
                return v

            # Spezialfall: Migrationstabelle – vermeide PK-Mapping und erzeuge fortlaufende int-IDs
            is_migration_like = set(['app', 'name', 'applied']).issubset(set(cols)) and 'id' in cols
            tuple_rows = []
            if is_migration_like:
                id_idx = cols.index('id')
                for i, row in enumerate(rows):
                    vals = []
                    for c in cols:
                        if c == 'id':
                            vals.append(i + 1)  # stabile, fortlaufende int-IDs
                        else:
                            vals.append(norm(row.get(c)))
                    tuple_rows.append(tuple(vals))
            else:
                tuple_rows = [tuple(norm(row.get(c)) for c in cols) for row in rows]
            if distinct_flag:
                seen, deduped = set(), []
                for row in tuple_rows:
                    if row not in seen:
                        seen.add(row)
                        deduped.append(row)
                return deduped
            return tuple_rows
        # Kein dict-basiertes Ergebnis
        return rows

    def _parse_select_columns(self, sql: str) -> Optional[List[str]]:
        """Extrahiert die Spaltenliste zwischen SELECT und FROM und liefert die Ergebnis-Spaltennamen
        (berücksichtigt AS-Aliase; entfernt Qualifizierer). Bei Fehlern: None.
        """
        import re
        m = re.match(r'^\s*select\s+(?P<cols>.+?)\s+from\s', sql, flags=re.IGNORECASE)
        if not m:
            return None
        cols_raw = m.group('cols')
        # DISTINCT wurde ggf. vorher entfernt; falls noch vorhanden, entfernen
        cols_raw = re.sub(r'^\s*distinct\s+', '', cols_raw, flags=re.IGNORECASE)
        # Zerlege per Komma unter Beachtung einfacher Klammern/Strings
        parts = []
        buf = []
        depth_round = 0
        in_single = False
        esc = False
        for ch in cols_raw:
            if in_single:
                buf.append(ch)
                if ch == "'" and not esc:
                    in_single = False
                esc = (ch == "'" and not esc)
                if ch != "'":
                    esc = False
                continue
            if ch == "'":
                in_single = True
                buf.append(ch)
                continue
            if ch == '(':
                depth_round += 1
            elif ch == ')':
                depth_round = max(0, depth_round - 1)
            if ch == ',' and depth_round == 0:
                parts.append(''.join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            parts.append(''.join(buf).strip())

        def out_name(expr: str) -> str:
            # Falls AS alias: nimm alias; sonst letzte Identifier-Komponente
            expr2 = expr
            m_as = re.search(r'\bas\s+([A-Za-z_][\w]*)\s*$', expr2, flags=re.IGNORECASE)
            if m_as:
                return m_as.group(1)
            # Entferne Qualifizierer a.b -> b
            expr2 = re.sub(r'"', '', expr2)
            expr2 = re.sub(r'`', '', expr2)
            expr2 = re.sub(r'.*\.', '', expr2)
            return expr2.strip()

        cols = [out_name(p) for p in parts]
        return cols if cols else None

    def close(self) -> None:
        return None

    def fetchmany(self, size: Optional[int] = None) -> list:
        if size is None:
            size = len(self._results) - self._result_index
        if self._result_index >= len(self._results):
            return []
        end = min(self._result_index + size, len(self._results))
        res = self._results[self._result_index:end]
        self._result_index = end
        return res

    def fetchall(self) -> list:
        return self._results

    def fetchone(self) -> Optional[Any]:
        if self._result_index >= len(self._results):
            return None
        row = self._results[self._result_index]
        self._result_index += 1
        return row

    def executemany(self, query: str, param_list: Sequence[Sequence[Any]]):
        results: List[Any] = []
        for params in (param_list or []):
            self.execute(query, params)
            results.append(self._results)
        return results

    def execute(self, query: str, params: Optional[Sequence[Any]] = None):  # noqa: C901  # NOSONAR
        import re
        import time
        surreal_query = str(query)
        if getattr(self.connection, '_log_queries', False):
            try:
                print(f"[SurrealDB-DEBUG] SQL in: {query} params={params}")
            except Exception:
                pass
        # Emulation: einfache SELECT-Konstante wie "SELECT 1" oder "SELECT 1 AS one"
        m_sel_const = re.match(r"^\s*select\s+(-?\d+)\s*(?:as\s+([A-Za-z_][\w]*))?\s*;?\s*$", surreal_query, flags=re.IGNORECASE)
        if m_sel_const and not params:
            val = int(m_sel_const.group(1))
            alias = m_sel_const.group(2) or str(val)
            self.description = [(alias, None, None, None, None, None, None)]
            self._results = [(val,)]
            self._result_index = 0
            self.rowcount = -1
            if getattr(self.connection, '_log_queries', False):
                try:
                    print(f"[SurrealDB-DEBUG] SQL out: <emulated SELECT const>")
                except Exception:
                    pass
            return
        # DISTINCT erkennen und später clientseitig deduplizieren
        distinct_flag = False
        if re.match(r'^\s*SELECT\s+DISTINCT\b', surreal_query, flags=re.IGNORECASE):
            distinct_flag = True
            surreal_query = re.sub(r'^\s*SELECT\s+DISTINCT\b', 'SELECT', surreal_query, flags=re.IGNORECASE)
        # RETURNING-Klauseln entfernen (nicht unterstützt)
        surreal_query = re.sub(r'\bRETURNING\b\s+[^,;\n]+', '', surreal_query, flags=re.IGNORECASE)

        # Parameter einfügen (%s → literal)
        if params:
            for p in params:
                surreal_query = surreal_query.replace('%s', self._fmt_param(p), 1)

        # Basis-Übersetzungen anwenden
        surreal_query = self._apply_basic_transforms(surreal_query)

        # ORDER BY <position> (z. B. ORDER BY 1) → ORDER BY <spaltenname>
        # Einige SQL-Dialekte erlauben positionsbasierte Sortierung; SurrealDB erwartet Spaltennamen.
        try:
            select_cols_for_ob = self._parse_select_columns(surreal_query)
        except Exception:
            select_cols_for_ob = None
        if select_cols_for_ob:
            def _split_order_terms(expr: str) -> list[str]:
                parts: list[str] = []
                buf: list[str] = []
                depth_round = 0
                in_single = False
                esc = False
                for ch in expr:
                    if in_single:
                        buf.append(ch)
                        if ch == "'" and not esc:
                            in_single = False
                        esc = (ch == "'" and not esc)
                        if ch != "'":
                            esc = False
                        continue
                    if ch == "'":
                        in_single = True
                        buf.append(ch)
                        continue
                    if ch == '(':
                        depth_round += 1
                    elif ch == ')':
                        depth_round = max(0, depth_round - 1)
                    if ch == ',' and depth_round == 0:
                        parts.append(''.join(buf).strip())
                        buf = []
                    else:
                        buf.append(ch)
                if buf:
                    parts.append(''.join(buf).strip())
                return parts

            import re as _re_ob
            def _rewrite_order_by(m: Any) -> str:  # type: ignore[no-redef]
                list_part = m.group(1)
                tail = m.group(2) or ''
                terms = _split_order_terms(list_part)
                new_terms: list[str] = []
                for t in terms:
                    mt = _re_ob.match(r"^\s*(\d+)\s*(ASC|DESC)?\s*$", t, flags=_re_ob.IGNORECASE)
                    if mt:
                        try:
                            idx = int(mt.group(1))
                        except Exception:
                            new_terms.append(t)
                            continue
                        direction = (mt.group(2) or '').upper()
                        if 1 <= idx <= len(select_cols_for_ob):
                            colname = select_cols_for_ob[idx - 1]
                            repl = f"{colname} {direction}".strip()
                            new_terms.append(repl)
                        else:
                            new_terms.append(t)
                    else:
                        # NULLS FIRST/LAST Emulation: wir entfernen es aus SQL, merken es aber in Klammern
                        mt_nulls = _re_ob.match(r"^(.+?)\s+NULLS\s+(FIRST|LAST)\s*$", t, flags=_re_ob.IGNORECASE)
                        if mt_nulls:
                            base = mt_nulls.group(1).strip()
                            pos = mt_nulls.group(2).upper()
                            new_terms.append(f"{base} /*NULLS {pos}*/")
                        else:
                            new_terms.append(t)
                return "ORDER BY " + ', '.join(new_terms) + tail

            surreal_query = _re_ob.sub(
                r"(?i)\border\s+by\s+(.+?)(\s+limit\b|\s+start\b|\s+fetch\b|$)",
                _rewrite_order_by,
                surreal_query,
            )

        # PK-Mapping: id = <int> oder id IN [<ints>] in RecordID-Vergleiche umschreiben
        # Unterstützt Muster wie: SELECT ... FROM <tbl> WHERE id = 1 [LIMIT ...]
        # und:                    SELECT ... FROM <tbl> WHERE id IN [1,2,3] [LIMIT ...]
        def _map_pk_to_rids(tbl: str, pk_val: int) -> list[str]:
            map_tbl = f"django_pk_{tbl}"
            out: list[str] = []
            # Cache-Hit zuerst prüfen
            try:
                cached = self.connection.cache_get_pk_to_rids(tbl, int(pk_val))
                if cached:
                    return list(cached)
            except Exception:
                pass
            try:
                res = self.connection.db.query(f"SELECT rid FROM {map_tbl} WHERE pk = {int(pk_val)}")
                rows_local: list[Any] = []
                if isinstance(res, list) and res and isinstance(res[0], dict) and ('status' in res[0] or 'result' in res[0]):
                    for e in cast(List[Dict[str, Any]], res):
                        if 'result' in e and e['result']:
                            if isinstance(e['result'], list):
                                rows_local.extend(cast(List[Any], e['result']))
                            else:
                                rows_local.append(e['result'])
                elif isinstance(res, list):
                    rows_local = cast(List[Any], res)
                for r in rows_local:
                    if isinstance(r, dict):
                        rid = r.get('rid')
                        if isinstance(rid, str) and ':' in rid:
                            out.append(rid)
            except Exception:
                pass
            # Deduplizieren, Reihenfolge stabil lassen
            seen: set[str] = set()
            uniq: list[str] = []
            for rid in out:
                if rid not in seen:
                    seen.add(rid)
                    uniq.append(rid)
            # Cache aktualisieren
            try:
                self.connection.cache_set_pk_to_rids(tbl, int(pk_val), uniq)
            except Exception:
                pass
            return uniq

        def _map_pks_to_rids_bulk(tbl: str, pk_values: list[int]) -> dict[int, list[str]]:
            """Batch-Lookup für mehrere PKs → RIDs mit Cache-Nutzung."""
            result: dict[int, list[str]] = {}
            missing: list[int] = []
            # Zuerst Cache auswerten
            for pk in pk_values:
                cached = self.connection.cache_get_pk_to_rids(tbl, int(pk))
                if cached is not None:
                    result[int(pk)] = list(cached)
                else:
                    missing.append(int(pk))
            if not missing:
                return result
            map_tbl = f"django_pk_{tbl}"
            try:
                plist = ', '.join(str(int(p)) for p in missing)
                res = self.connection.db.query(f"SELECT pk, rid FROM {map_tbl} WHERE pk IN [{plist}]")
                rows_local: list[Any] = []
                if isinstance(res, list) and res and isinstance(res[0], dict) and ('status' in res[0] or 'result' in res[0]):
                    for e in res:
                        if isinstance(e, dict) and 'result' in e and e['result']:
                            if isinstance(e['result'], list):
                                rows_local.extend(e['result'])
                            else:
                                rows_local.append(e['result'])
                elif isinstance(res, list):
                    rows_local = res
                # Sammeln
                for r in rows_local:
                    if isinstance(r, dict):
                        pk = r.get('pk')
                        rid = r.get('rid')
                        if isinstance(pk, int) and isinstance(rid, str) and ':' in rid:
                            result.setdefault(pk, []).append(rid)
                # Cache für alle befüllen (auch leere Listen, um künftige Misses zu vermeiden)
                for pk in missing:
                    rid_list = result.get(pk, [])
                    try:
                        self.connection.cache_set_pk_to_rids(tbl, int(pk), list(rid_list))
                    except Exception:
                        pass
            except Exception:
                # Bei Fehlern: Falls etwas im Cache war, gib das zurück; Rest bleibt leer
                for pk in missing:
                    result.setdefault(pk, [])
            return result

        # id = <int>
        m_id_eq = re.search(r'^\s*select\s+.+?\s+from\s+([A-Za-z_][\w]*)\s+where\s+id\s*=\s*(\d+)\b', surreal_query, flags=re.IGNORECASE)
        if m_id_eq:
            tbl = m_id_eq.group(1)
            pk = int(m_id_eq.group(2))
            rids = _map_pk_to_rids(tbl, pk)
            if rids:
                rid_list = ', '.join(rids)
                surreal_query = re.sub(
                    r'\bwhere\s+id\s*=\s*\d+\b',
                    lambda m: f'WHERE id IN [{rid_list}]',
                    surreal_query,
                    flags=re.IGNORECASE,
                )

        # id IN [<ints>]
        m_id_in = re.search(r'^\s*select\s+.+?\s+from\s+([A-Za-z_][\w]*)\s+where\s+id\s+in\s*\[([^\]]*)\]', surreal_query, flags=re.IGNORECASE)
        if m_id_in:
            tbl = m_id_in.group(1)
            raw_list = m_id_in.group(2)
            int_items: list[int] = []
            for part in raw_list.split(','):
                p = part.strip()
                if p.isdigit():
                    int_items.append(int(p))
            rid_items: list[str] = []
            if int_items:
                bulk_map = _map_pks_to_rids_bulk(tbl, int_items)
                # Preserve originale PK-Reihenfolge
                for v in int_items:
                    rid_items.extend(bulk_map.get(int(v), []))
            if rid_items:
                # dedupe
                seen: set[str] = set()
                uniq: list[str] = []
                for x in rid_items:
                    if x not in seen:
                        seen.add(x)
                        uniq.append(x)
                rid_list = ', '.join(uniq)
                surreal_query = re.sub(
                    r'(\bwhere\s+id\s+in\s*)\[[^\]]*\]',
                    lambda m: m.group(1) + f'[{rid_list}]',
                    surreal_query,
                    flags=re.IGNORECASE,
                )

        # UPDATE <tbl> SET ... WHERE id = <int>  →  UPDATE <rid> SET ... (oder WHERE id IN [...])
        m_upd_eq = re.search(r'^\s*update\s+"?([A-Za-z_][\w]*)"?\s+set\s+(.+?)\s+where\s+id\s*=\s*(\d+)\b', surreal_query, flags=re.IGNORECASE)
        if m_upd_eq:
            tbl = m_upd_eq.group(1)
            set_part = m_upd_eq.group(2)
            pk = int(m_upd_eq.group(3))
            rids = _map_pk_to_rids(tbl, pk)
            if rids:
                if len(rids) == 1:
                    surreal_query = f"UPDATE {rids[0]} SET {set_part}"
                else:
                    rid_list = ', '.join(rids)
                    surreal_query = f"UPDATE {tbl} SET {set_part} WHERE id IN [{rid_list}]"

        # UPDATE <tbl> SET ... WHERE id IN [<ints>]  →  mappe zu RID-Liste
        m_upd_in = re.search(r'^\s*update\s+"?([A-Za-z_][\w]*)"?\s+set\s+(.+?)\s+where\s+id\s+in\s*\[([^\]]*)\]', surreal_query, flags=re.IGNORECASE)
        if m_upd_in:
            tbl = m_upd_in.group(1)
            set_part = m_upd_in.group(2)
            raw_list = m_upd_in.group(3)
            pks: list[int] = []
            for part in raw_list.split(','):
                s = part.strip()
                if s.isdigit():
                    pks.append(int(s))
            rid_items: list[str] = []
            if pks:
                bulk_map = _map_pks_to_rids_bulk(tbl, pks)
                for v in pks:
                    rid_items.extend(bulk_map.get(int(v), []))
            if rid_items:
                seen: set[str] = set()
                uniq: list[str] = []
                for x in rid_items:
                    if x not in seen:
                        seen.add(x)
                        uniq.append(x)
                rid_list = ', '.join(uniq)
                surreal_query = f"UPDATE {tbl} SET {set_part} WHERE id IN [{rid_list}]"

        # DELETE FROM <tbl> WHERE id = <int> / IN [<ints>]  →  mappe zu RID(s)
        m_del_eq = re.search(r'^\s*delete\s+from\s+"?([A-Za-z_][\w]*)"?\s+where\s+id\s*=\s*(\d+)\b', surreal_query, flags=re.IGNORECASE)
        if m_del_eq:
            tbl = m_del_eq.group(1)
            pk = int(m_del_eq.group(2))
            rids = _map_pk_to_rids(tbl, pk)
            if rids:
                if len(rids) == 1:
                    surreal_query = f"DELETE {rids[0]}"
                else:
                    rid_list = ', '.join(rids)
                    surreal_query = f"DELETE FROM {tbl} WHERE id IN [{rid_list}]"

        m_del_in = re.search(r'^\s*delete\s+from\s+"?([A-Za-z_][\w]*)"?\s+where\s+id\s+in\s*\[([^\]]*)\]', surreal_query, flags=re.IGNORECASE)
        if m_del_in:
            tbl = m_del_in.group(1)
            raw_list = m_del_in.group(2)
            pks: list[int] = []
            for part in raw_list.split(','):
                s = part.strip()
                if s.isdigit():
                    pks.append(int(s))
            rid_items: list[str] = []
            if pks:
                bulk_map = _map_pks_to_rids_bulk(tbl, pks)
                for v in pks:
                    rid_items.extend(bulk_map.get(int(v), []))
            if rid_items:
                seen: set[str] = set()
                uniq: list[str] = []
                for x in rid_items:
                    if x not in seen:
                        seen.add(x)
                        uniq.append(x)
                rid_list = ', '.join(uniq)
                surreal_query = f"DELETE FROM {tbl} WHERE id IN [{rid_list}]"
        if getattr(self.connection, '_log_queries', False):
            try:
                print(f"[SurrealDB-DEBUG] SQL out: {surreal_query}")
            except Exception:
                pass

        # INSERT INTO <t> (a,b) VALUES (x,y) -> CREATE <t> CONTENT { a: x, b: y }
        def split_csv(expr: str) -> list[str]:
            parts = []
            buf = []
            depth_round = 0
            depth_square = 0
            in_single = False
            esc = False
            for ch in expr:
                if in_single:
                    buf.append(ch)
                    if ch == "'" and not esc:
                        in_single = False
                    esc = (ch == "'" and not esc)
                    if ch != "'":
                        esc = False
                    continue
                if ch == "'":
                    in_single = True
                    buf.append(ch)
                    continue
                if ch == '(':
                    depth_round += 1
                elif ch == ')':
                    depth_round = max(0, depth_round - 1)
                elif ch == '[':
                    depth_square += 1
                elif ch == ']':
                    depth_square = max(0, depth_square - 1)
                if ch == ',' and depth_round == 0 and depth_square == 0:
                    parts.append(''.join(buf).strip())
                    buf = []
                else:
                    buf.append(ch)
            if buf:
                parts.append(''.join(buf).strip())
            return parts

        m_ins = re.search(r'^\s*INSERT\s+INTO\s+"?([\w]+)"?\s*\(([^)]+)\)\s*VALUES\s*\((.+)\)\s*;?\s*$', surreal_query, flags=re.IGNORECASE)
        if m_ins:
            tbl = m_ins.group(1)
            cols_raw = m_ins.group(2)
            vals_raw = m_ins.group(3)
            cols = [c.strip().strip('"') for c in split_csv(cols_raw)]
            vals = split_csv(vals_raw)
            if len(cols) == len(vals):
                content_inner = ', '.join(f"{cols[i]}: {vals[i]}" for i in range(len(cols)))
                surreal_query = f"CREATE {tbl} CONTENT {{ {content_inner} }}"

        # Einfache JOIN-Emulation (INNER JOIN ... ON (...))
        if re.search(r'\bfrom\s+[`"\w]+\s+inner\s+join\b', surreal_query, flags=re.IGNORECASE):
            m = re.search(r'from\s+([`"\w]+)\s+inner\s+join\s+([`"\w]+)\s+on\s+\(([^)]+)\)', surreal_query, flags=re.IGNORECASE)
            if m:
                t1 = m.group(1).strip('`"')
                t2 = m.group(2).strip('`"')
                res1 = self.connection.db.query(f'SELECT * FROM {t1}')
                res2 = self.connection.db.query(f'SELECT * FROM {t2}')

                def extract(res):
                    out = []
                    if isinstance(res, list):
                        for r in res:
                            if isinstance(r, dict) and 'result' in r:
                                rv = r['result']
                                if isinstance(rv, list):
                                    out.extend(rv)
                                elif rv is not None:
                                    out.append(rv)
                    return out

                rows1 = extract(res1)
                rows2 = extract(res2)
                cond = m.group(3)
                # Zuerst versuchen wir qualifizierte Spalten (t1.a = t2.b)
                on_parts = re.findall(r'([`"\w]+)\.([`"\w]+)\s*=\s*([`"\w]+)\.([`"\w]+)', cond)
                left_col = right_col = None
                if on_parts:
                    _, left_col, _, right_col = on_parts[0]
                    left_col = left_col.strip('`"')
                    right_col = right_col.strip('`"')
                else:
                    # Fallback: unqualifiziertes Muster (a = b)
                    m_simple = re.findall(r'([`"\w]+)\s*=\s*([`"\w]+)', cond)
                    if m_simple:
                        cand_left, cand_right = m_simple[0]
                        cand_left = cand_left.strip('`"')
                        cand_right = cand_right.strip('`"')
                        # Heuristik: ordne anhand der vorhandenen Keys in t1/t2 zu
                        keys1 = set(rows1[0].keys()) if rows1 and isinstance(rows1[0], dict) else set()
                        keys2 = set(rows2[0].keys()) if rows2 and isinstance(rows2[0], dict) else set()
                        if cand_left in keys1 and cand_right in keys2:
                            left_col, right_col = cand_left, cand_right
                        elif cand_left in keys2 and cand_right in keys1:
                            left_col, right_col = cand_right, cand_left
                        else:
                            # Ambiguität: nehme Reihenfolge an (t1.cand_left = t2.cand_right)
                            left_col, right_col = cand_left, cand_right
                if left_col and right_col:
                    joined = []
                    for r1 in rows1:
                        for r2 in rows2:
                            if isinstance(r1, dict) and isinstance(r2, dict) and r1.get(left_col) == r2.get(right_col):
                                joined.append({**r1, **r2})
                    # Ergebnisse wie bei SELECT liefern
                    if joined:
                        cols = list(joined[0].keys())
                        self.description = [(c, None, None, None, None, None, None) for c in cols]
                        self._results = [tuple(row.get(c) for c in cols) for row in joined]
                    else:
                        self.description = None
                        self._results = []
                    self._result_index = 0
                    self.rowcount = -1
                    return

        # Sonderfall: einfaches Aggregat SELECT count() FROM <t> [AS alias]
        # Alias darf mit Unterstrich beginnen (Django nutzt z.B. "__count")
        pattern_cnt = rf'^\s*select\s+{re.escape(COUNT_FUNC)}\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*))?\s+from\s+([A-Za-z][A-Za-z0-9_]*)\s*;?\s*$'
        m_cnt_simple = re.match(pattern_cnt, surreal_query, flags=re.IGNORECASE)
        if m_cnt_simple:
            alias = m_cnt_simple.group(1) or 'count'
            tbl = m_cnt_simple.group(2)
            # Zähle clientseitig, um Dialektunterschiede zu umgehen
            res: Any = self.connection.db.query(f'SELECT * FROM {tbl}')
            total = 0
            if isinstance(res, list):
                res_list = cast(List[Any], res)
                if res_list and isinstance(res_list[0], dict) and ('status' in res_list[0] or 'result' in res_list[0]):
                    for e in res_list:
                        rv = e.get('result') if isinstance(e, dict) else None
                        if isinstance(rv, list):
                            total += len(rv)
                        elif rv is not None:
                            total += 1
                else:
                    total = len(res_list)
            else:
                total = 1 if res is not None else 0
            self.description = [(alias, None, None, None, None, None, None)]
            self._results = [(total,)]
            self._result_index = 0
            self.rowcount = -1
            return

        # Sonderfall: SELECT count() FROM <t> WHERE ...  → clientseitig zählen mit gleicher WHERE
        m_cnt_where = re.match(
            rf'^\s*select\s+{re.escape(COUNT_FUNC)}\s*(?:as\s+([A-Za-z_][A-Za-z0-9_]*))?\s+from\s+([A-Za-z_][\w]*)\s+(where\s+.+?)\s*;?\s*$',
            surreal_query,
            flags=re.IGNORECASE,
        )
        if m_cnt_where:
            alias = m_cnt_where.group(1) or 'count'
            tbl = m_cnt_where.group(2)
            tail = m_cnt_where.group(3)
            # Nur die WHERE-Klausel extrahieren (ORDER BY/LIMIT/START etc. entfernen)
            m_where_only = re.match(r'^(where\s+.+?)(\s+order\s+by\b|\s+limit\b|\s+start\b|\s+fetch\b|$)', tail, flags=re.IGNORECASE)
            where_part = m_where_only.group(1) if m_where_only else tail
            sel = f"SELECT * FROM {tbl} {where_part}"
            res_any: Any = self.connection.db.query(sel)
            rows_list = self._extract_result_rows(res_any) or []
            total = len(rows_list)
            self.description = [(alias, None, None, None, None, None, None)]
            self._results = [(total,)]
            self._result_index = 0
            self.rowcount = -1
            return

        # Sonderfall: einfache Aggregat-Emulation SUM/AVG/MIN/MAX
        m_aggr = re.match(
            r"^\s*select\s+(sum|avg|min|max)\(\s*([A-Za-z_][\w]*)\s*\)\s*(?:as\s+([A-Za-z_][\w]*))?\s+from\s+([A-Za-z_][\w]*)\s*(?:(where\s+.+?))?\s*;?\s*$",
            surreal_query,
            flags=re.IGNORECASE,
        )
        if m_aggr:
            func = m_aggr.group(1).lower()
            col = m_aggr.group(2)
            alias = m_aggr.group(3) or func
            tbl = m_aggr.group(4)
            tail = m_aggr.group(5) or ''
            # WHERE-Klausel extrahieren ohne ORDER/LIMIT/START
            where_part = ''
            if tail:
                m_where_only = re.match(r'^(where\s+.+?)(\s+order\s+by\b|\s+limit\b|\s+start\b|\s+fetch\b|$)', tail, flags=re.IGNORECASE)
                where_part = m_where_only.group(1) if m_where_only else tail
            sel = f"SELECT {col} FROM {tbl} {where_part}".strip()
            res_any: Any = self.connection.db.query(sel)
            rows_list = self._extract_result_rows(res_any) or []
            vals: list[Any] = []
            for r in rows_list:
                if isinstance(r, dict):
                    vals.append(r.get(col))
                else:
                    vals.append(r)
            # Filter None & nicht-numerische für AVG/SUM; für MIN/MAX erlauben Vergleichbare
            result_val: Any = None
            try:
                if func in ('sum', 'avg'):
                    nums = []
                    for v in vals:
                        if isinstance(v, (int, float)):
                            nums.append(float(v))
                        else:
                            try:
                                if v is not None:
                                    nums.append(float(v))
                            except Exception:
                                pass
                    if func == 'sum':
                        result_val = float(sum(nums)) if nums else 0.0
                    else:
                        result_val = float(sum(nums) / len(nums)) if nums else 0.0
                elif func == 'min':
                    cand = [v for v in vals if v is not None]
                    result_val = min(cand) if cand else None
                elif func == 'max':
                    cand = [v for v in vals if v is not None]
                    result_val = max(cand) if cand else None
            except Exception:
                result_val = None
            self.description = [(alias, None, None, None, None, None, None)]
            self._results = [(result_val,)]
            self._result_index = 0
            self.rowcount = -1
            return

        # Sonderfall: einfache GROUP BY-Emulation für Muster
        # SELECT <col>, count() [AS a] FROM <t> [WHERE <col> IN (...)] GROUP BY <col> [ORDER BY <col>]
        m_gb = re.match(
            rf'^\s*select\s+([A-Za-z_][\w]*)\s*,\s*{re.escape(COUNT_FUNC)}\s*(?:as\s+([A-Za-z_][\w]*))?\s+'
            rf'from\s+([A-Za-z_][\w]*)\s+(?:where\s+\1\s+in\s*\[(?P<inlist>[^\]]*)\]\s+)?group\s+by\s+\1(?:\s+order\s+by\s+\1\s*)?;?\s*$',
            surreal_query,
            flags=re.IGNORECASE,
        )
        if m_gb:
            col = m_gb.group(1)
            alias = m_gb.group(2) or 'count'
            tbl = m_gb.group(3)
            # Daten laden und optional nach WHERE <col> IN [...] filtern
            res: Any = self.connection.db.query(f'SELECT * FROM {tbl}')
            rows_src: List[Any] = []
            if isinstance(res, list) and res and isinstance(res[0], dict) and ('status' in res[0] or 'result' in res[0]):
                for e in cast(List[Any], res):
                    rv = e.get('result') if isinstance(e, dict) else None
                    if isinstance(rv, list):
                        rows_src.extend(rv)
                    elif rv is not None:
                        rows_src.append(rv)
            elif isinstance(res, list):
                rows_src = cast(List[Any], res)
            inlist_raw = m_gb.group('inlist')
            allowed = None
            if inlist_raw is not None:
                # Zerlege eine Liste einfacher Literale (true/false/strings/ints)
                items = [s.strip().strip("'") for s in inlist_raw.split(',') if s.strip()]
                def parse_item(x: str):
                    if x.lower() in ('true', 'false'):
                        return x.lower() == 'true'
                    try:
                        return int(x)
                    except Exception:
                        return x
                allowed = {parse_item(x) for x in items}
            # Gruppieren
            counts = {}
            for r in rows_src:
                if isinstance(r, dict):
                    key = r.get(col)
                    if allowed is not None and key not in allowed:
                        continue
                    counts[key] = counts.get(key, 0) + 1
            # Sortierung nach col (None zuerst konsistent wie Python-Sort)
            ordered_keys = sorted(counts.keys())
            self.description = [(col, None, None, None, None, None, None), (alias, None, None, None, None, None, None)]
            self._results = [(k, counts[k]) for k in ordered_keys]
            self._result_index = 0
            self.rowcount = -1
            return

        # Normale Ausführung
        # Ausführung mit optionalem Profiling
        if getattr(self.connection, '_profile', False):
            t0 = time.perf_counter()
            raw: Any = self.connection.db.query(surreal_query)
            dt = (time.perf_counter() - t0) * 1000.0
            try:
                print(f"[SurrealDB-PROFILE] execute: {dt:.2f} ms :: {surreal_query}")
            except Exception:
                pass
        else:
            raw = self.connection.db.query(surreal_query)
        if getattr(self.connection, '_log_responses', False):
            try:
                print(f"[SurrealDB-DEBUG] response: {raw}")
            except Exception:
                pass
        self._results = self._extract_result_rows(raw) or []

        # lastrowid bestimmen (falls vorhanden) und Surreal-RecordID als String merken
        self.lastrowid = None
        created_rid_str = None
        if self._results and isinstance(self._results[0], dict):
            first = self._results[0]
            lid = first.get('id') or first.get('@id')
            # RecordID → als rid_str erfassen
            if lid is not None and not isinstance(lid, (int, str)):
                # duck-typing: table_name + id vorhanden?
                tname = getattr(lid, 'table_name', None)
                rid = getattr(lid, 'id', None)
                if tname and rid:
                    created_rid_str = f"{tname}:{rid}"
            # Falls Surreal die RecordID als String liefert (z. B. "table:xyz"), ebenfalls übernehmen
            if isinstance(lid, str) and ':' in lid:
                created_rid_str = lid
            if isinstance(lid, int):
                self.lastrowid = lid
            elif isinstance(lid, str) and lid.isdigit():
                self.lastrowid = int(lid)

        # SELECT-Ergebnisse in Tupel + description verwandeln
        ql = surreal_query.strip().lower()
        if ql.startswith('select'):
            sel_cols = self._parse_select_columns(surreal_query) or None
            self._results = self._normalize_select_rows(self._results, distinct_flag, sel_cols)
            # Post-Emulation: NULLS FIRST/LAST – wenn vorhanden, sortiere clientseitig entsprechend
            try:
                import re as _re_nulls
                m_ob = _re_nulls.search(r"(?i)order\s+by\s+(.+)$", surreal_query)
                if m_ob:
                    terms_expr = m_ob.group(1)
                    # Extrahiere evtl. markierte /*NULLS FIRST|LAST*/ Felder
                    # Einfachheit: nur Einzelfeld und passender Spaltenname
                    parts = [p.strip() for p in terms_expr.split(',')]
                    marked = None
                    for p in parts:
                        if '/*NULLS FIRST*/' in p.upper() or '/*NULLS LAST*/' in p.upper():
                            marked = p
                            break
                    if marked and sel_cols:
                        # Bestimme Spaltenname und Richtung
                        base = marked.split('/*')[0].strip()
                        m_dir = _re_nulls.match(r"^([A-Za-z_][\w]*)\s*(ASC|DESC)?", base, flags=_re_nulls.IGNORECASE)
                        if m_dir:
                            cname = m_dir.group(1)
                            direction = (m_dir.group(2) or 'ASC').upper()
                            nulls_first = 'FIRST' in marked.upper()
                            try:
                                idx = sel_cols.index(cname)
                                def _key(row: tuple[Any, ...]):
                                    v = row[idx] if 0 <= idx < len(row) else None
                                    is_null = v is None
                                    return (0 if (is_null and nulls_first) else (1 if is_null else 0), v)
                                self._results.sort(key=_key, reverse=(direction == 'DESC'))
                            except Exception:
                                pass
            except Exception:
                pass
            self._result_index = 0
            self.rowcount = -1

        # Nicht-SELECT: künstliche lastrowid generieren (INSERT/CREATE)
        is_insert = ql.startswith('insert') or ql.startswith('create')
        if is_insert and self.lastrowid is None:
            tbl = None
            m_ins = re.match(r'^\s*insert\s+into\s+[`"]?([\w]+)[`"]?', surreal_query, flags=re.IGNORECASE)
            m_cre = re.match(r'^\s*create\s+([\w]+)\b', surreal_query, flags=re.IGNORECASE)
            if m_ins:
                tbl = m_ins.group(1)
            elif m_cre:
                tbl = m_cre.group(1)
            if tbl:
                map_tbl = f"django_pk_{tbl}"
                try:
                    self.lastrowid = self.connection.next_pk(map_tbl)
                except Exception:
                    # Fallback auf lokalen Zähler über öffentliche Methode
                    cnt = self.connection.add_insert_counter(tbl)
                    self.lastrowid = cnt
                # Mapping in SurrealDB persistieren, falls wir die RecordID kennen
                try:
                    if created_rid_str:
                        self.connection.db.query(f"CREATE {map_tbl} CONTENT {{ rid: '{created_rid_str}', pk: {self.lastrowid} }}")
                        # Cache befüllen (RID→PK und PK→RID)
                        try:
                            self.connection.cache_set_pk_for_rid(created_rid_str, int(self.lastrowid))
                            self.connection.cache_set_pk_to_rids(tbl, int(self.lastrowid), [created_rid_str])
                        except Exception:
                            pass
                except Exception:
                    pass
        # Ergebnisindex zurücksetzen
        self._result_index = 0
        # Für Nicht-SELECT konservativ 1 betroffene Zeile annehmen, damit Django .rowcount nutzen kann
        if not ql.startswith('select'):
            self.rowcount = 1
