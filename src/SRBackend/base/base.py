from typing import Any, List, Tuple, Optional, Dict, Sequence, cast
try:
    from surrealdb import Surreal  # type: ignore  # NOSONAR
except Exception:  # pragma: no cover - Typing-Fallback, wenn Stubs fehlen
    Surreal = Any  # type: ignore
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.creation import BaseDatabaseCreation
from .operations import DatabaseOperations


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
        if isinstance(result, list) and result and isinstance(result[0], dict) and 'result' in result[0]:
            dbinfo_any: Any = result[0]['result']
            dbinfo: Dict[str, Any] = dbinfo_any if isinstance(dbinfo_any, dict) else {}
            tb_any: Any = dbinfo.get('tb', {})
            if isinstance(tb_any, dict):
                names: List[str] = [str(k) for k in tb_any.keys()]  # type: ignore[arg-type]
                return names  # type: ignore[return-value]
        return []

    # Wird u. a. von flush() in Tests verwendet
    def django_table_names(self, _only_existing: bool = False, _include_views: bool = True) -> List[str]:  # noqa: ARG002
        try:
            return self.table_names(None)
        except Exception:
            return []

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
        def __init__(self, db_wrapper: Any):
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
        def __init__(self, db_wrapper: Any):
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

        def __init__(self, connection, *_args: Any, **_kwargs: Any):
            self.connection = connection
            self.deferred_sql = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc: Any):
            return False

        def create_model(self, *_args: Any, **_kwargs: Any):
            return None

        def remove_field(self, *_args: Any, **_kwargs: Any):
            return None

        def alter_field(self, *_args: Any, **_kwargs: Any):
            return None

        def alter_unique_together(self, *_args: Any, **_kwargs: Any):
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

    def __init__(self, settings_dict: Dict[str, Any], alias: Optional[str] = None):  # type: ignore[override]
        super().__init__(settings_dict, alias)  # type: ignore[arg-type]
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

    def get_new_connection(self, conn_params: Dict[str, Any]):  # type: ignore[override]
        return CustomDBConnection(conn_params)

    def init_connection_state(self) -> None:
        return None

    def create_cursor(self, name: Optional[str] = None):  # type: ignore[override]
        return self.connection.cursor()

    def close(self) -> None:
        if self.connection:
            self.connection.close()

    def rollback(self) -> None:
        self.connection.rollback()

    def commit(self) -> None:
        self.connection.commit()

    def _set_autocommit(self, _autocommit: bool) -> None:  # type: ignore[override]
        # SurrealDB autocommit – nichts zu tun
        pass


class CustomDBConnection:
    def __init__(self, settings_dict: Optional[Dict[str, Any]] = None, *args: Any, **kwargs: Any):
        # Debug-Ausgabe der Settings
        self.settings_dict = settings_dict or {}
        # Debug-/Logging-Steuerung über settings.py -> DATABASES[...]['OPTIONS']
        # Beispiel:
        # DATABASES = {
        #   'default': {
        #       'ENGINE': 'SRBackend.base',
        #       'NAME': '...', 'NAMESPACE': '...',
        #       'OPTIONS': {
        #           'SUR_DEBUG': True,  # oder 'DEBUG': True
        #       },
        #   }
        # }
        opts: Dict[str, Any] = cast(Dict[str, Any], self.settings_dict.get('OPTIONS') or {})
        try:
            self._debug = bool(opts.get('SUR_DEBUG') or opts.get('DEBUG') or False)
        except Exception:
            self._debug = False
        try:
            self._log_queries = bool(opts.get('SUR_LOG_QUERIES') or False)
        except Exception:
            self._log_queries = False
        if self._debug:
            print("[SurrealDB-DEBUG] settings_dict (komplett):", self.settings_dict)
        self.user = cast(str, self.settings_dict.get('USER') or 'root')
        self.password = cast(str, self.settings_dict.get('PASSWORD') or 'root')
        self.host = cast(str, self.settings_dict.get('HOST') or 'localhost')
        self.port = int(self.settings_dict.get('PORT') or 8080)
        self.db_name = cast(Optional[str], self.settings_dict.get('NAME'))
        self.namespace = cast(Optional[str], self.settings_dict.get('NAMESPACE'))
        if not self.db_name or not self.namespace:
            raise ValueError("SurrealDB: NAME und NAMESPACE müssen in settings.py gesetzt sein!")
        self.database = self.db_name
        if self._debug:
            print(f"[SurrealDB-DEBUG] USER={self.user} PASSWORD={self.password} HOST={self.host} PORT={self.port} NAME={self.db_name} NAMESPACE={self.namespace}")
        self.connection = None
        self.db = Surreal(f"http://{self.host}:{self.port}")
        self.connected = False
        self._insert_counters = {}
        self._pk_counters = {}
        self.connect()

    def query(self, sql: str) -> Any:
        return self.db.query(sql)

    def commit(self) -> None:
        if self._debug:
            print("Transaction committed.")

    def close(self) -> None:
        self.connected = False
        if self._debug:
            print("Connection closed.")

    def rollback(self) -> None:
        if self._debug:
            print("Transaction rollback (no-op for SurrealDB).")

    def connect(self) -> None:
        self.db.signin({"username": self.user, "password": self.password})
        self.db.use(self.namespace, self.database)
        self.connected = True
        if self._debug:
            print(f"Connected to SurrealDB: {self.host}:{self.port} as {self.user} (NS: {self.namespace}, DB: {self.database})")

    def check(self, **kwargs):
        raise NotImplementedError

    def check_field(self, field, **kwargs):
        raise NotImplementedError

    def cursor(self) -> "CustomDBCursor":
        return CustomDBCursor(self)

    # Interner Helfer: liefert nächste PK für eine Mapping-Tabelle
    def next_pk(self, map_tbl: str) -> int:  # NOSONAR - bewusst kompakt, aber leicht verzweigt
        cur = self._pk_counters.get(map_tbl)
        if cur is None:
            mx_val = 0
            try:
                res_mx = self.db.query(f"SELECT math::max(pk) AS mx FROM {map_tbl}")
                if isinstance(res_mx, list) and res_mx:
                    if isinstance(res_mx[0], dict) and 'result' in res_mx[0]:
                        r = res_mx[0]['result']
                        if isinstance(r, list) and r and isinstance(r[0], dict) and 'mx' in r[0] and r[0]['mx'] is not None:
                            mx_val = int(r[0]['mx'])
                    elif isinstance(res_mx[0], dict) and 'mx' in res_mx[0] and res_mx[0]['mx'] is not None:
                        mx_val = int(res_mx[0]['mx'])
            except Exception:
                mx_val = 0
            cur = mx_val
        cur += 1
        self._pk_counters[map_tbl] = cur
        return cur


class CustomDBCursor:
    def __init__(self, connection: CustomDBConnection):
        self.connection: CustomDBConnection = connection
        self.lastrowid: Optional[int] = None
        self._results: List[Any] = []
        self._result_index: int = 0
        self.description: Optional[List[Tuple]] = None
        # DB-API 2.0: Anzahl betroffener Zeilen; für SELECT üblicherweise -1
        self.rowcount: int = -1

    # --- Interne Helper für Übersetzungen (nur Struktur, Verhalten unverändert) ---
    def _fmt_param(self, p: Any) -> str:
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
        # Tabellenqualifizierer entfernen
        q = re.sub(r'\b([A-Za-z_][\w]*)\.([A-Za-z_@][\w]*)\b', r'\2', q)
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

            def norm(v):
                tname = getattr(v, 'table_name', None)
                rid = getattr(v, 'id', None)
                if tname and rid:
                    rid_str = f"{tname}:{rid}"
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
                                    return int(max(pks))
                                except Exception:
                                    return pks[0]
                            new_id = self.connection.next_pk(map_tbl)
                            try:
                                self.connection.db.query(f"UPDATE {map_tbl} SET pk = {new_id} WHERE rid = '{rid_str}'")
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
                        return new_id
                    except Exception:
                        return rid_str
                return v

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

    def _parse_select_columns(self, sql: str) -> Optional[List[str]]:  # NOSONAR - bewusst detailliert
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
            m_as = re.search(r'\bas\s+([A-Za-z_]\w*)\s*$', expr2, flags=re.IGNORECASE)
            if m_as:
                return m_as.group(1)
            # Entferne Qualifizierer a.b -> b
            expr2 = expr2.replace('"', '')
            expr2 = expr2.replace('`', '')
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
        surreal_query = str(query)
        # DISTINCT erkennen und später clientseitig deduplizieren
        if getattr(self.connection, '_log_queries', False):
            try:
                print(f"[SurrealDB-DEBUG] SQL in: {query} params={params}")
            except Exception:
                pass
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
        pattern_cnt = rf'^\s*select\s+{re.escape(COUNT_FUNC)}\s*(?:as\s+([A-Za-z][A-Za-z0-9_]*))?\s+from\s+([A-Za-z][A-Za-z0-9_]*)\s*;?\s*$'
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
        raw: Any = cast(Any, self.connection.db).query(surreal_query)
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
            if isinstance(lid, int):
                self.lastrowid = lid
            elif isinstance(lid, str) and lid.isdigit():
                self.lastrowid = int(lid)

        # SELECT-Ergebnisse in Tupel + description verwandeln
        ql = surreal_query.strip().lower()
        if ql.startswith('select'):
            sel_cols = self._parse_select_columns(surreal_query) or None
            self._results = self._normalize_select_rows(self._results, distinct_flag, sel_cols)
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
                    # Fallback auf lokalen Zähler
                    cnt = self.connection._insert_counters.get(tbl, 0) + 1
                    self.connection._insert_counters[tbl] = cnt
                    self.lastrowid = cnt
                # Mapping in SurrealDB persistieren, falls wir die RecordID kennen
                try:
                    if created_rid_str:
                        cast(Any, self.connection.db).query(f"CREATE {map_tbl} CONTENT {{ rid: '{created_rid_str}', pk: {self.lastrowid} }}")
                except Exception:
                    pass
        # Ergebnisindex zurücksetzen
        self._result_index = 0
        # Für Nicht-SELECT konservativ 1 betroffene Zeile annehmen, damit Django .rowcount nutzen kann
        if not ql.startswith('select'):
            self.rowcount = 1
