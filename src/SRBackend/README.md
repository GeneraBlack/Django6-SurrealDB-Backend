# Django 6 SurrealDB Backend (SRBackend)

Deutsches, produktionsnahes Django-Datenbank-Backend für [SurrealDB](https://surrealdb.com/).

Dieses Backend ermöglicht:
- Migrations und ORM-Queries gegen SurrealDB
- SQL→SurrealQL-Übersetzung (COUNT, IN, Backticks, Tabellenqualifizierer, INSERT→CREATE)
- Einfache JOIN-Emulation (INNER JOIN on Gleichheit)
- Persistente ID-Normalisierung (Surreal RecordID → ganzzahliger Django-PK)

Hinweis: Das Backend ist bewusst pragmatisch gehalten, damit typische Django-Workloads (Migrations, CRUD, einfache Aggregationen) funktionieren. Für komplexe SQL/ORM-Fälle sind ggf. Erweiterungen nötig.

---

## Installation & Voraussetzungen

- Django 6.x
- Python-Paket `surrealdb`
- Laufende SurrealDB-Instanz (z. B. `http://localhost:8080`)

Beispielkonfiguration in `settings.py`:

```python
DATABASES = {
    'default': {
        'ENGINE': 'SRBackend.base',
        'NAME': 'core',           # SurrealDB DB-Name
        'NAMESPACE': 'core',      # SurrealDB Namespace
        'HOST': 'localhost',
        'PORT': '8080',
        'USER': 'root',
        'PASSWORD': 'root',
        'OPTIONS': {
            # Logging & Profiling
            'SUR_DEBUG': True,
            'SUR_LOG_QUERIES': True,
            'SUR_LOG_RESPONSES': False,
            'SUR_PROFILE': True,
            # Transport: http|https|ws|wss
            'SUR_PROTOCOL': 'ws',
            # Caching
            'SUR_CACHE_MAX_ENTRIES': 10_000,
            # Konsistenz / Constraints
            'SUR_ENSURE_UNIQUES': True,
            # Middleware/Headers (siehe unten)
            'SUR_SLOW_QUERY_MS': 150.0,
            'SUR_LOG_QUERY_BODY': True,
            'SUR_METRICS_HEADERS_VERBOSE': False,
            'SUR_TRACE_SQL': False,
        },
    }
}
```

Tipp: In Projekten wie `dj-cc` werden diese Werte häufig aus Umgebungsvariablen befüllt (kleine Helferfunktionen in `settings.py`).

---

## Funktionsumfang (Überblick)

- SQL→SurrealQL-Übersetzung:
  - `COUNT(*)`/`COUNT(1)` → `count()`
  - `IN (a, b)` → `IN [a, b]`
  - Entfernt Backticks und Tabellen-Qualifikationen (`table.column` → `column`)
  - `INSERT INTO <t>(...) VALUES (...)` → `CREATE <t> CONTENT { ... }`
- JOIN-Emulation (einfach):
  - `INNER JOIN ... ON (a.x = b.y)` wird clientseitig emuliert
  - Unqualifizierte ON-Bedingungen (z. B. `content_type_id = id`) werden heuristisch zugeordnet
- ID-Normalisierung (wichtig für Django):
  - Surreal-`RecordID` → fortlaufender `int`-PK
  - Persistente Zuordnung in `django_pk_<tabelle>`
  - Automatische Vergabe bei Lesen/Schreiben

Weitere Emulationen:
- `DISTINCT` (clientseitig)
- `OFFSET n` → `START n`
- `SELECT count() FROM <t>` (robuste Zählung)
- Einfache `GROUP BY`
- `flush`: `DELETE <table>` für alle Tabellen

---

## Performance‑Metriken & Middleware

Die Middleware `SRBackend.base.middleware.DBPerformanceMiddleware` erfasst Query-Metriken pro Request und setzt Antwort‑Header:

- `X-DB-Queries`: Anzahl ORM‑Queries
- `X-DB-Total-ms`: Gesamtzeit der DB‑Queries
- `X-DB-Max-ms`: Langsamste Query
- `X-Request-Duration-ms`: Request‑Dauer insgesamt
- Optional bei `SUR_METRICS_HEADERS_VERBOSE=True`:
  - `X-DB-ByVerb`: Aggregation nach SQL‑Verb (z. B. `SELECT=10/35.2ms`)
  - `X-DB-CacheHits` / `X-DB-CacheMisses`
- Optional bei `SUR_TRACE_SQL=True`:
  - `X-DB-Top-1-ms` und gekürztes `X-DB-Top-1-sql`

Aktivierung: Middleware in `MIDDLEWARE` eintragen (siehe Beispielprojekt `dj-cc`).

Wichtige Optionen (in `DATABASES['default']['OPTIONS']`):
- `SUR_SLOW_QUERY_MS` (float, Default 100.0): Ab dieser Dauer werden Queries als „slow“ geloggt.
- `SUR_LOG_QUERY_BODY` (bool, Default True): Query‑Text in Slow‑Logs anzeigen.
- `SUR_METRICS_HEADERS_VERBOSE` (bool): Zusätzliche Header mit Aggregaten/Caches.
- `SUR_TRACE_SQL` (bool): Top‑Query (gekürzt) als Response‑Header ausgeben.

---

## Debugging & Logging

- `SUR_DEBUG`: Allgemeine Debug‑Ausgaben (Verbindung etc.)
- `SUR_LOG_QUERIES`: SQL in / SurrealQL out
- `SUR_LOG_RESPONSES`: Rohantwort der DB (nur kurzzeitig aktivieren)
- `SUR_PROFILE`: Messung der Laufzeiten (ms)
- `SUR_PROTOCOL`: `http|https|ws|wss`
- `SUR_CACHE_MAX_ENTRIES`: Größe der In‑Memory‑Caches für PK↔RID
- `SUR_ENSURE_UNIQUES`: Erzwingt Einzigartigkeit/Constraints (z. B. ContentType (app_label, model))

Empfehlung Produktion: `SUR_PROFILE=False`, `SUR_LOG_RESPONSES=False`, `SUR_LOG_QUERIES` nur bei Bedarf. Caches aktiviert lassen.

---

## Management Command: Mapping neu aufbauen

```powershell
c:/Users/Gener/Projekte/corecontrol/.venv/Scripts/python.exe manage.py rebuild_surreal_pk_map
# nur App
c:/Users/Gener/Projekte/corecontrol/.venv/Scripts/python.exe manage.py rebuild_surreal_pk_map --app auth
# nur Modell
c:/Users/Gener/Projekte/corecontrol/.venv/Scripts/python.exe manage.py rebuild_surreal_pk_map --app auth --model Group
```

Dieses Kommando leert die `django_pk_*`‑Tabellen, liest alle Datensätze und vergibt fortlaufende `pk`‑Werte.

---

## Grenzen & Hinweise

- JOIN‑Emulation ist einfach (INNER JOIN, Gleichheit)
- SQL‑Übersetzung ist auf gängige Django‑SQLs optimiert
- Transaktionen: Autocommit; `commit/rollback` werden für API‑Kompatibilität bereitgestellt
- Quoting/Backticks/qualifizierte Spalten werden neutralisiert

---

## Praxis‑Tipp (dj-cc)

Das Projekt `dj-cc` zeigt bewährte Patterns:
- Umschalten von Logging per Env‑Variablen in `settings.py`
- Aktivierte DB‑Metrik‑Middleware in DEV, deaktiviert in PROD
- Beispiel‑OPTIONS wie oben (inkl. `SUR_ENSURE_UNIQUES`, `SUR_SLOW_QUERY_MS`, `SUR_LOG_QUERY_BODY`)

Wenn erweiterte SQL‑Fälle oder Performance‑Ziele anstehen, können die Übersetzungen modular erweitert werden.
