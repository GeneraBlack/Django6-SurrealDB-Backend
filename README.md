# Django 6 SurrealDB Backend (Standalone)

Dieses Repository enthält ein extrahiertes, eigenständiges Django-Datenbank-Backend für SurrealDB.

Funktionen:
- SQL→SurrealQL-Übersetzung (COUNT, IN, BACKTICKS entfernen, OFFSET→START, INSERT→CREATE CONTENT)
- JOIN-Emulation (einfache Gleichheits-JOINs) clientseitig
- DISTINCT-Deduplizierung clientseitig, einfache GROUP BY count()-Emulation
- Persistente ID-Normalisierung (RecordID→int) via `django_pk_<tabelle>`
- `sql_flush` via `DELETE <table>`
- Debug/Logging via `DATABASES['default']['OPTIONS']`

## Installation

1) In ein bestehendes Django-Projekt aufnehmen (als App-ähnliches Paket, aber ohne Django-Models):

```
project/
  manage.py
  settings.py
  ...
  external/
    django-surrealdb-backend/
      src/
        SRBackend/
          base/
            base.py
            operations.py
          management/
            commands/
              rebuild_surreal_pk_map.py
      README.md
```

2) `PYTHONPATH` so setzen, dass `django-surrealdb-backend/src` importiert wird, z. B. in `manage.py` oder via Umgebungsvariable.

3) In `settings.py` die Datenbank setzen:

```python
_SUR_NS = os.environ.get('DJCC_SUR_NAMESPACE') or os.environ.get('SUR_DB_NAMESPACE') or 'core'
_SUR_DB = os.environ.get('DJCC_SUR_DB') or os.environ.get('SUR_DB_NAME') or 'core'

DATABASES = {
    'default': {
        'ENGINE': 'SRBackend.base',
        'NAME': _SUR_DB,
        'NAMESPACE': _SUR_NS,
        'HOST': 'localhost',
        'PORT': '8080',
        'USER': 'root',
        'PASSWORD': 'root',
        'OPTIONS': {
            # Performance-Hinweis: Standardmäßig AUS, per Env aktivierbar
            # Während der aktiven Entwicklungs-/Arbeitsphase standardmäßig AN.
            # Per Env-Var übersteuerbar (z. B. PowerShell: $env:SUR_DEBUG='0').
            'SUR_DEBUG': _env_bool('SUR_DEBUG', True),
            'SUR_LOG_QUERIES': _env_bool('SUR_LOG_QUERIES', True),
            'SUR_PROFILE': _env_bool('SUR_PROFILE', True),
            'SUR_LOG_RESPONSES': _env_bool('SUR_LOG_RESPONSES', True),
            'SUR_LOG_QUERY_BODY': _env_bool('SUR_LOG_QUERY_BODY', True),

            'SUR_PROTOCOL': 'ws',
            'SUR_CACHE_MAX_ENTRIES': 10000,
            # Einzigartigkeit/Constraints (z.B. ContentType (app_label, model)) sicherstellen
            'SUR_ENSURE_UNIQUES': True,
            # Optional: Schwellwert für Slow-Query-Markierung (in Millisekunden)
            'SUR_SLOW_QUERY_MS': 150.0,
        },
    }
}
```

## Management Command

- `rebuild_surreal_pk_map`: Baut die Mapping-Tabellen `django_pk_*` neu auf.

Beispiel:
```
python manage.py rebuild_surreal_pk_map
python manage.py rebuild_surreal_pk_map --app auth
python manage.py rebuild_surreal_pk_map --app auth --model Group
```

## Hinweise

- Transaktionen: autocommit; commit/rollback sind No-Ops.
- SchemaEditor: Stub (DDL wird nicht wirklich migriert); Migrations, die Inserts etc. ausführen, funktionieren.
- Komplexe SQL (verschachtelte Subqueries, Window-Funktionen) sind nicht abgedeckt.
- Getestet unter Django 6; Minor-Differenzen bitte mit CI absichern.

