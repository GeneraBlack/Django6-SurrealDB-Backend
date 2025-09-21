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
DATABASES = {
    'default': {
        'ENGINE': 'SRBackend.base',
        'NAME': 'core',
        'NAMESPACE': 'core',
        'HOST': 'localhost',
        'PORT': '8080',
        'USER': 'root',
        'PASSWORD': 'root',
        'OPTIONS': {
            'SUR_DEBUG': False,
            'SUR_LOG_QUERIES': False,
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
