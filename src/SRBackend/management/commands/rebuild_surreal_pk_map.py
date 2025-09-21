from django.core.management.base import BaseCommand
from django.apps import apps
from django.db import connection


class Command(BaseCommand):
    help = "Baut die Mapping-Tabellen django_pk_<tabelle> in SurrealDB neu auf (rid → fortlaufender int-pk)."

    def add_arguments(self, parser):
        parser.add_argument('--app', dest='app_label', help='Nur eine bestimmte App verarbeiten')
        parser.add_argument('--model', dest='model_name', help='Nur ein bestimmtes Modell innerhalb der App verarbeiten')

    def handle(self, *args, **options):
        app_label = options.get('app_label')
        model_name = options.get('model_name')

        # Stelle sicher, dass die DB-Verbindung aufgebaut ist und wir Zugriff auf den Surreal-Client haben
        using = connection
        using.ensure_connection()
        conn = using.connection
        db = conn.db

        self.stdout.write(self.style.MIGRATE_HEADING('Baue Surreal-PK-Mappings neu auf...'))

        def rebuild_for_model(model):
            # Nur konkrete (nicht-proxy, nicht-abstrakte) Modelle haben echte Tabellen
            if model._meta.proxy or model._meta.abstract:
                return
            table = model._meta.db_table
            map_tbl = f"django_pk_{table}"
            self.stdout.write(f" - {table} -> {map_tbl}")

            # 1) Bestehende Mapping-Tabelle leeren (falls vorhanden)
            try:
                db.query(f"DELETE {map_tbl}")
            except Exception:
                # Falls Tabelle nicht existiert, ignorieren wir das
                pass

            # 2) Alle Datensätze der Zieltabelle lesen und RID erfassen
            try:
                res = db.query(f"SELECT id, @id AS rid FROM {table}")
            except Exception as e:
                self.stderr.write(self.style.WARNING(f"   WARN: Kann {table} nicht lesen: {e}"))
                return

            rows = []
            if isinstance(res, list) and res and isinstance(res[0], dict) and ('status' in res[0] or 'result' in res[0]):
                for entry in res:
                    rv = entry.get('result') if isinstance(entry, dict) else None
                    if isinstance(rv, list):
                        rows.extend(rv)
                    elif rv is not None:
                        rows.append(rv)
            elif isinstance(res, list):
                rows = res

            # 3) Fortlaufende pk ab 1 vergeben und Mapping schreiben
            pk = 0
            for r in rows:
                if not isinstance(r, dict):
                    continue
                rid = r.get('rid')
                if rid is None:
                    rid = r.get('id')
                if rid is None:
                    continue
                pk += 1
                try:
                    db.query(
                        f"CREATE {map_tbl} CONTENT {{ rid: '{table}:{getattr(rid, 'id', rid)}', pk: {pk} }}"
                    )
                except Exception as e:
                    self.stderr.write(self.style.WARNING(f"   WARN: Konnte Mapping für {table} nicht schreiben: {e}"))

        # Zielmenge der Modelle bestimmen
        models = []
        if app_label and model_name:
            models = [apps.get_model(app_label, model_name)]
        elif app_label:
            models = list(apps.get_app_config(app_label).get_models())
        else:
            for app in apps.get_app_configs():
                models.extend(app.get_models())

        for m in models:
            rebuild_for_model(m)

        self.stdout.write(self.style.SUCCESS('Mapping-Aufbau abgeschlossen.'))
