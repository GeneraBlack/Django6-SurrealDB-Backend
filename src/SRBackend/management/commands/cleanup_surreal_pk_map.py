from django.core.management.base import BaseCommand
from django.db import connection

import re


class Command(BaseCommand):
    help = (
        "Bereinigt die PK→RID-Mapping-Tabellen (django_pk_*) in SurrealDB:\n"
        "- löscht verwaiste Einträge (RID ohne Ziel-Record)\n"
        "- entfernt exakte Duplikate (gleiches (rid, pk))\n"
        "Optional: definiert UNIQUE-Index auf rid je Mapping-Tabelle (--define-unique)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Nur anzeigen, keine Änderungen schreiben")
        parser.add_argument("--define-unique", action="store_true", help="DEFINE INDEX unique_rid ON <map> FIELDS rid UNIQUE")
        parser.add_argument("--verbose", action="store_true", help="Mehr Ausgaben")

    def handle(self, *args, **options):
        dry = options.get("dry_run") or False
        define_unique = options.get("define_unique") or False
        verbose = options.get("verbose") or False

        cur = connection.cursor()

        # Liste aller Tabellen
        cur.execute("INFO FOR DB")
        # Unser Cursor liefert danach keine direkte Liste; wir holen Namen separat über Introspection
        names = connection.introspection.django_table_names()
        map_tables = [n for n in names if n.startswith("django_pk_")]

        total_removed = 0
        total_dupes = 0
        total_indexed = 0

        for map_tbl in map_tables:
            target = map_tbl[len("django_pk_"):]
            if verbose:
                self.stdout.write(f"[INFO] Prüfe Mapping-Tabelle {map_tbl} → Ziel {target}")

            # Alle Mapping-Zeilen holen
            cur.execute(f"SELECT rid, pk FROM {map_tbl}")
            rows = cur.fetchall() or []
            # rows sind Tupel (rid, pk)

            seen_pairs = set()
            for rid, pk in rows:
                # rid kann als dict/objekt oder string kommen; wir wollen String
                rid_str = None
                if isinstance(rid, str):
                    rid_str = rid
                elif hasattr(rid, "table_name") and hasattr(rid, "id"):
                    rid_str = f"{getattr(rid, 'table_name')}:{getattr(rid, 'id')}"
                else:
                    # Fallback auf str
                    rid_str = str(rid)

                # Duplikate (identisches (rid,pk)) überspringen einmal, weitere löschen
                key = (rid_str, pk)
                if key in seen_pairs:
                    total_dupes += 1
                    if not dry:
                        cur.execute(
                            f"DELETE FROM {map_tbl} WHERE rid = %s AND pk = %s",
                            [rid_str, pk],
                        )
                    if verbose:
                        self.stdout.write(f"[DUPE] Entfernt doppelten Eintrag {map_tbl}: rid={rid_str} pk={pk}")
                    continue
                seen_pairs.add(key)

                # Verwaist? — existiert das Zielobjekt noch?
                # Wir prüfen per direkter RID-Ansprache in WHERE id = <rid literal> (unquoted)
                rid_sane = rid_str if re.match(r"^[A-Za-z_][\w]*:[A-Za-z0-9]+$", rid_str or "") else None
                exists = False
                if rid_sane:
                    # Direkt in die SQL einsetzen (nicht als Param), damit RecordID literal bleibt
                    sql = f"SELECT id FROM {target} WHERE id = {rid_sane} LIMIT 1"
                    try:
                        cur.execute(sql)
                        res = cur.fetchall() or []
                        exists = bool(res)
                    except Exception:
                        exists = False
                # Falls kein valider RID oder nicht existent → löschen
                if not exists:
                    total_removed += 1
                    if not dry:
                        cur.execute(
                            f"DELETE FROM {map_tbl} WHERE rid = %s AND pk = %s",
                            [rid_str, pk],
                        )
                    if verbose:
                        self.stdout.write(f"[GC] Entfernt verwaisten Eintrag {map_tbl}: rid={rid_str} pk={pk}")

            if define_unique:
                try:
                    if not dry:
                        cur.execute(f"DEFINE INDEX unique_rid ON {map_tbl} FIELDS rid UNIQUE")
                    total_indexed += 1
                    if verbose:
                        self.stdout.write(f"[INDEX] UNIQUE Index auf {map_tbl}.rid definiert")
                except Exception as ex:
                    if verbose:
                        self.stdout.write(f"[INDEX] Konnte UNIQUE nicht definieren: {ex}")

        self.stdout.write(
            f"Fertig. Entfernt: {total_removed}, Duplikate: {total_dupes}, Unique-Indizes gesetzt: {total_indexed}"
        )
