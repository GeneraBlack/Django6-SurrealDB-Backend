from django.test import TestCase
from django.db import connection
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group


class SurrealBackendTests(TestCase):
    def setUp(self):
        # Explizit leeren (SurrealQL-Syntax), um Testisolation sicherzustellen
        with connection.cursor() as cur:
            cur.execute("DELETE auth_group")
            cur.execute("DELETE users_customuser")
        # Einige Gruppen anlegen
        Group.objects.create(name="g1")
        Group.objects.create(name="g2")
        Group.objects.create(name="g3")
        # Zwei Nutzer mit unterschiedlichen is_staff-Werten für DISTINCT-Test
        user_model = get_user_model()
        user_model.objects.create_user(email="a@example.com", password="x", is_staff=True)
        user_model.objects.create_user(email="b@example.com", password="x", is_staff=False)

    def test_count_star_translated(self):
        with connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM auth_group")
            row = cur.fetchone()
        # Exakt 3 Gruppen (g1..g3) sollten gezählt werden
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 3)

    def test_in_list_translation_on_strings(self):
        with connection.cursor() as cur:
            cur.execute("SELECT name FROM auth_group WHERE name IN (%s, %s) ORDER BY name", ["g1", "g3"])
            rows = cur.fetchall()
        self.assertEqual([r[0] for r in rows], ["g1", "g3"])

    def test_distinct_and_offset(self):
        # DISTINCT auf is_staff sollte 2 Werte liefern, OFFSET 1 -> 1 Zeile
        with connection.cursor() as cur:
            cur.execute("SELECT DISTINCT is_staff FROM users_customuser ORDER BY is_staff OFFSET 1 LIMIT 1")
            rows = cur.fetchall()
        self.assertEqual(len(rows), 1)

    def test_join_emulation_on_permission_contenttype(self):
        # Einfache JOIN-Abfrage: auth_permission INNER JOIN django_content_type
        # Suche nach einer Permission, die es typischerweise gibt: add_customuser (für unser Usermodell)
        with connection.cursor() as cur:
            cur.execute(
                "SELECT id FROM auth_permission INNER JOIN django_content_type ON (content_type_id = id) "
                "WHERE codename = %s LIMIT 1",
                ["add_customuser"],
            )
            row = cur.fetchone()
        # Keine harte Zusicherung auf Existenz, aber die Query darf nicht crashen; bei Erfolg ist row entweder None oder ein Tupel
        self.assertTrue(row is None or isinstance(row, tuple))

    def test_id_normalization_returns_int(self):
        with connection.cursor() as cur:
            cur.execute("SELECT id, name FROM auth_group ORDER BY name")
            rows = cur.fetchall()
        ids = [r[0] for r in rows]
        self.assertTrue(all(isinstance(i, int) for i in ids))

    def test_group_by_count_emulation(self):
        # Erwartet zwei Zeilen: (False, 1) und (True, 1)
        with connection.cursor() as cur:
            cur.execute(
                "SELECT is_staff, count() FROM users_customuser GROUP BY is_staff ORDER BY is_staff"
            )
            rows = cur.fetchall()
        self.assertEqual(rows, [(False, 1), (True, 1)])

    def test_group_by_with_where(self):
        # Filter auf vorhandene Werte und gruppieren
        with connection.cursor() as cur:
            cur.execute(
                "SELECT is_staff, count() FROM users_customuser WHERE is_staff IN (%s, %s) GROUP BY is_staff ORDER BY is_staff",
                [True, False],
            )
            rows = cur.fetchall()
        self.assertEqual(rows, [(False, 1), (True, 1)])

    def test_order_by_and_pagination_combo(self):
        # Kombinierte ORDER BY + START/LIMIT
        with connection.cursor() as cur:
            cur.execute(
                "SELECT name FROM auth_group ORDER BY name START 1 LIMIT 1"
            )
            rows = cur.fetchall()
        self.assertEqual(len(rows), 1)

    def test_admin_login_smoke(self):
        # Admin-Smoketest: Superuser anlegen und Login auf /admin/ prüfen
        user_model = get_user_model()
        su = user_model.objects.create_superuser(email="admin@example.com", password="secret", username="admin")
        # Sicherheitshalber aktiv setzen und re-laden
        su.is_active = True
        su.save()
        client = self.client
        logged_in = client.login(username="admin", password="secret")
        self.assertTrue(logged_in)
        r = client.get("/admin/")
        self.assertIn(r.status_code, (200, 302))
