from __future__ import annotations

"""Middleware zur Ausgabe von DB-Performance-Metriken pro Request.

Aktivierung, wenn eine der folgenden Bedingungen erfüllt ist:
- settings.DEBUG ist True
- DATABASES['default']['OPTIONS']['SUR_PROFILE'] ist True

Konfiguration (optional) in DATABASES['default']['OPTIONS']:
- SUR_SLOW_QUERY_MS: float (Default 100.0) – Ab dieser Dauer werden Queries
  als "slow" markiert.
- SUR_LOG_QUERY_BODY: bool (Default True) – Query-Text mitlogggen.
"""

import traceback
from typing import Any
from django.conf import settings
from django.utils.deprecation import MiddlewareMixin

from . import metrics as _dbm


class DBPerformanceMiddleware(MiddlewareMixin):
    def __init__(self, get_response):
        super().__init__(get_response)
        dbcfg = getattr(settings, 'DATABASES', {}).get('default', {})
        opts = (dbcfg or {}).get('OPTIONS', {}) or {}
        self.enabled = bool(getattr(settings, 'DEBUG', False) or opts.get('SUR_PROFILE', False))
        try:
            self.slow_ms = float(opts.get('SUR_SLOW_QUERY_MS', 100.0))
        except Exception:
            self.slow_ms = 100.0
        self.log_sql = bool(opts.get('SUR_LOG_QUERY_BODY', True))
        # Zusätzliche Optionen
        self.verbose_headers = bool(opts.get('SUR_METRICS_HEADERS_VERBOSE', False))
        self.trace_sql = bool(opts.get('SUR_TRACE_SQL', False))

    def process_request(self, request):  # type: ignore[override]
        if not self.enabled:
            return None
        try:
            _dbm.start_collection()
        except Exception:
            pass
        return None

    def process_response(self, request, response):  # type: ignore[override]
        if self.enabled:
            try:
                summary = _dbm.summarize()
                if summary:
                    cnt = summary.get('count', 0)
                    total_ms = summary.get('total_ms', 0.0)
                    max_ms = summary.get('max_ms', 0.0)
                    duration_ms = summary.get('duration_ms', 0.0)
                    # Schreibe kompakte Kennzahlen in Response-Header
                    try:
                        response['X-DB-Queries'] = str(cnt)
                        response['X-DB-Total-ms'] = f"{float(total_ms):.2f}"
                        response['X-DB-Max-ms'] = f"{float(max_ms):.2f}"
                        response['X-Request-Duration-ms'] = f"{float(duration_ms):.2f}"
                        if self.verbose_headers:
                            # By-Verb komprimiert darstellen: SELECT=10/35.2ms|UPDATE=2/1.1ms
                            byv = summary.get('by_verb', {}) or {}
                            parts = []
                            for k, v in byv.items():
                                try:
                                    parts.append(f"{k}={int(v.get('count',0))}/{float(v.get('total_ms',0.0)):.1f}ms")
                                except Exception:
                                    continue
                            if parts:
                                response['X-DB-ByVerb'] = '|'.join(parts)
                            # Cache-Hits/Misses kompakt
                            ch = summary.get('cache_hits', {}) or {}
                            cm = summary.get('cache_misses', {}) or {}
                            if ch:
                                response['X-DB-CacheHits'] = ','.join(f"{k}={v}" for k, v in ch.items())
                            if cm:
                                response['X-DB-CacheMisses'] = ','.join(f"{k}={v}" for k, v in cm.items())
                            # Top-Query in Header, wenn tracing aktiv (SQL gekürzt)
                            if self.trace_sql:
                                top = summary.get('top', []) or []
                                if top:
                                    t0 = top[0]
                                    sql = t0.get('sql', '')
                                    if isinstance(sql, str):
                                        sql = sql.replace('\n', ' ')
                                    sql_short = (sql[:180] + '…') if isinstance(sql, str) and len(sql) > 180 else sql
                                    response['X-DB-Top-1-ms'] = f"{float(t0.get('ms',0.0)):.2f}"
                                    response['X-DB-Top-1-sql'] = str(sql_short)
                    except Exception:
                        pass
                    print(f"[DB-PERF] queries={cnt} total={total_ms:.2f} ms max={max_ms:.2f} ms request={duration_ms:.2f} ms")
                    if cnt and self.slow_ms > 0:
                        for q in summary.get('queries', []):
                            ms = float(q.get('ms', 0.0))
                            if ms >= self.slow_ms:
                                sql = q.get('sql', '') if self.log_sql else '<redacted>'
                                print(f"[DB-PERF][SLOW ≥{self.slow_ms:.0f}ms] {ms:.2f} ms :: {sql}")
            except Exception:
                # niemals Fehler im Response verursachen
                print("[DB-PERF] summarize failed:\n" + traceback.format_exc())
            finally:
                try:
                    _dbm.clear_collection()
                except Exception:
                    pass
        return response
