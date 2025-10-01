"""Thread-lokale Erfassung von DB-Query-Metriken für Requests.

Wenn aktiviert (über Middleware), werden alle Query-Laufzeiten, die der
SurrealDB-Backendcode misst, hier gesammelt und am Ende des Requests
aggregiert ausgegeben.

Aktivierung: Die Middleware `DBPerformanceMiddleware` ruft start_collection()
zu Beginn auf und fasst die Daten mit summarize() am Ende zusammen.

Der Backendcode ruft record(sql, ms) auf, wenn eine Collection aktiv ist.
Zusätzlich können Cache-Hits/-Misses vermerkt werden.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

_tls = threading.local()


def _get_aggr() -> Optional[Dict[str, Any]]:
    return getattr(_tls, "db_perf", None)


def start_collection() -> None:
    _tls.db_perf = {  # type: ignore[attr-defined]
        "queries": [],  # List[{"sql": str, "ms": float, "verb": str}]
        "t0": time.perf_counter(),
        "by_verb": {},  # Dict[str, {count:int, total_ms: float, max_ms: float}]
        "cache_hits": {},  # Dict[str, int]
        "cache_misses": {},  # Dict[str, int]
    }


def clear_collection() -> None:
    if hasattr(_tls, "db_perf"):
        delattr(_tls, "db_perf")


def is_active() -> bool:
    return _get_aggr() is not None


def _extract_verb(sql: str) -> str:
    try:
        s = sql.strip()
        if not s:
            return ""
        # Erster Token bis Leerzeichen
        tok = s.split()[0]
        return str(tok).upper()
    except Exception:
        return ""


def record(sql: str, ms: float) -> None:
    aggr = _get_aggr()
    if aggr is None:
        return
    try:
        verb = _extract_verb(sql)
        aggr["queries"].append({"sql": sql, "ms": float(ms), "verb": verb})
        byv: Dict[str, Dict[str, Any]] = aggr.setdefault("by_verb", {})  # type: ignore[assignment]
        ent: Dict[str, Any] = dict(byv.get(verb) or {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
        ent["count"] = int(ent.get("count", 0) or 0) + 1
        ent["total_ms"] = float(ent.get("total_ms", 0.0) or 0.0) + float(ms)
        ent["max_ms"] = max(float(ent.get("max_ms", 0.0) or 0.0), float(ms))
        byv[verb] = ent
    except Exception:
        # defensive: nie die Ausführung stören
        pass


def record_cache_hit(kind: str) -> None:
    aggr = _get_aggr()
    if aggr is None:
        return
    try:
        k = str(kind)
        d: Dict[str, int] = aggr.setdefault("cache_hits", {})  # type: ignore[assignment]
        d[k] = int(d.get(k, 0) or 0) + 1
    except Exception:
        pass


def record_cache_miss(kind: str) -> None:
    aggr = _get_aggr()
    if aggr is None:
        return
    try:
        k = str(kind)
        d: Dict[str, int] = aggr.setdefault("cache_misses", {})  # type: ignore[assignment]
        d[k] = int(d.get(k, 0) or 0) + 1
    except Exception:
        pass


def summarize() -> Optional[Dict[str, Any]]:
    aggr = _get_aggr()
    if aggr is None:
        return None
    try:
        queries: List[Dict[str, Any]] = list(aggr.get("queries", []))
        total_ms = sum(float(q.get("ms", 0.0)) for q in queries)
        max_ms = max([float(q.get("ms", 0.0)) for q in queries], default=0.0)
        duration_ms = (time.perf_counter() - float(aggr.get("t0", time.perf_counter()))) * 1000.0
        # Top-3 langsame Queries
        try:
            top = sorted(queries, key=lambda x: float(x.get("ms", 0.0)), reverse=True)[:3]
        except Exception:
            top = []
        by_verb: Dict[str, Dict[str, Any]] = dict(aggr.get("by_verb", {}))
        cache_hits: Dict[str, int] = dict(aggr.get("cache_hits", {}))
        cache_misses: Dict[str, int] = dict(aggr.get("cache_misses", {}))
        return {
            "count": len(queries),
            "total_ms": total_ms,
            "max_ms": max_ms,
            "duration_ms": duration_ms,
            "queries": queries,
            "by_verb": by_verb,
            "top": top,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
        }
    except Exception:
        return None
