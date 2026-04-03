"""
ORACLE Memory — Rolling windows, trend detection, intervention tracking.

Layer 1: Remembers how metrics trend over time (not just current snapshot).
Layer 2: Tracks whether ORACLE's own interventions actually worked.

Stores hourly snapshots. Computes trends over 1h/4h/24h windows.
Persists to oracle_trends + oracle_interventions tables.
"""

import time
import json
import requests
from collections import defaultdict

import oracle_db as db

# ── Metrics we track ─────────────────────────────────────────────────────────

TRACKED_METRICS = [
    "scalper_wr",           # Scalper win rate
    "hypothesis_wr",        # Hypothesis engine win rate
    "hive_meme_wr",         # Hive meme scalper agents win rate
    "hive_major_wr",        # Hive major/swing agents win rate
    "macro_regime",         # RISK_ON=1, NEUTRAL=0, RISK_OFF=-1
    "enrichment_fresh_pct", # % of enrichment modes that are fresh
    "portfolio_drawdown",   # Drawdown % from peak
    "health_score",         # Cross-system health (0-10)
    "hive_agent_count",     # Active Hive agents
    "trades_per_hour",      # Total trades across all engines
]

# In-memory rolling buffer (persisted to DB periodically)
_memory = defaultdict(list)  # metric -> [{value, timestamp}, ...]
_max_buffer = 48  # 48 hours of hourly snapshots

# ── Service URLs ─────────────────────────────────────────────────────────────

def _nikita():
    return "http://192.168.158.237:5000"

def _hive():
    return "http://192.168.158.203:5001"

def _llama():
    return "http://192.168.158.237:8989"

def _mechanicus():
    return "http://192.168.158.237:7777"


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 1: ROLLING MEMORY
# ═══════════════════════════════════════════════════════════════════════════════

def record_metric(metric: str, value: float):
    """Record a metric value with current timestamp."""
    now = time.time()
    _memory[metric].append({"value": value, "ts": now})
    # Trim to max buffer
    if len(_memory[metric]) > _max_buffer:
        _memory[metric] = _memory[metric][-_max_buffer:]


def get_trend(metric: str, window_hours: float = 4) -> dict:
    """Get trend for a metric over a time window.
    Returns: {current, avg, min, max, direction, change_pct, samples}
    """
    cutoff = time.time() - (window_hours * 3600)
    points = [p for p in _memory.get(metric, []) if p["ts"] >= cutoff]

    if not points:
        return {"current": None, "direction": "unknown", "samples": 0}

    values = [p["value"] for p in points]
    current = values[-1]
    avg = sum(values) / len(values)

    # Direction: compare last quarter to first quarter
    if len(values) >= 4:
        first_q = sum(values[:len(values)//4]) / max(len(values)//4, 1)
        last_q = sum(values[-len(values)//4:]) / max(len(values)//4, 1)
        if last_q > first_q * 1.05:
            direction = "improving"
        elif last_q < first_q * 0.95:
            direction = "declining"
        else:
            direction = "stable"
    else:
        direction = "insufficient_data"

    change_pct = ((current - values[0]) / max(abs(values[0]), 0.001)) * 100 if values[0] != 0 else 0

    return {
        "current": round(current, 4),
        "avg": round(avg, 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "direction": direction,
        "change_pct": round(change_pct, 2),
        "samples": len(values),
    }


def get_all_trends(window_hours: float = 4) -> dict:
    """Get trends for all tracked metrics."""
    return {m: get_trend(m, window_hours) for m in TRACKED_METRICS if _memory.get(m)}


def collect_snapshot():
    """Pull current metrics from all services and record them.
    Called every watch cycle (90s). Lightweight — uses cached/health endpoints."""

    # Scalper win rate
    try:
        r = requests.get(f"{_nikita()}/api/scalper/stats", timeout=3)
        if r.ok:
            d = r.json()
            total = d.get("total", 0)
            wins = d.get("wins", 0)
            if total > 0:
                record_metric("scalper_wr", round(wins / total * 100, 1))
                record_metric("trades_per_hour", total)
    except Exception:
        pass

    # Hypothesis win rate (from Mechanicus)
    try:
        r = requests.get(f"{_mechanicus()}/api/executor/stats", timeout=3)
        if r.ok:
            d = r.json()
            wr = d.get("win_rate", 0)
            if wr:
                record_metric("hypothesis_wr", float(wr))
    except Exception:
        pass

    # Hive metrics
    try:
        r = requests.get(f"{_nikita()}/api/hive/report", timeout=3)
        if r.ok:
            d = r.json()
            if not d.get("error"):
                ep = d.get("engine_performance", {})
                meme = ep.get("meme_scalp", {})
                major = ep.get("major_swing", {})
                if meme.get("win_rate") is not None:
                    record_metric("hive_meme_wr", meme["win_rate"])
                if major.get("win_rate") is not None:
                    record_metric("hive_major_wr", major["win_rate"])
                record_metric("hive_agent_count", d.get("agent_count", 0))
    except Exception:
        pass

    # Enrichment freshness
    try:
        r = requests.get(f"{_llama()}/cache/enrichment", timeout=3)
        if r.ok:
            d = r.json()
            total = len(d)
            fresh = sum(1 for v in d.values() if isinstance(v, dict) and v.get("is_fresh"))
            if total > 0:
                record_metric("enrichment_fresh_pct", round(fresh / total * 100, 1))
    except Exception:
        pass

    # Portfolio drawdown
    try:
        r = requests.get(f"{_nikita()}/api/snapshot/lite", timeout=3)
        if r.ok:
            d = r.json()
            balance = d.get("balance", 0)
            # Simple drawdown estimate from start
            start = 1000  # default start balance
            if balance < start:
                record_metric("portfolio_drawdown", round((1 - balance / start) * 100, 1))
            else:
                record_metric("portfolio_drawdown", 0)
    except Exception:
        pass


def persist_to_db():
    """Save current memory state to oracle_trends table. Called periodically."""
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    for metric, points in _memory.items():
        if points:
            latest = points[-1]
            try:
                db.execute(
                    "INSERT INTO oracle_trends (timestamp, metric, value, window) VALUES (?, ?, ?, '1h')",
                    (now_str, metric, latest["value"]),
                )
            except Exception:
                pass


def load_from_db():
    """Load recent trends from DB into memory on startup."""
    try:
        rows = db.query(
            "SELECT metric, value, timestamp FROM oracle_trends ORDER BY id DESC LIMIT 500"
        )
        for row in reversed(rows):
            metric = row.get("metric", "")
            value = row.get("value", 0)
            if metric:
                _memory[metric].append({"value": value, "ts": time.time()})
        if rows:
            print(f"  [ORACLE MEMORY] Loaded {len(rows)} trend points from DB")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2: INTERVENTION TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

def record_intervention(intervention_type: str, target_engine: str, action: str, metrics_before: dict = None):
    """Record that ORACLE took an action. Metrics_before is a snapshot of relevant metrics."""
    try:
        db.execute(
            """INSERT INTO oracle_interventions
               (intervention_type, target_engine, action_taken, metrics_before)
               VALUES (?, ?, ?, ?)""",
            (intervention_type, target_engine, action,
             json.dumps(metrics_before or {})),
        )
    except Exception:
        pass


def evaluate_pending_interventions():
    """Check interventions that haven't been evaluated yet.
    Compare metrics_before to current state. Score the outcome."""
    try:
        pending = db.query(
            """SELECT id, intervention_type, target_engine, action_taken, metrics_before, timestamp
               FROM oracle_interventions
               WHERE evaluated_at IS NULL AND timestamp < datetime('now', 'localtime', '-30 minutes')
               LIMIT 10"""
        )

        for interv in pending:
            before = json.loads(interv.get("metrics_before", "{}"))
            engine = interv.get("target_engine", "")

            # Get current metrics for comparison
            after = {}
            if engine == "scalper":
                trend = get_trend("scalper_wr", 1)
                after["wr"] = trend.get("current", 0)
            elif engine == "hypothesis":
                trend = get_trend("hypothesis_wr", 1)
                after["wr"] = trend.get("current", 0)

            # Score: did the target metric improve?
            before_wr = before.get("wr", 0) or 0
            after_wr = after.get("wr", 0) or 0

            if after_wr > before_wr + 5:
                outcome = "improved"
                score = 1.0
            elif after_wr < before_wr - 5:
                outcome = "worsened"
                score = -1.0
            else:
                outcome = "no_change"
                score = 0.0

            db.execute(
                """UPDATE oracle_interventions
                   SET metrics_after=?, evaluated_at=datetime('now','localtime'), outcome=?, score=?
                   WHERE id=?""",
                (json.dumps(after), outcome, score, interv["id"]),
            )

        if pending:
            improved = sum(1 for p in pending if True)  # logged for debugging
            print(f"  [ORACLE MEMORY] Evaluated {len(pending)} interventions")

    except Exception as e:
        print(f"  [ORACLE MEMORY] Evaluation error: {e}")


def get_intervention_history(intervention_type: str = None, limit: int = 20) -> list:
    """Get past interventions with outcomes."""
    if intervention_type:
        return db.query(
            "SELECT * FROM oracle_interventions WHERE intervention_type=? ORDER BY id DESC LIMIT ?",
            (intervention_type, limit),
        )
    return db.query(
        "SELECT * FROM oracle_interventions ORDER BY id DESC LIMIT ?",
        (limit,),
    )


def get_intervention_hit_rate(intervention_type: str) -> dict:
    """What % of this intervention type historically improved things?"""
    rows = db.query(
        """SELECT outcome, COUNT(*) as cnt FROM oracle_interventions
           WHERE intervention_type=? AND evaluated_at IS NOT NULL
           GROUP BY outcome""",
        (intervention_type,),
    )
    total = sum(r["cnt"] for r in rows)
    improved = sum(r["cnt"] for r in rows if r["outcome"] == "improved")
    worsened = sum(r["cnt"] for r in rows if r["outcome"] == "worsened")
    return {
        "total": total,
        "improved": improved,
        "worsened": worsened,
        "hit_rate": round(improved / max(total, 1) * 100, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 6: CROSS-SYSTEM HEALTH SCORE
# ═══════════════════════════════════════════════════════════════════════════════

def compute_health_score() -> dict:
    """Compute composite system health (0-10) from all services."""
    score = 10.0
    details = {}

    # Nikita alive and trading?
    try:
        r = requests.get(f"{_nikita()}/health", timeout=3)
        if r.ok:
            details["nikita"] = "online"
        else:
            score -= 3
            details["nikita"] = "down"
    except Exception:
        score -= 3
        details["nikita"] = "unreachable"

    # Hive alive?
    try:
        r = requests.get(f"{_hive()}/api/status", timeout=3)
        if r.ok:
            d = r.json()
            obs = d.get("observer", {})
            if obs.get("error_count", 0) > 10:
                score -= 0.5
                details["hive_errors"] = obs["error_count"]
            details["hive"] = "online"
            details["hive_agents"] = d.get("active_agents", 0)
        else:
            score -= 1
            details["hive"] = "down"
    except Exception:
        score -= 0.5
        details["hive"] = "unreachable"

    # Llama alive + both GPUs?
    try:
        r = requests.get(f"{_llama()}/status", timeout=3)
        if r.ok:
            d = r.json()
            router = d.get("router", {}).get("instances", {})
            pri = router.get("primary", {}).get("online", False)
            sec = router.get("secondary", {}).get("online", False)
            if not pri:
                score -= 1.5
                details["llama_primary"] = "offline"
            if not sec:
                score -= 0.5
                details["llama_secondary"] = "offline"
            details["llama"] = "online"
            details["llama_queue"] = d.get("queue_depth", 0)
        else:
            score -= 2
            details["llama"] = "down"
    except Exception:
        score -= 1
        details["llama"] = "unreachable"

    # Mechanicus alive?
    try:
        r = requests.get(f"{_mechanicus()}/health", timeout=3)
        if r.ok:
            details["mechanicus"] = "online"
        else:
            score -= 1
            details["mechanicus"] = "down"
    except Exception:
        score -= 0.5
        details["mechanicus"] = "unreachable"

    # Enrichment freshness
    enrichment_trend = get_trend("enrichment_fresh_pct", 1)
    if enrichment_trend.get("current") is not None:
        pct = enrichment_trend["current"]
        if pct < 50:
            score -= 1.5
            details["enrichment"] = f"stale ({pct:.0f}% fresh)"
        elif pct < 75:
            score -= 0.5
            details["enrichment"] = f"partially stale ({pct:.0f}%)"

    # Scalper performance
    scalper_trend = get_trend("scalper_wr", 4)
    if scalper_trend.get("current") is not None and scalper_trend["current"] < 20:
        score -= 0.5
        details["scalper"] = f"struggling ({scalper_trend['current']:.0f}% WR)"

    score = max(0, min(10, score))
    record_metric("health_score", round(score, 1))

    # Persist snapshot
    try:
        db.execute(
            "INSERT INTO oracle_health_snapshots (health_score, details) VALUES (?, ?)",
            (round(score, 1), json.dumps(details)),
        )
    except Exception:
        pass

    return {
        "score": round(score, 1),
        "details": details,
        "trends": get_all_trends(4),
    }
