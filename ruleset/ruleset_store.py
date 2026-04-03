"""
ORACLE Ruleset Store — CRUD operations for ruleset lifecycle.

Handles creation, activation, archival, and performance tracking of rulesets.
"""

import json
import uuid
import time
import oracle_db as db

STATUS_PENDING = "pending_approval"
STATUS_ACTIVE = "active"
STATUS_ARCHIVED = "archived"
STATUS_EXPIRED = "expired"
STATUS_REJECTED = "rejected"


def create_ruleset(engine: str, rules: dict, reasoning: str, market_context: dict, ttl_hours: int = 24) -> str:
    """Create a new ruleset and store it in the database.

    Args:
        engine: Engine this ruleset is for (e.g., 'scalper', 'sniper')
        rules: The actual rule definitions
        reasoning: LLM reasoning for why these rules were chosen
        market_context: Snapshot of market conditions at generation time
        ttl_hours: Time-to-live in hours (default 24)

    Returns:
        ruleset_id string
    """
    now = time.time()
    ruleset_id = f"{engine}_v{int(now)}"

    record = {
        "ruleset_id": ruleset_id,
        "engine": engine,
        "rules": rules,
        "reasoning": reasoning,
        "market_context": market_context,
        "status": STATUS_PENDING,
        "created_at": now,
        "activated_at": None,
        "expires_at": now + (ttl_hours * 3600),
        "ttl_hours": ttl_hours,
        "baseline_win_rate": None,
        "performance": {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "pnl": 0.0,
        },
    }

    db.store_ruleset(record)
    return ruleset_id


def activate_ruleset(ruleset_id: str) -> bool:
    """Activate a pending ruleset and archive the previous active one for the same engine.

    Returns True if activation succeeded.
    """
    ruleset = db.get_ruleset(ruleset_id)
    if not ruleset:
        return False

    if ruleset.get("status") not in (STATUS_PENDING, STATUS_ARCHIVED):
        # Only pending or previously archived rulesets can be activated
        if ruleset.get("status") == STATUS_ACTIVE:
            return True  # Already active
        return False

    engine = ruleset.get("engine")

    # Archive current active ruleset for this engine
    current_active = db.get_active_ruleset(engine)
    if current_active and current_active.get("ruleset_id") != ruleset_id:
        db.update_ruleset_status(current_active["ruleset_id"], STATUS_ARCHIVED)

    # Activate the new one
    now = time.time()
    ttl_hours = ruleset.get("ttl_hours", 24)
    db.update_ruleset_status(ruleset_id, STATUS_ACTIVE)
    db.update_ruleset_field(ruleset_id, "activated_at", now)
    db.update_ruleset_field(ruleset_id, "expires_at", now + (ttl_hours * 3600))

    return True


def archive_ruleset(ruleset_id: str) -> bool:
    """Archive a ruleset (remove from active duty).

    Returns True if archival succeeded.
    """
    ruleset = db.get_ruleset(ruleset_id)
    if not ruleset:
        return False

    db.update_ruleset_status(ruleset_id, STATUS_ARCHIVED)
    return True


def get_active(engine: str) -> dict:
    """Get the currently active ruleset for an engine.

    Returns the ruleset dict or None.
    """
    ruleset = db.get_active_ruleset(engine)
    if not ruleset:
        return None

    # Check if expired
    now = time.time()
    if ruleset.get("expires_at", 0) < now:
        db.update_ruleset_status(ruleset["ruleset_id"], STATUS_EXPIRED)
        return None

    return ruleset


def get_performance(ruleset_id: str) -> dict:
    """Get performance metrics for a ruleset.

    Returns: {trades, wins, losses, pnl, win_rate}
    """
    ruleset = db.get_ruleset(ruleset_id)
    if not ruleset:
        return {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "win_rate": 0.0}

    perf = ruleset.get("performance", {})
    trades = perf.get("trades", 0)
    wins = perf.get("wins", 0)
    losses = perf.get("losses", 0)
    pnl = perf.get("pnl", 0.0)
    win_rate = wins / max(trades, 1)

    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "pnl": pnl,
        "win_rate": win_rate,
    }


def update_performance(ruleset_id: str, trades: int, wins: int, losses: int, pnl: float) -> bool:
    """Update cumulative performance metrics for a ruleset.

    Args:
        ruleset_id: The ruleset to update
        trades: Total trades so far
        wins: Total winning trades
        losses: Total losing trades
        pnl: Total P&L

    Returns True if update succeeded.
    """
    ruleset = db.get_ruleset(ruleset_id)
    if not ruleset:
        return False

    perf = {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "pnl": pnl,
    }

    db.update_ruleset_field(ruleset_id, "performance", perf)
    return True
