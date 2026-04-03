"""
ORACLE Playbooks — Named regime response bundles.

Layer 4: Instead of individual parameter tweaks on regime shifts,
apply coordinated multi-engine playbooks.

Layer 5: Proactive alerts — detect trends worth flagging and push to Discord.
"""

import json
import time
import requests

import oracle_db as db
from oracle_memory import get_trend, get_all_trends, record_intervention, get_intervention_hit_rate

# ── Discord webhook (shared with Nikita) ─────────────────────────────────────

def _get_discord_webhook():
    try:
        import os
        keys_path = os.path.join(os.path.dirname(__file__), "..", "Paper trader", "api_keys.json")
        with open(keys_path) as f:
            keys = json.load(f)
        return keys.get("discord_summary_webhook") or keys.get("discord_webhook", "")
    except Exception:
        return ""


def _send_discord(message: str):
    webhook = _get_discord_webhook()
    if not webhook:
        return
    try:
        requests.post(webhook, json={"content": message[:1950]}, timeout=5)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 4: REGIME PLAYBOOKS
# ═══════════════════════════════════════════════════════════════════════════════

PLAYBOOKS = {
    "FEAR_CAPITULATION": {
        "description": "Extreme fear + high volatility — defensive posture",
        "conditions": lambda ctx: ctx.get("macro") in ("BEARISH", "RISK_OFF") and ctx.get("fear_greed", "NEUTRAL") == "EXTREME_FEAR",
        "actions": {
            "scalper": {"mode": "CONSERVATIVE", "max_concurrent_scalps": 1},
            "hypothesis": {"min_bars_green": 6, "min_alignment_total": 2.5},
            "swing": {"paused": True},
        },
        "signals": ["ORACLE_TRADE_BLOCK"],
    },
    "SLOW_BLEED": {
        "description": "Bearish + low volatility — reduce exposure, widen stops",
        "conditions": lambda ctx: ctx.get("macro") in ("BEARISH", "RISK_OFF") and ctx.get("fear_greed", "NEUTRAL") != "EXTREME_FEAR",
        "actions": {
            "scalper": {"mode": "CONSERVATIVE", "max_concurrent_scalps": 2},
            "hypothesis": {"min_bars_green": 6},
        },
        "signals": [],
    },
    "BULL_BREAKOUT": {
        "description": "Risk-on + momentum confirmed — aggressive posture",
        "conditions": lambda ctx: ctx.get("macro") in ("BULLISH", "RISK_ON"),
        "actions": {
            "scalper": {"mode": "AGGRESSIVE", "max_concurrent_scalps": 3},
            "hypothesis": {"min_bars_green": 4, "min_alignment_total": 1.8},
            "swing": {"paused": False},
        },
        "signals": [],
    },
    "CHOP_RANGE": {
        "description": "Neutral + no trend — scalper only, fast in/out",
        "conditions": lambda ctx: ctx.get("macro") == "NEUTRAL",
        "actions": {
            "scalper": {"mode": "MOMENTUM"},
            "hypothesis": {"min_bars_green": 5},
        },
        "signals": [],
    },
}

_active_playbook = None


def evaluate_playbook(enrichment_context: dict) -> str | None:
    """Determine which playbook should be active based on current context."""
    global _active_playbook

    ctx = {
        "macro": enrichment_context.get("macro_lean", "NEUTRAL"),
        "fear_greed": enrichment_context.get("fear_greed_zone", "NEUTRAL"),
        "cycle_phase": enrichment_context.get("cycle_phase", "UNKNOWN"),
        "social_velocity": enrichment_context.get("social_velocity", "FLAT"),
    }

    # Find matching playbook (first match wins — order matters)
    for name, playbook in PLAYBOOKS.items():
        try:
            if playbook["conditions"](ctx):
                if name != _active_playbook:
                    _activate_playbook(name, playbook, ctx)
                return name
        except Exception:
            continue

    return _active_playbook


def _activate_playbook(name: str, playbook: dict, ctx: dict):
    """Activate a playbook — apply all actions across engines."""
    global _active_playbook
    prev = _active_playbook
    _active_playbook = name

    print(f"  [ORACLE PLAYBOOK] Activating: {name} (was: {prev or 'none'})")
    print(f"    {playbook['description']}")

    nikita = "http://192.168.158.237:5000"
    mechanicus = "http://192.168.158.237:7777"

    # Record intervention (Layer 2)
    metrics_before = {}
    scalper_trend = get_trend("scalper_wr", 1)
    if scalper_trend.get("current") is not None:
        metrics_before["scalper_wr"] = scalper_trend["current"]
    hypothesis_trend = get_trend("hypothesis_wr", 1)
    if hypothesis_trend.get("current") is not None:
        metrics_before["hypothesis_wr"] = hypothesis_trend["current"]

    record_intervention(
        f"playbook_{name}", "all",
        f"Activated playbook {name} (from {prev or 'none'})",
        metrics_before,
    )

    # ── Advisory mode: log recommended actions, don't push configs ──────
    actions = playbook.get("actions", {})

    # Log what we WOULD have done (advisory only)
    if actions:
        print(f"    [ADVISORY] Recommended actions: {actions}")
        # Broadcast playbook recommendation to Hive so agents can incorporate it
        try:
            requests.post(f"http://192.168.158.203:5001/api/broadcast_signal", json={
                "asset": "SYSTEM",
                "direction": "ADVISORY",
                "confidence": "high",
                "trend": name.lower(),
                "playbook": name,
                "recommended_actions": actions,
            }, timeout=5)
        except Exception:
            pass

    # Safety rail signals still get emitted (TRADE_BLOCK is a hard stop)
    for signal_type in playbook.get("signals", []):
        try:
            requests.post(f"{nikita}/api/engine-signals", json={
                "signal_type": signal_type,
                "data": {"reasoning": f"Playbook {name}: {playbook['description']}", "playbook": name},
                "ttl_minutes": 30,
                "engine": "oracle",
            }, timeout=5)
        except Exception:
            pass

    # Log observation
    db.add_observation("playbook_activated", {
        "playbook": name,
        "previous": prev,
        "context": ctx,
        "actions": actions,
    }, severity="info", action_taken=f"Activated playbook: {name}")


def get_active_playbook() -> dict:
    """Return current playbook status."""
    return {
        "active": _active_playbook,
        "description": PLAYBOOKS.get(_active_playbook, {}).get("description", ""),
        "available": list(PLAYBOOKS.keys()),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 5: PROACTIVE ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

_last_alert_time = 0
_alert_cooldown = 1800  # 30 min between alerts (avoid spam)


def check_proactive_alerts(llm_caller=None):
    """Check memory trends for things worth flagging to the user.
    Only fires if something genuinely changed. Max 1 alert per 30 min."""
    global _last_alert_time

    if time.time() - _last_alert_time < _alert_cooldown:
        return None

    alerts = []

    # Win rate collapse
    scalper = get_trend("scalper_wr", 4)
    if scalper.get("direction") == "declining" and scalper.get("change_pct", 0) < -15:
        alerts.append(f"Scalper win rate declining sharply: {scalper['change_pct']:+.0f}% over 4h (now {scalper['current']:.0f}%)")

    hypothesis = get_trend("hypothesis_wr", 4)
    if hypothesis.get("direction") == "declining" and hypothesis.get("change_pct", 0) < -15:
        alerts.append(f"Hypothesis WR declining: {hypothesis['change_pct']:+.0f}% over 4h (now {hypothesis['current']:.0f}%)")

    # Hive meme performance drop
    hive_meme = get_trend("hive_meme_wr", 4)
    if hive_meme.get("direction") == "declining" and hive_meme.get("change_pct", 0) < -10:
        alerts.append(f"Hive meme agents declining: {hive_meme['change_pct']:+.0f}% WR over 4h")

    # Enrichment staleness
    enrichment = get_trend("enrichment_fresh_pct", 1)
    if enrichment.get("current") is not None and enrichment["current"] < 40:
        alerts.append(f"Enrichment severely stale: only {enrichment['current']:.0f}% modes fresh")

    # Portfolio drawdown accelerating
    drawdown = get_trend("portfolio_drawdown", 4)
    if drawdown.get("direction") == "declining" and drawdown.get("current", 0) > 10:
        alerts.append(f"Portfolio drawdown at {drawdown['current']:.1f}% and accelerating")

    # Health score dropping
    health = get_trend("health_score", 1)
    if health.get("current") is not None and health["current"] < 5:
        alerts.append(f"System health score LOW: {health['current']:.1f}/10")

    if not alerts:
        return None

    _last_alert_time = time.time()

    # Build the alert message
    alert_text = "\n".join(f"  - {a}" for a in alerts)
    raw_message = f"**ORACLE PROACTIVE ALERT**\n{alert_text}"

    # If LLM available, get natural language summary
    if llm_caller:
        try:
            trends = get_all_trends(4)
            prompt = (
                f"You are ORACLE, the intelligence layer of a crypto trading network. "
                f"These alerts just fired:\n{alert_text}\n\n"
                f"Current trends (4h window):\n{json.dumps(trends, indent=2, default=str)[:1500]}\n\n"
                f"Write a 2-3 sentence briefing for the operator. Be specific, actionable, concise."
            )
            narrative = llm_caller(prompt, "You are ORACLE. Brief the operator.", temperature=0.3, max_tokens=300)
            if narrative and not narrative.startswith("[ORACLE ERROR"):
                raw_message = f"**ORACLE PROACTIVE ALERT**\n\n{narrative}\n\n```\nRaw alerts:\n{alert_text}\n```"
        except Exception:
            pass

    _send_discord(raw_message)

    db.add_observation("proactive_alert", {
        "alerts": alerts,
        "trends": {k: v.get("direction") for k, v in get_all_trends(4).items()},
    }, severity="warning", action_taken=f"Sent {len(alerts)} proactive alerts to Discord")

    print(f"  [ORACLE ALERT] Sent {len(alerts)} alerts to Discord")
    return alerts


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3: HIVE EXPERIMENT DRIVER
# ═══════════════════════════════════════════════════════════════════════════════

_last_hive_experiment = 0
_hive_experiment_cooldown = 7200  # 2 hours between experiments


def drive_hive_experiments():
    """Analyze Hive trait data and submit experiments if warranted.
    Max 1 experiment at a time. Auto-reverts after 4 hours if no improvement."""
    global _last_hive_experiment

    if time.time() - _last_hive_experiment < _hive_experiment_cooldown:
        return None

    hive_url = "http://192.168.158.203:5001"

    # Check if there's already an active experiment
    try:
        r = requests.get(f"{hive_url}/api/trait-lab/experiments", timeout=5)
        if r.ok:
            exps = r.json()
            active = [e for e in exps if not e.get("reverted")]
            if active:
                # Check if oldest active experiment is >4h old — auto-revert
                oldest = active[-1]
                if oldest.get("started_at"):
                    from datetime import datetime, timezone
                    started = datetime.fromisoformat(oldest["started_at"].replace("Z", "+00:00"))
                    age_h = (datetime.now(timezone.utc) - started).total_seconds() / 3600
                    if age_h > 4:
                        requests.post(f"{hive_url}/api/trait-lab/experiment/{oldest['id']}/revert", timeout=5)
                        print(f"  [ORACLE→HIVE] Auto-reverted experiment #{oldest['id']} after {age_h:.1f}h")
                return None  # Don't stack experiments
    except Exception:
        return None

    # Pull trait correlations from Hive
    try:
        r = requests.get(f"{hive_url}/api/trait-lab/correlations", timeout=5)
        if not r.ok:
            return None
        correlations = r.json()
    except Exception:
        return None

    if not correlations:
        return None

    # Find worst-performing trait
    worst = None
    worst_corr = 0
    for c in correlations:
        if c.get("verdict") == "hurts" and c.get("sample", 0) >= 10:
            avg = (c.get("corr_winrate", 0) + c.get("corr_pnl", 0)) / 2
            if avg < worst_corr:
                worst_corr = avg
                worst = c

    if not worst:
        return None

    trait = worst["trait"]
    action = "scale_down"  # Scale down the trait that's hurting

    # Check if we've tried this before and it didn't work
    history = get_intervention_hit_rate(f"hive_experiment_{trait}")
    if history.get("total", 0) >= 3 and history.get("hit_rate", 0) < 30:
        # This experiment type has failed before — skip
        return None

    # Submit experiment
    try:
        r = requests.post(f"{hive_url}/api/trait-lab/experiment", json={
            "trait": trait,
            "action": action,
        }, timeout=5)
        if r.ok:
            result = r.json()
            if not result.get("error"):
                _last_hive_experiment = time.time()

                record_intervention(
                    f"hive_experiment_{trait}", "hive",
                    f"Scale down {trait} (corr={worst_corr:.3f}, verdict=hurts)",
                    {"trait": trait, "corr_winrate": worst.get("corr_winrate"), "corr_pnl": worst.get("corr_pnl")},
                )

                print(f"  [ORACLE→HIVE] Experiment: scale_down {trait} (corr={worst_corr:.3f})")

                db.add_observation("hive_experiment_submitted", {
                    "trait": trait,
                    "action": action,
                    "correlation": worst_corr,
                    "agents_affected": result.get("agents_affected", 0),
                }, severity="info", action_taken=f"Submitted Hive experiment: {action} {trait}")

                return {"trait": trait, "action": action}
    except Exception:
        pass

    return None
