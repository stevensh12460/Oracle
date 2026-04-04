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
from oracle_memory import get_trend, get_all_trends, record_intervention, get_intervention_hit_rate, get_intervention_history

# ── Hive HTTP session (shared across playbook + culling advisory) ─────────────
_HIVE_BASE = "http://192.168.158.203:5001"
_http_hive = requests.Session()

# ── Herald delivery (replaces direct Discord webhook POSTs) ──────────────────

_HERALD_URL = "http://localhost:5700/send"
_herald_session = requests.Session()


def _send_discord(message: str):
    """Send an alert via Herald (alerts channel, oracle_alert type)."""
    try:
        _herald_session.post(_HERALD_URL, json={
            "channel": "alerts",
            "type": "oracle_alert",
            "format": "raw",
            "data": {"text": message},
        }, timeout=5)
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
        "hive": {
            "advisory": "Reduce position sizes, favor patient agents, pause meme engine if WR <30%",
            "trait_bias": {"patience": "increase", "aggression": "decrease", "risk_tolerance": "decrease"},
            "meme_engine_pause": True,
        },
    },
    "SLOW_BLEED": {
        "description": "Bearish + low volatility — reduce exposure, widen stops",
        "conditions": lambda ctx: ctx.get("macro") in ("BEARISH", "RISK_OFF") and ctx.get("fear_greed", "NEUTRAL") != "EXTREME_FEAR",
        "actions": {
            "scalper": {"mode": "CONSERVATIVE", "max_concurrent_scalps": 2},
            "hypothesis": {"min_bars_green": 6},
        },
        "signals": [],
        "hive": {
            "advisory": "Slow bleed environment — tighten stops, reduce aggression, prefer major assets over memes",
            "trait_bias": {"aggression": "decrease", "asset_preference": "major", "patience": "increase"},
            "meme_engine_pause": False,
        },
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
        "hive": {
            "advisory": "Risk-on confirmed — maximize agent aggression, allow meme engine, favor momentum-sensitive agents",
            "trait_bias": {"aggression": "increase", "reaction_speed": "increase", "risk_tolerance": "increase"},
            "meme_engine_pause": False,
        },
    },
    "CHOP_RANGE": {
        "description": "Neutral + no trend — scalper only, fast in/out",
        "conditions": lambda ctx: ctx.get("macro") == "NEUTRAL",
        "actions": {
            "scalper": {"mode": "MOMENTUM"},
            "hypothesis": {"min_bars_green": 5},
        },
        "signals": [],
        "hive": {
            "advisory": "Choppy market — favor range-trading agents, reduce momentum bias, protect regime specialists",
            "trait_bias": {"patience": "increase", "reaction_speed": "decrease"},
            "meme_engine_pause": False,
        },
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
            _http_hive.post(f"{_HIVE_BASE}/api/broadcast_signal", json={
                "asset": "SYSTEM",
                "direction": "ADVISORY",
                "confidence": "high",
                "trend": name.lower(),
                "playbook": name,
                "recommended_actions": actions,
            }, timeout=5)
        except Exception:
            pass

    # ── Hive-specific playbook actions ───────────────────────────────────
    hive_actions = playbook.get("hive", {})
    if hive_actions:
        print(f"    [HIVE ADVISORY] {hive_actions.get('advisory', '')}")
        try:
            _http_hive.post(f"{_HIVE_BASE}/api/broadcast_signal", json={
                "asset": "SYSTEM",
                "direction": "PLAYBOOK_HIVE",
                "confidence": "high",
                "playbook": name,
                "hive_advisory": hive_actions.get("advisory", ""),
                "trait_bias": hive_actions.get("trait_bias", {}),
                "meme_engine_pause": hive_actions.get("meme_engine_pause", False),
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


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3b: CULLING ADVISORY
# ═══════════════════════════════════════════════════════════════════════════════

_last_advisory_time = 0
_ADVISORY_INTERVAL = 6600  # 110 minutes (10 min before 2-hour evolution)


def build_culling_advisory(memory) -> dict:
    """Build a structured culling advisory for The Hive's next evolution cycle.

    Analyzes: regime specialists, trait trends, diversity, experiment history,
    and market context to recommend who to protect, flag, and how to mutate.
    """
    advisory = {
        "timestamp": time.time(),
        "protected_agents": [],
        "protection_reasons": {},
        "flagged_agents": [],
        "flag_reasons": {},
        "trait_guidance": {},
        "regime_forecast": "",
        "diversity_score": 0.0,
        "diversity_warning": "",
        "experiment_insights": {},
    }

    # 1. Get regime specialists from Hive
    try:
        # Get current macro regime
        macro_trend = memory.get_trend("macro_regime", 4)
        current_regime = "CHOP"  # default
        if macro_trend and macro_trend.get("current") is not None:
            val = macro_trend["current"]
            current_regime = "STRONG_TREND" if abs(val) > 0.7 else "WEAK_TREND" if abs(val) > 0.3 else "CHOP"

        # Protect regime specialists for current AND likely next regime
        for regime in [current_regime, "CHOP", "WEAK_TREND"]:
            try:
                resp = _http_hive.get(f"{_HIVE_BASE}/api/elo/regime/{regime}", timeout=5)
                if resp.ok:
                    specialists = resp.json()
                    if isinstance(specialists, list):
                        for agent in specialists[:2]:  # Top 2 per regime
                            name = agent.get("name", "")
                            if name and name not in advisory["protected_agents"]:
                                advisory["protected_agents"].append(name)
                                advisory["protection_reasons"][name] = f"Top {regime} specialist (regime rating: {agent.get('regime_rating', '?')})"
            except Exception:
                pass
    except Exception:
        pass

    # 2. Check diversity — flag clones with identical traits
    try:
        resp = _http_hive.get(f"{_HIVE_BASE}/api/elo/leaderboard?limit=100", timeout=8)
        if resp.ok:
            agents = resp.json()
            lineage_counts = {}
            count = 0
            for a in agents:
                lin = a.get("lineage", "unknown")
                lineage_counts[lin] = lineage_counts.get(lin, 0) + 1
                count += 1

            # Diversity score: 1.0 = perfectly diverse, 0.0 = all same lineage
            if count > 0:
                max_concentration = max(lineage_counts.values()) / count
                advisory["diversity_score"] = round(1.0 - max_concentration, 2)

                # Flag over-concentrated lineages
                for lin, cnt in lineage_counts.items():
                    if cnt > count * 0.3:  # >30% from same lineage
                        lin_agents = [a for a in agents if a.get("lineage") == lin]
                        lin_agents.sort(key=lambda x: x.get("elo_rating", 0))
                        for a in lin_agents[:3]:  # Flag bottom 3 of concentrated lineage
                            name = a.get("name", "")
                            if name:
                                advisory["flagged_agents"].append(name)
                                advisory["flag_reasons"][name] = f"Lineage {lin} over-concentrated ({cnt}/{count} agents = {cnt/count*100:.0f}%)"

                if advisory["diversity_score"] < 0.5:
                    advisory["diversity_warning"] = f"Swarm converging — diversity score {advisory['diversity_score']}. Consider protecting unique agents and pruning clones."
    except Exception:
        pass

    # 3. Trait guidance from correlations
    try:
        resp = _http_hive.get(f"{_HIVE_BASE}/api/trait-lab/correlations", timeout=5)
        if resp.ok:
            corrs = resp.json()
            if isinstance(corrs, dict):
                for trait, data in corrs.items():
                    if isinstance(data, dict):
                        verdict = data.get("verdict", "neutral")
                        corr_wr = data.get("corr_winrate", 0)
                        if verdict == "helps" and abs(corr_wr) > 0.15:
                            advisory["trait_guidance"][f"increase_{trait}"] = f"{trait} correlates with winning (r={corr_wr:.2f})"
                        elif verdict == "hurts" and abs(corr_wr) > 0.15:
                            advisory["trait_guidance"][f"reduce_{trait}"] = f"{trait} correlates with losing (r={corr_wr:.2f})"
    except Exception:
        pass

    # 4. Experiment insights from intervention history
    try:
        interventions = get_intervention_history(limit=50)
        if interventions:
            experiment_types = {}
            for inv in interventions:
                inv_type = inv.get("intervention_type", "")
                if "experiment" in inv_type.lower() or "trait" in inv_type.lower():
                    outcome = inv.get("outcome", "no_change") or "no_change"
                    if inv_type not in experiment_types:
                        experiment_types[inv_type] = {"improved": 0, "worsened": 0, "no_change": 0, "total": 0}
                    experiment_types[inv_type][outcome] = experiment_types[inv_type].get(outcome, 0) + 1
                    experiment_types[inv_type]["total"] += 1

            for exp_type, counts in experiment_types.items():
                total = counts["total"]
                if total > 0:
                    hit_rate = counts["improved"] / total
                    advisory["experiment_insights"][exp_type] = (
                        f"{'improved' if hit_rate > 0.5 else 'mixed' if hit_rate > 0.3 else 'worsened'} "
                        f"(hit rate {hit_rate*100:.0f}% over {total} experiments)"
                    )
    except Exception:
        pass

    # 5. Regime forecast from MACRO trend
    try:
        macro_trend = memory.get_trend("macro_regime", 4)
        if macro_trend:
            direction = macro_trend.get("direction", "stable")
            if direction == "declining":
                advisory["regime_forecast"] = "Macro trending bearish — favor defensive agents (high patience, low aggression)"
            elif direction == "improving":
                advisory["regime_forecast"] = "Macro trending bullish — favor aggressive agents with momentum sensitivity"
            else:
                advisory["regime_forecast"] = "Macro stable — current regime specialists should continue performing"
    except Exception:
        advisory["regime_forecast"] = "Unable to determine regime trend"

    return advisory


def maybe_send_culling_advisory(memory):
    """Send culling advisory to The Hive 10 minutes before evolution cycle."""
    global _last_advisory_time
    now = time.time()
    if now - _last_advisory_time < _ADVISORY_INTERVAL:
        return
    _last_advisory_time = now

    import logging
    log = logging.getLogger("oracle")

    advisory = build_culling_advisory(memory)
    try:
        _http_hive.post(f"{_HIVE_BASE}/api/evolution/advisory", json=advisory, timeout=10)
        log.info("[ORACLE] Culling advisory sent to Hive")
        db.add_observation(
            obs_type="culling_advisory",
            data=advisory,
            engine="hive",
            severity="info",
            action_taken=f"Sent advisory: {len(advisory['protected_agents'])} protected, {len(advisory['flagged_agents'])} flagged",
        )
    except Exception as e:
        log.warning(f"[ORACLE] Failed to send culling advisory: {e}")
