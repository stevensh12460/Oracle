"""
ORACLE Mode 2 — Proactive Mode

ORACLE watches all engines and pushes unsolicited observations/advice
when it detects issues: performance drops, regime shifts, signal conflicts.
"""

import time
import oracle_db as db
from oracle_tools import (
    get_regime_state,
    get_win_rates,
    get_cross_engine_signals,
    get_enrichment_cache,
)
from ruleset.ruleset_manager import get_active_rulesets


def check_for_issues() -> list:
    """Called by the watch loop. Scans for problems across all engines.

    Checks:
      - Performance drops (win rate declining)
      - Regime shifts (market environment changed since ruleset was generated)
      - Signal conflicts (cross-engine signals contradicting active rules)
      - Stale rulesets (no trades in extended period)

    Returns:
        List of observation dicts, each with: engine, type, severity, message, data
    """
    observations = []
    now = time.time()
    active_rulesets = get_active_rulesets()
    current_regime = get_regime_state()
    current_signals = get_cross_engine_signals()

    for ruleset in active_rulesets:
        engine = ruleset.get("engine", "unknown")
        ruleset_id = ruleset.get("ruleset_id", "")

        # --- Performance drop detection ---
        perf = ruleset.get("performance", {})
        trades = perf.get("trades", 0)
        if trades >= 10:
            win_rate = perf.get("wins", 0) / max(trades, 1)
            # Check recent window vs overall
            recent_wr = get_win_rates(engine, "1h")
            if isinstance(recent_wr, dict):
                recent_val = recent_wr.get("win_rate", win_rate)
            else:
                recent_val = win_rate

            if win_rate > 0 and recent_val < win_rate * 0.7:
                observations.append({
                    "engine": engine,
                    "ruleset_id": ruleset_id,
                    "type": "PERFORMANCE_DECLINING",
                    "severity": "warning",
                    "message": (
                        f"{engine} recent win rate ({recent_val*100:.0f}%) is significantly "
                        f"below overall ({win_rate*100:.0f}%)"
                    ),
                    "timestamp": now,
                    "data": {"overall_wr": win_rate, "recent_wr": recent_val},
                })

        # --- Regime mismatch detection ---
        gen_regime = ruleset.get("market_context", {}).get("regime")
        if isinstance(current_regime, dict):
            live_regime = current_regime.get("macro") or current_regime.get("regime")
        else:
            live_regime = current_regime

        if gen_regime and live_regime and gen_regime != live_regime:
            observations.append({
                "engine": engine,
                "ruleset_id": ruleset_id,
                "type": "REGIME_MISMATCH",
                "severity": "warning",
                "message": (
                    f"{engine} ruleset was generated for '{gen_regime}' regime, "
                    f"but current regime is '{live_regime}'"
                ),
                "timestamp": now,
                "data": {"generated_for": gen_regime, "current": live_regime},
            })

        # --- Signal conflict detection ---
        if isinstance(current_signals, list):
            rules = ruleset.get("rules", {})
            rule_bias = _extract_bias(rules)
            for signal in current_signals:
                signal_direction = signal.get("direction", "").lower()
                if rule_bias and signal_direction and rule_bias != signal_direction:
                    observations.append({
                        "engine": engine,
                        "ruleset_id": ruleset_id,
                        "type": "SIGNAL_CONFLICT",
                        "severity": "info",
                        "message": (
                            f"Cross-engine signal '{signal.get('type', 'unknown')}' suggests "
                            f"'{signal_direction}' but {engine} rules are biased '{rule_bias}'"
                        ),
                        "timestamp": now,
                        "data": {"signal": signal, "rule_bias": rule_bias},
                    })

        # --- Stale ruleset detection (no trades in 2+ hours) ---
        last_trade_time = perf.get("last_trade_time", ruleset.get("activated_at", now))
        if last_trade_time and (now - last_trade_time) > 7200:
            observations.append({
                "engine": engine,
                "ruleset_id": ruleset_id,
                "type": "STALE_RULESET",
                "severity": "info",
                "message": (
                    f"{engine} has had no trades for "
                    f"{(now - last_trade_time) / 3600:.1f}h — rules may be too restrictive"
                ),
                "timestamp": now,
                "data": {"hours_since_trade": (now - last_trade_time) / 3600},
            })

    # Store all observations
    for obs in observations:
        try:
            db.add_observation(
                obs_type=obs.get("type", "UNKNOWN"),
                data=obs.get("data", {}),
                engine=obs.get("engine"),
                severity=obs.get("severity", "info"),
                action_taken=obs.get("action_taken"),
            )
        except Exception:
            pass

    return observations


def _extract_bias(rules: dict) -> str:
    """Try to determine the directional bias from a ruleset.

    Returns 'bullish', 'bearish', or None.
    """
    if not isinstance(rules, dict):
        return None

    bias_field = rules.get("bias") or rules.get("direction") or rules.get("market_bias")
    if isinstance(bias_field, str):
        bias_lower = bias_field.lower()
        if "bull" in bias_lower or "long" in bias_lower:
            return "bullish"
        elif "bear" in bias_lower or "short" in bias_lower:
            return "bearish"

    return None
