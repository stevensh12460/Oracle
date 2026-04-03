"""ORACLE Ruleset Manager — the brain of ORACLE's autonomous capability.

Enhanced with active integration hooks:
- Signal emission to cross-engine signal bus
- Intelligence briefing injection into Llama prompts
- Scalper + Hypothesis engine config tuning
- Enrichment cache writing
- Pre-trade veto signal emission
"""

import time
import json
import re
import requests
from pathlib import Path

import oracle_db as db
from oracle_tools import (
    get_enrichment_cache,
    get_enrichment_freshness,
    get_regime_state,
    get_win_rates,
    get_cross_engine_signals,
    get_active_directives,
    get_pattern_learner_insights,
    get_enrichment_accuracy,
    get_scalp_fingerprints,
    get_scalper_current_ruleset,
    get_engine_status,
    get_open_positions,
    get_portfolio_summary,
    get_guardian_status,
    get_hypothesis_history,
    get_trade_history,
)

# Config
_config_path = Path(__file__).parent.parent / "oracle_config.json"
_config = {}

def _load_config():
    global _config
    with open(_config_path) as f:
        _config = json.load(f)

# Watch state
_watch_state = {
    "last_macro": None,
    "last_briefing": 0,
    "last_scalper_tune": 0,
    "last_hypothesis_tune": 0,
    "last_cache_write": 0,
    "last_ruleset_check": 0,
    "scalper_baseline": {},
    "hypothesis_baseline": {},
}

# Service URLs
def _nikita():
    if not _config: _load_config()
    return _config.get("services", {}).get("nikita", {}).get("base_url", "http://192.168.158.237:5000")

def _llama():
    if not _config: _load_config()
    return _config.get("services", {}).get("llama", {}).get("base_url", "http://192.168.158.237:8989")

def _mechanicus():
    if not _config: _load_config()
    return _config.get("services", {}).get("mechanicus", {}).get("base_url", "http://192.168.158.237:7777")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN WATCH CYCLE — called every 60 seconds
# ═══════════════════════════════════════════════════════════════════════════════

def run_watch_cycle():
    """Main ORACLE watch cycle. Checks everything and takes action.
    Enhanced with 6 intelligence layers: memory, intervention tracking,
    playbooks, proactive alerts, Hive experiments, cross-system health."""
    if not _config:
        _load_config()

    now = time.time()
    actions = []

    try:
        # ── Layer 1: Collect memory snapshot (every cycle) ──
        try:
            from oracle_memory import collect_snapshot, evaluate_pending_interventions, persist_to_db, load_from_db
            if not _watch_state.get("memory_loaded"):
                load_from_db()
                _watch_state["memory_loaded"] = True
            collect_snapshot()
        except Exception as e:
            if "memory_load_err" not in _watch_state:
                print(f"  [ORACLE MEMORY] Load error: {e}")
                _watch_state["memory_load_err"] = True

        # ── Hook 1: Signal Emission ──
        actions += _emit_signals(now)

        # ── Hook 2: Intelligence Briefing (every 30 min) ──
        if now - _watch_state["last_briefing"] > 1800:
            _write_intelligence_briefing()
            _watch_state["last_briefing"] = now
            actions.append("briefing_written")

        # ── Hook 3: Scalper Config Tuning (every 5 min) ──
        if now - _watch_state["last_scalper_tune"] > 300:
            tuned = _tune_scalper()
            _watch_state["last_scalper_tune"] = now
            if tuned:
                actions.append(f"scalper_tuned:{tuned}")

        # ── Hook 6: Enrichment Cache Write (every 30 min) ──
        if now - _watch_state["last_cache_write"] > 1800:
            _write_enrichment_cache()
            _watch_state["last_cache_write"] = now
            actions.append("cache_written")

        # ── Hook 7: Hypothesis Threshold Tuning (every 5 min) ──
        if now - _watch_state["last_hypothesis_tune"] > 300:
            tuned = _tune_hypothesis()
            _watch_state["last_hypothesis_tune"] = now
            if tuned:
                actions.append(f"hypothesis_tuned:{tuned}")

        # ── Layer 4: Playbook evaluation (every cycle) ──
        try:
            from oracle_playbooks import evaluate_playbook
            enrichment = get_enrichment_cache() or {}
            # Build enrichment context for playbook matching
            macro = enrichment.get("MACRO", {})
            macro_result = macro.get("result", {}) if isinstance(macro, dict) else {}
            enrichment_ctx = {
                "macro_lean": str(macro_result.get("directional_lean") or "NEUTRAL").upper(),
                "fear_greed_zone": "NEUTRAL",  # Will be extracted below
            }
            # Extract fear/greed from MACRO stage data
            stages = macro_result.get("stage_results") or {}
            macro_crypto = str(stages.get("macro_crypto") or "").lower()
            if "extreme fear" in macro_crypto:
                enrichment_ctx["fear_greed_zone"] = "EXTREME_FEAR"
            elif "fear" in macro_crypto[:200]:
                enrichment_ctx["fear_greed_zone"] = "FEAR"
            elif "extreme greed" in macro_crypto:
                enrichment_ctx["fear_greed_zone"] = "EXTREME_GREED"
            elif "greed" in macro_crypto[:200]:
                enrichment_ctx["fear_greed_zone"] = "GREED"

            playbook = evaluate_playbook(enrichment_ctx)
            if playbook:
                actions.append(f"playbook:{playbook}")
        except Exception:
            pass

        # ── Proactive LLM Analysis (every 2 hours) ──
        if now - _watch_state.get("last_proactive", 0) > 7200:
            _run_proactive_analysis()
            _watch_state["last_proactive"] = now
            actions.append("proactive_analysis")

        # ── Layer 5: Proactive Alerts (checked every cycle, fires max every 30 min) ──
        try:
            from oracle_playbooks import check_proactive_alerts
            llm_caller = None
            try:
                from oracle_llm import call_qwen
                llm_caller = call_qwen
            except Exception:
                pass
            alerts = check_proactive_alerts(llm_caller=llm_caller)
            if alerts:
                actions.append(f"alerts:{len(alerts)}")
        except Exception:
            pass

        # ── Layer 2: Evaluate pending interventions (every 10 min) ──
        if now - _watch_state.get("last_perf_sync", 0) > 600:
            _sync_ruleset_performance()
            try:
                evaluate_pending_interventions()
            except Exception:
                pass
            _watch_state["last_perf_sync"] = now

        # ── Layer 6: Cross-system health check (every 5 min) ──
        if now - _watch_state.get("last_health_check", 0) > 300:
            try:
                from oracle_memory import compute_health_score
                health = compute_health_score()
                score = health.get("score", 10)
                if score < 7:
                    actions.append(f"health:{score:.1f}/10")
                _watch_state["last_health_check"] = now
            except Exception:
                pass

        # ── Layer 3: Hive experiment driver (every 2 hours) ──
        if now - _watch_state.get("last_hive_experiment", 0) > 7200:
            try:
                from oracle_playbooks import drive_hive_experiments
                exp = drive_hive_experiments()
                if exp:
                    actions.append(f"hive_exp:{exp['trait']}")
                _watch_state["last_hive_experiment"] = now
            except Exception:
                pass

        # ── Auto Ruleset Management (every 4 hours, or 5 min after startup) ──
        ruleset_interval = 300 if _watch_state["last_ruleset_check"] == 0 else 14400
        if now - _watch_state["last_ruleset_check"] > ruleset_interval:
            generated = _auto_manage_rulesets()
            _watch_state["last_ruleset_check"] = now
            if generated:
                actions.append(f"rulesets:{generated}")

        # ── Persist memory to DB (every 10 min) ──
        if now - _watch_state.get("last_memory_persist", 0) > 600:
            try:
                persist_to_db()
                _watch_state["last_memory_persist"] = now
            except Exception:
                pass

    except Exception as e:
        print(f"  [ORACLE WATCH] Error: {e}")

    if actions:
        print(f"  [ORACLE WATCH] Actions: {', '.join(actions)}")


# ═══════════════════════════════════════════════════════════════════════════════
# HOOK 1: SIGNAL EMISSION
# ═══════════════════════════════════════════════════════════════════════════════

def _emit_signals(now) -> list:
    """Analyze cross-system patterns and emit signals to the signal bus."""
    actions = []

    try:
        # Get current system state
        enrichment = get_enrichment_cache() or {}
        engine_status = get_engine_status() or {}
        positions = get_open_positions() or {}
        freshness = get_enrichment_freshness() or {}

        # ── MACRO Regime Shift ──
        macro_entry = enrichment.get("MACRO", {})
        macro_result = macro_entry.get("result", {}) if isinstance(macro_entry, dict) else {}
        # Use direct field first, fall back to parsing compiler_output
        macro_verdict = (
            str(macro_result.get("directional_lean") or "").upper()
            or _extract_verdict(macro_result.get("compiler_output", ""))
        )
        if macro_verdict in ("BULLISH", "BEARISH", "NEUTRAL"):
            pass  # valid
        else:
            macro_verdict = None

        if macro_verdict and macro_verdict != _watch_state.get("last_macro"):
            if _watch_state.get("last_macro"):  # Don't emit on first run
                _emit_signal("ORACLE_REGIME_SHIFT", data={
                    "from": _watch_state["last_macro"],
                    "to": macro_verdict,
                    "reasoning": f"MACRO regime shifted from {_watch_state['last_macro']} to {macro_verdict}",
                }, ttl=30)
                actions.append(f"regime_shift:{macro_verdict}")
            _watch_state["last_macro"] = macro_verdict

        # ── Scalper Performance Signals ──
        try:
            scalper_stats = requests.get(f"{_nikita()}/api/scalper/stats", timeout=5).json()
            wr = scalper_stats.get("win_rate", 50)
            total = scalper_stats.get("total", 0)

            if total >= 20:
                if wr > 60:
                    _emit_signal("ORACLE_SCALPER_HOT", data={
                        "win_rate": wr, "trades": total,
                        "reasoning": f"Scalper on fire — {wr}% win rate over {total} trades",
                    }, ttl=60)
                    actions.append("scalper_hot")
                elif wr < 30:
                    _emit_signal("ORACLE_SCALPER_COLD", data={
                        "win_rate": wr, "trades": total,
                        "reasoning": f"Scalper struggling — {wr}% win rate over {total} trades",
                    }, ttl=15)
                    actions.append("scalper_cold")
        except Exception:
            pass

        # ── Timing Warning (only emit if none exists already) ──
        # Removed: was too noisy. The overall win rate check blocked ALL scalper trades.
        # TODO: Implement proper hourly win rate tracking per time bucket before re-enabling.

        # ── Enrichment Staleness → Trade Block (only for HIGH market engines) ──
        # Only block if critical modes (MACRO, CORRELATION) are stale — not all modes
        critical_stale = 0
        for mode in ("MACRO", "CORRELATION"):
            mode_data = freshness.get(mode, {})
            if isinstance(mode_data, dict) and not mode_data.get("fresh", True):
                critical_stale += 1
        if critical_stale >= 2:  # Both MACRO and CORRELATION stale
            # Only block hypothesis/swing, not scalper (scalper doesn't need enrichment)
            existing = get_cross_engine_signals()
            has_block = any(s.get("signal_type") == "ORACLE_TRADE_BLOCK" and s.get("data", {}).get("reason") == "enrichment_stale"
                          for s in (existing or []))
            if not has_block:
                _emit_signal("ORACLE_TRADE_BLOCK", data={
                    "reason": "enrichment_stale",
                    "stale_modes": critical_stale,
                    "reasoning": f"Critical enrichment modes stale — hypothesis/swing should wait",
                }, ttl=10)
                actions.append("trade_block:stale")

        # ── Portfolio Concentration → Trade Block ──
        pos_data = positions.get("positions", [])
        if len(pos_data) >= 3:
            # Check sector concentration
            sectors = {}
            for p in pos_data:
                sector = _get_sector(p.get("asset", ""))
                sectors[sector] = sectors.get(sector, 0) + p.get("amount", 0)
            total_exposure = sum(sectors.values()) or 1
            for sector, amount in sectors.items():
                if amount / total_exposure > 0.6:
                    _emit_signal("ORACLE_TRADE_BLOCK", asset=sector, data={
                        "reason": "concentration",
                        "sector": sector,
                        "pct": round(amount / total_exposure * 100, 1),
                        "reasoning": f"Sector {sector} concentration at {amount/total_exposure*100:.0f}%",
                    }, ttl=15)
                    actions.append(f"trade_block:concentration:{sector}")

        # ── Hypothesis Confidence Signal ──
        try:
            hyp_stats = engine_status.get("hypothesis_engine", {})
            if hyp_stats.get("win_rate", 0) > 60 and hyp_stats.get("trades", 0) >= 5:
                _emit_signal("ORACLE_HYPOTHESIS_CONFIDENCE", data={
                    "win_rate": hyp_stats["win_rate"],
                    "trades": hyp_stats["trades"],
                    "reasoning": f"Hypothesis engine performing well — {hyp_stats['win_rate']}% WR",
                }, ttl=60)
                actions.append("hypothesis_confidence")
        except Exception:
            pass

    except Exception as e:
        print(f"  [ORACLE SIGNALS] Error: {e}")

    return actions


def _emit_signal(signal_type, asset=None, chain=None, data=None, ttl=30):
    """Emit a signal to Nikita's cross-engine signal bus. Deduplicates — won't stack."""
    try:
        # Check if this signal type already exists (don't stack)
        existing = requests.get(f"{_nikita()}/api/engine-signals?type={signal_type}&engine=oracle", timeout=3)
        if existing.status_code == 200:
            active = existing.json()
            if isinstance(active, list) and len(active) > 0:
                return  # Signal already active, don't stack

        requests.post(f"{_nikita()}/api/engine-signals", json={
            "engine": "oracle",
            "signal_type": signal_type,
            "asset": asset,
            "chain": chain,
            "data": data or {},
            "ttl_minutes": ttl,
        }, timeout=5)
    except Exception as e:
        print(f"  [ORACLE] Signal emit failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# HOOK 2: INTELLIGENCE BRIEFING
# ═══════════════════════════════════════════════════════════════════════════════

def _write_intelligence_briefing():
    """Write a cross-system intelligence summary that The Llama injects into prompts."""
    try:
        portfolio = get_portfolio_summary() or {}
        if portfolio.get("error"):
            print(f"  [ORACLE BRIEFING] Skipping — Nikita unreachable")
            return
        engine_status = get_engine_status() or {}
        enrichment = get_enrichment_freshness() or {}
        guardian = get_guardian_status() or {}
        signals = get_cross_engine_signals() or []

        # Build summary text
        parts = []

        # Portfolio
        balance = portfolio.get("balance", 0)
        positions = portfolio.get("positions", 0)
        total_equity = portfolio.get("total_equity", balance)
        parts.append(f"Portfolio: ${total_equity:.0f} equity (${balance:.0f} cash + ${total_equity - balance:.0f} in positions), {positions} positions open.")

        # Engine performance
        hyp = engine_status.get("hypothesis_engine", {})
        if hyp.get("trades", 0) > 0:
            parts.append(f"Hypothesis engine: {hyp.get('win_rate', 0)}% WR over {hyp.get('trades', 0)} trades.")

        scalper = engine_status.get("scalper", {})
        if scalper.get("trades", 0) > 0:
            parts.append(f"Scalper: {scalper.get('trades', 0)} trades executed.")

        # MACRO
        macro_verdict = _watch_state.get("last_macro", "unknown")
        parts.append(f"MACRO regime: {macro_verdict}.")

        # Enrichment health
        fresh_count = sum(1 for d in enrichment.values() if isinstance(d, dict) and d.get("fresh"))
        total_count = len(enrichment)
        parts.append(f"Enrichment: {fresh_count}/{total_count} modes fresh.")

        # Signals
        if isinstance(signals, list) and signals:
            sig_types = [s.get("signal_type", "") for s in signals[:5]]
            parts.append(f"Active signals: {', '.join(sig_types)}.")

        # Guardian
        guardian_mods = guardian.get("recent_modifications", [])
        if isinstance(guardian_mods, list) and guardian_mods:
            parts.append(f"Guardian: {len(guardian_mods)} recent position modifications.")

        summary_text = " ".join(parts)

        # Write to the intelligence summary path that The Llama reads
        summary_path = Path("D:/mechanicias/intelligence_summary.json")
        with open(summary_path, "w") as f:
            json.dump({
                "summary_text": summary_text,
                "oracle_confidence": 0.75,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "source": "oracle",
            }, f, indent=2)

        db.add_observation("briefing_written", {"summary_length": len(summary_text)},
                          severity="info", action_taken="intelligence_briefing_updated")

    except Exception as e:
        print(f"  [ORACLE BRIEFING] Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# HOOK 3: SCALPER CONFIG TUNING
# ═══════════════════════════════════════════════════════════════════════════════

def _tune_scalper() -> str | None:
    """Analyze scalper/Hive meme performance. Nikita scalper disabled — this now monitors Hive."""
    try:
        stats = requests.get(f"{_nikita()}/api/scalper/stats", timeout=5).json()
        mode_data = requests.get(f"{_nikita()}/api/scalper/mode", timeout=5).json()
        macro_verdict = _watch_state.get("last_macro", "UNKNOWN")

        total = stats.get("total", 0)
        wr = stats.get("win_rate", 50)
        current_mode = mode_data.get("mode", "MOMENTUM")
        scan = stats.get("scan", {})
        cycles = scan.get("cycles_total", 0)

        new_mode = current_mode
        reason = ""

        # Starving (many cycles but 0 trades) → go AGGRESSIVE
        if cycles > 100 and total == 0:
            new_mode = "AGGRESSIVE"
            reason = f"starving ({cycles} cycles, 0 trades)"
        # Losing badly → go CONSERVATIVE
        elif total >= 10 and wr < 35:
            new_mode = "CONSERVATIVE"
            reason = f"losing (WR={wr}% over {total} trades)"
        # MACRO RISK_OFF → go CONSERVATIVE
        elif macro_verdict == "RISK_OFF":
            new_mode = "CONSERVATIVE"
            reason = f"MACRO RISK_OFF"
        # Winning well → go MOMENTUM (ride the wave)
        elif total >= 10 and wr > 60:
            new_mode = "MOMENTUM"
            reason = f"hot streak (WR={wr}%)"
        # Default balanced
        elif total >= 5 and 40 <= wr <= 60:
            new_mode = "MOMENTUM"
            reason = "balanced performance"

        if new_mode != current_mode:
            requests.post(f"{_nikita()}/api/scalper/mode",
                          json={"mode": new_mode}, timeout=5)
            db.add_observation("scalper_mode_switch", {
                "from": current_mode,
                "to": new_mode,
                "reason": reason,
                "macro": macro_verdict,
                "win_rate": wr,
                "total_trades": total,
            }, engine="scalper", severity="info",
                action_taken=f"Mode: {current_mode} → {new_mode} ({reason})")
            print(f"  [ORACLE] Scalper mode: {current_mode} → {new_mode} ({reason})")
            return f"{current_mode}→{new_mode}"

    except Exception as e:
        print(f"  [ORACLE SCALPER TUNE] Error: {e}")

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HOOK 6: ENRICHMENT CACHE WRITE
# ═══════════════════════════════════════════════════════════════════════════════

def _write_enrichment_cache():
    """Write ORACLE_BRIEFING to The Llama's enrichment cache."""
    try:
        portfolio = get_portfolio_summary() or {}
        engine_status = get_engine_status() or {}
        macro_verdict = _watch_state.get("last_macro", "UNKNOWN")

        briefing = {
            "compiler_output": (
                f"VERDICT: {'CAUTIOUS' if macro_verdict in ('RISK_OFF', 'BEARISH') else 'NEUTRAL'}\n"
                f"Portfolio: ${portfolio.get('balance', 0):.0f}, {portfolio.get('positions', 0)} positions. "
                f"MACRO: {macro_verdict}. "
                f"Hypothesis: {engine_status.get('hypothesis_engine', {}).get('win_rate', '?')}% WR. "
            ),
            "directional_lean": "NEUTRAL",
            "confidence": "MEDIUM",
        }

        requests.post(f"{_llama()}/cache/enrichment", json={
            "mode": "ORACLE_BRIEFING",
            "result": briefing,
            "freshness_minutes": 35,
        }, timeout=5)

    except Exception as e:
        # Endpoint may not exist yet — that's OK
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# HOOK 7: HYPOTHESIS THRESHOLD TUNING
# ═══════════════════════════════════════════════════════════════════════════════

def _tune_hypothesis() -> str | None:
    """Analyze hypothesis engine performance and adjust thresholds."""
    try:
        engine_status = get_engine_status() or {}
        hyp = engine_status.get("hypothesis_engine", {})
        macro_verdict = _watch_state.get("last_macro", "UNKNOWN")

        trades = hyp.get("trades", 0)
        wr = hyp.get("win_rate", 50)

        if trades < 5:
            return None  # Not enough data

        adjustments = {}
        reasons = []

        # MACRO RISK_ON → loosen thresholds
        if macro_verdict == "RISK_ON":
            adjustments["min_alignment_total"] = 1.8
            reasons.append("RISK_ON: min_alignment 2.0→1.8")

        # MACRO RISK_OFF → tighten
        elif macro_verdict in ("RISK_OFF", "BEARISH"):
            adjustments["min_alignment_total"] = 2.5
            reasons.append("RISK_OFF: min_alignment 2.0→2.5")

        # Winning streak → loosen
        if wr > 65 and trades >= 10:
            adjustments["min_bars_green"] = 4
            reasons.append(f"winning({wr}%): min_bars 5→4")

        # Losing streak → tighten
        elif wr < 30 and trades >= 10:
            adjustments["min_bars_green"] = 6
            reasons.append(f"losing({wr}%): min_bars 5→6")

        if adjustments:
            # Advisory mode: log recommendation, don't push config
            db.add_observation("hypothesis_tuned", {
                "adjustments": adjustments,
                "reasons": reasons,
                "macro": macro_verdict,
                "win_rate": wr,
                "advisory": True,
            }, engine="hypothesis", severity="info",
                action_taken=f"Advisory: recommends hypothesis config: {', '.join(reasons)}")
            return ", ".join(reasons)

    except Exception as e:
        print(f"  [ORACLE HYPOTHESIS TUNE] Error: {e}")

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# AUTO RULESET MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def _sync_ruleset_performance():
    """Sync trade performance into ruleset_performance table.

    Pulls recent trade data from Nikita and Mechanicus, maps trades
    to the active ruleset for each engine, and records aggregate stats.
    """
    try:
        # Get active rulesets
        active_rulesets = db.query(
            "SELECT ruleset_id, engine, activated_at FROM rulesets WHERE status = 'active'"
        )
        if not active_rulesets:
            return

        # Get recent trades from Nikita
        trade_data = get_trade_history() or {}
        trades = trade_data.get("trades", [])
        if not trades:
            return

        # Get scalper stats
        scalper_stats = get_scalper_current_ruleset() or {}

        for ruleset in active_rulesets:
            engine = ruleset.get("engine", "")
            ruleset_id = ruleset.get("ruleset_id", "")

            # Count trades by engine
            engine_trades = []
            for t in trades:
                source = t.get("source", "")
                if engine == "scalper" and "scalper" in source:
                    engine_trades.append(t)
                elif engine == "hypothesis" and source in ("executor", "mechanicus"):
                    engine_trades.append(t)
                elif engine == "swing" and "swing" in source:
                    engine_trades.append(t)
                elif engine == "sniper" and "sniper" in source:
                    engine_trades.append(t)

            if not engine_trades and engine != "scalper":
                continue

            # For scalper, use scalper stats directly (more accurate)
            if engine == "scalper" and scalper_stats:
                total = scalper_stats.get("total", 0)
                wins = scalper_stats.get("wins", 0)
                pnl = scalper_stats.get("total_pnl_usd", 0)
            else:
                total = len(engine_trades)
                wins = sum(1 for t in engine_trades if t.get("pnl", 0) > 0)
                pnl = sum(t.get("pnl", 0) for t in engine_trades)

            if total == 0:
                continue

            # Upsert into ruleset_performance
            existing = db.query(
                "SELECT id FROM ruleset_performance WHERE ruleset_id = ? AND engine = ?",
                (ruleset_id, engine), one=True,
            )
            if existing:
                db.execute(
                    """UPDATE ruleset_performance SET trades_taken=?, wins=?, losses=?,
                       win_rate=?, total_pnl=?, macro_regime=?
                       WHERE ruleset_id=? AND engine=?""",
                    (total, wins, total - wins,
                     round(wins / max(total, 1) * 100, 1), round(pnl, 2),
                     _watch_state.get("last_macro", "UNKNOWN"),
                     ruleset_id, engine),
                )
            else:
                db.execute(
                    """INSERT INTO ruleset_performance
                       (ruleset_id, engine, trades_taken, wins, losses, win_rate, total_pnl, macro_regime)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ruleset_id, engine, total, wins, total - wins,
                     round(wins / max(total, 1) * 100, 1), round(pnl, 2),
                     _watch_state.get("last_macro", "UNKNOWN")),
                )

        print(f"  [ORACLE PERF SYNC] Updated {len(active_rulesets)} rulesets, {len(trades)} trades analyzed")

    except Exception as e:
        import traceback
        print(f"  [ORACLE PERF SYNC] Error: {e}")
        traceback.print_exc()


def _auto_manage_rulesets() -> str | None:
    """Automatically generate/refresh rulesets for all engines. Runs every 4 hours."""
    engines = ["scalper", "hypothesis", "sniper", "swing"]
    generated = []

    for engine in engines:
        try:
            # Check if engine has an active ruleset
            active = db.query(
                "SELECT * FROM rulesets WHERE engine = ? AND status = 'active' ORDER BY id DESC LIMIT 1",
                (engine,), one=True,
            )

            needs_new = False
            reason = ""

            if not active:
                needs_new = True
                reason = "no_active_ruleset"
            else:
                # Check TTL
                generated_at = active.get("generated_at", "")
                ttl_hours = active.get("ttl_hours", 24)
                try:
                    import datetime
                    gen_time = datetime.datetime.strptime(generated_at, "%Y-%m-%d %H:%M:%S")
                    age_hours = (datetime.datetime.now() - gen_time).total_seconds() / 3600
                    if age_hours > ttl_hours:
                        needs_new = True
                        reason = f"expired ({age_hours:.0f}h old, TTL={ttl_hours}h)"
                except Exception:
                    pass

            if needs_new:
                ruleset = _generate_ruleset_for_engine(engine, reason)
                if ruleset:
                    generated.append(engine)

        except Exception as e:
            print(f"  [ORACLE RULESET] Error managing {engine}: {e}")

    if generated:
        return ", ".join(generated)
    return None


def _get_hive_insights() -> dict:
    """Pull latest Hive report data for ruleset intelligence."""
    try:
        r = requests.get(f"{_nikita()}/api/hive/report", timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def _get_recent_proactive_analysis() -> str:
    """Get the most recent proactive LLM analysis from observations."""
    rows = db.query(
        "SELECT data FROM observations WHERE type = 'proactive_analysis' ORDER BY id DESC LIMIT 1"
    )
    if rows:
        try:
            data = json.loads(rows[0].get("data", "{}"))
            return data.get("analysis", "")
        except Exception:
            pass
    return ""


def _generate_ruleset_for_engine(engine: str, reason: str) -> dict | None:
    """Generate intelligent ruleset using LLM + system data + Hive insights + proactive analysis."""
    try:
        print(f"  [ORACLE RULESET] Generating ruleset for {engine} (reason: {reason})")

        # Assemble all available context
        context = assemble_generation_context(engine)
        macro_verdict = _watch_state.get("last_macro", "UNKNOWN")
        import datetime
        time_bucket = "ASIA" if 0 <= datetime.datetime.utcnow().hour < 8 else \
                      "EUROPE" if 8 <= datetime.datetime.utcnow().hour < 16 else "US"

        # Get engine-specific current config
        if engine == "scalper":
            current_config = get_scalper_current_ruleset() or {}
        elif engine == "hypothesis":
            engine_status = get_engine_status() or {}
            current_config = engine_status.get("hypothesis_engine", {})
        else:
            current_config = {}

        # Pull Hive trait insights (200 agents' experience)
        hive_data = _get_hive_insights()
        hive_trait_insights = hive_data.get("trait_insights", {})
        hive_recommendations = hive_data.get("recommended_config", {})
        hive_consensus = hive_data.get("consensus", {})
        hive_engine_perf = hive_data.get("engine_performance", {})

        # Pull recent proactive LLM analysis
        proactive = _get_recent_proactive_analysis()

        # Get performance of previous ruleset
        perf_rows = db.query(
            "SELECT * FROM ruleset_performance WHERE engine = ? ORDER BY id DESC LIMIT 1",
            (engine,),
        )
        prev_perf = perf_rows[0] if perf_rows else {}

        # ── LLM-POWERED RULE GENERATION ──
        llm_recommendations = []
        try:
            from oracle_llm import call_qwen

            data_block = json.dumps({
                "engine": engine,
                "current_config": current_config,
                "macro": macro_verdict,
                "session": time_bucket,
                "reason_for_regeneration": reason,
                "previous_ruleset_performance": {
                    "win_rate": prev_perf.get("win_rate"),
                    "total_pnl": prev_perf.get("total_pnl"),
                    "trades": prev_perf.get("trades_taken"),
                },
                "hive_trait_insights": hive_trait_insights,
                "hive_recommendations": hive_recommendations.get(engine, {}),
                "hive_engine_performance": hive_engine_perf,
                "recent_analysis": proactive[:1000] if proactive else "none",
            }, indent=2, default=str)[:3000]

            prompt = f"""Generate 3-5 specific configuration rules for the {engine} engine.

CONTEXT:
{data_block}

Return ONLY a JSON array of rule objects, each with:
- "rule": specific action (e.g. "set min_5m_change to 3.0")
- "priority": "high", "medium", or "low"
- "reasoning": why, backed by data
- "confidence": 0.0-1.0

Rules must reference actual config parameters. High priority = apply immediately.
Medium priority = apply if confirmed by another cycle. Low = observe only.
Consider the Hive data — 200 agents have been testing strategies."""

            system = "You are ORACLE, generating trading rules. Return valid JSON array only. No markdown."

            response = call_qwen(prompt, system, temperature=0.2, max_tokens=1500)

            if response:
                # Parse JSON from response
                # Strip any markdown fences
                clean = response.strip()
                if clean.startswith("```"):
                    clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
                    if "```" in clean:
                        clean = clean[:clean.rfind("```")]
                try:
                    parsed = json.loads(clean)
                    if isinstance(parsed, list):
                        llm_recommendations = parsed
                        print(f"  [ORACLE RULESET] LLM generated {len(llm_recommendations)} rules for {engine}")
                except json.JSONDecodeError:
                    print(f"  [ORACLE RULESET] LLM response not valid JSON, using fallback rules")

        except Exception as e:
            print(f"  [ORACLE RULESET] LLM generation failed: {e}")

        # Fallback: static recommendations if LLM failed
        static_recs = _build_recommendations(engine, context, macro_verdict)

        # Merge: LLM rules first, then static as fallback
        all_recommendations = llm_recommendations + [
            r for r in static_recs
            if not any(lr.get("rule", "").lower()[:20] == r.get("rule", "").lower()[:20] for lr in llm_recommendations)
        ]

        # Build the ruleset
        ruleset_data = {
            "config_snapshot": current_config,
            "market_context": {
                "macro": macro_verdict,
                "time_bucket": time_bucket,
                "generated_reason": reason,
            },
            "recommendations": all_recommendations,
            "hive_insights": {
                "trait_insights": hive_trait_insights,
                "engine_recommendation": hive_recommendations.get(engine, {}),
                "engine_performance": hive_engine_perf,
            },
            "llm_generated": len(llm_recommendations) > 0,
        }

        # Store in DB
        ruleset_id = f"{engine}_v{int(time.time())}"
        version = time.strftime("%Y%m%d_%H%M")

        db.execute(
            "UPDATE rulesets SET status = 'archived', archived_at = datetime('now') WHERE engine = ? AND status = 'active'",
            (engine,),
        )
        db.execute(
            """INSERT INTO rulesets (ruleset_id, engine, version, status, market_context, rules, reasoning, ttl_hours, activated_at)
            VALUES (?, ?, ?, 'active', ?, ?, ?, 24, datetime('now'))""",
            (ruleset_id, engine, version,
             json.dumps({"macro": macro_verdict, "time_bucket": time_bucket}),
             json.dumps(ruleset_data),
             f"{'LLM-generated' if llm_recommendations else 'Auto-generated'}: {reason}. MACRO={macro_verdict}, session={time_bucket}"),
        )

        db.add_observation("ruleset_generated", {
            "engine": engine,
            "ruleset_id": ruleset_id,
            "reason": reason,
            "macro": macro_verdict,
            "llm_rules": len(llm_recommendations),
            "static_rules": len(static_recs),
            "hive_data": bool(hive_trait_insights),
        }, engine=engine, severity="info",
            action_taken=f"Generated ruleset {ruleset_id} for {engine} ({len(all_recommendations)} rules)")

        print(f"  [ORACLE RULESET] Created {ruleset_id} for {engine} "
              f"({len(all_recommendations)} rules, LLM={'yes' if llm_recommendations else 'no'}, "
              f"Hive={'yes' if hive_trait_insights else 'no'})")

        # Push recommendations to engine configs
        _apply_recommendations(engine, all_recommendations, macro_verdict)

        return ruleset_data

    except Exception as e:
        import traceback
        print(f"  [ORACLE RULESET] Generation failed for {engine}: {e}")
        traceback.print_exc()
        return None


def _run_proactive_analysis():
    """Use Qwen to analyze system patterns and propose improvements."""
    try:
        from oracle_llm import call_qwen

        # Gather system snapshot
        engine_status = get_engine_status() or {}
        portfolio = get_portfolio_summary() or {}
        enrichment = get_enrichment_freshness() or {}
        signals = get_cross_engine_signals() or []
        patterns = get_pattern_learner_insights() or {}

        snapshot = json.dumps({
            "engines": engine_status,
            "portfolio": portfolio,
            "enrichment_freshness": enrichment,
            "active_signals": len(signals) if isinstance(signals, list) else 0,
            "learned_rules": patterns.get("learned_rules", [])[:5],
            "scalper_rules": patterns.get("scalper_rules", [])[:5],
            "macro": _watch_state.get("last_macro", "UNKNOWN"),
        }, indent=2, default=str)[:3000]

        prompt = f"""Analyze this trading system snapshot. Identify the top 1-2 actionable improvements.

SYSTEM SNAPSHOT:
{snapshot}

For each improvement, state:
1. WHAT to change (specific config parameter or behavior)
2. WHY (backed by data from the snapshot)
3. RISK level (low/medium/high)
4. CONFIDENCE (0.0-1.0)

Be specific. Reference numbers. Only suggest changes backed by the data."""

        system = "You are ORACLE, analyzing a crypto trading system. Be concise and data-driven. No speculation."

        response = call_qwen(prompt, system, temperature=0.3, max_tokens=1500)

        if response and not response.startswith("[ORACLE ERROR"):
            # Store as observation
            db.add_observation("proactive_analysis", {
                "analysis": response[:2000],
                "macro": _watch_state.get("last_macro"),
            }, severity="info", action_taken="LLM analysis completed")

            # Parse for auto-applicable low-risk suggestions
            _apply_proactive_suggestions(response)

            print(f"  [ORACLE PROACTIVE] Analysis complete ({len(response)} chars)")

    except Exception as e:
        print(f"  [ORACLE PROACTIVE] Error: {e}")


def _apply_proactive_suggestions(analysis: str):
    """Parse LLM analysis and auto-apply low-risk, high-confidence suggestions."""
    # Look for specific actionable patterns in the response
    analysis_lower = analysis.lower()

    try:
        # Advisory mode: log LLM recommendations, don't push configs
        if "tighten" in analysis_lower and "scalper" in analysis_lower and "stop" in analysis_lower:
            db.add_observation("proactive_advisory", {
                "recommendation": "Widen scalper stop loss",
                "source": "llm_analysis",
            }, engine="scalper", severity="info",
                action_taken="Advisory: LLM recommends widening scalper SL")

        if "reduce" in analysis_lower and "exposure" in analysis_lower and "concurrent" in analysis_lower:
            db.add_observation("proactive_advisory", {
                "recommendation": "Reduce scalper concurrent positions",
                "source": "llm_analysis",
            }, engine="scalper", severity="info",
                action_taken="Advisory: LLM recommends reducing scalper concurrent")

    except Exception as e:
        print(f"  [ORACLE PROACTIVE APPLY] Error: {e}")


def _apply_recommendations(engine: str, recommendations: list, macro: str):
    """Push ruleset recommendations to engine configs.
    High priority: apply immediately. Medium: apply if confidence >= 0.6."""
    try:
        adjustments = {}

        # Safe ranges to prevent dangerous values
        SAFE_RANGES = {
            "min_5m_change": (1.0, 8.0),
            "max_concurrent_scalps": (1, 5),
            "min_alignment_total": (1.5, 3.0),
            "min_bars_green": (3, 7),
            "max_hold_seconds": (30, 120),
            "stop_loss_pct": (1.5, 8.0),
        }

        for rec in recommendations:
            rule = rec.get("rule", "")
            priority = rec.get("priority", "low")
            confidence = rec.get("confidence", 0.5)

            # High: always apply. Medium: apply if confident. Low: skip.
            if priority == "low":
                continue
            if priority == "medium" and confidence < 0.6:
                continue

            # ── Parse LLM-generated rules: look for "set X to Y" patterns ──
            set_match = re.search(r'set\s+(\w+)\s+to\s+([\d.]+)', rule, re.IGNORECASE)
            if set_match:
                param = set_match.group(1)
                value = float(set_match.group(2))
                # Safety: clamp to safe range
                if param in SAFE_RANGES:
                    lo, hi = SAFE_RANGES[param]
                    value = max(lo, min(hi, value))
                    if isinstance(lo, int):
                        value = int(value)
                    adjustments[param] = value
                continue

            # ── Parse static rule patterns ──
            if engine == "scalper":
                if "min_5m_change" in rule and "higher" in rule:
                    adjustments["min_5m_change"] = 4.0
                elif "concurrent scalps to 1" in rule or "reduce concurrent" in rule.lower():
                    adjustments["max_concurrent_scalps"] = 1
                elif "concurrent scalps to 3" in rule or "increase concurrent" in rule.lower():
                    adjustments["max_concurrent_scalps"] = 3

            elif engine == "hypothesis":
                if "min_alignment_total" in rule:
                    val_match = re.search(r'(\d+\.?\d*)', rule)
                    if val_match:
                        val = float(val_match.group(1))
                        adjustments["min_alignment_total"] = max(1.5, min(3.0, val))
                elif "min_bars_green" in rule:
                    val_match = re.search(r'(\d+)', rule)
                    if val_match:
                        adjustments["min_bars_green"] = max(3, min(7, int(val_match.group(1))))

        if not adjustments:
            return

        # Advisory mode: log recommendation, don't push config
        db.add_observation("ruleset_applied", {
            "engine": engine,
            "adjustments": adjustments,
            "macro": macro,
            "advisory": True,
            "rules_applied": len(adjustments),
        }, engine=engine, severity="info",
            action_taken=f"Applied {len(adjustments)} config changes: {json.dumps(adjustments)}")

        print(f"  [ORACLE RULESET] Applied to {engine}: {adjustments}")

    except Exception as e:
        print(f"  [ORACLE RULESET] Apply failed for {engine}: {e}")


def _build_recommendations(engine: str, context: dict, macro: str) -> list:
    """Build rule recommendations based on context. No LLM needed — pure data."""
    recs = []

    # Performance-based recommendations
    perf = context.get("recent_performance", {})
    wr = perf.get("win_rate", 50) if isinstance(perf, dict) else 50

    if engine == "scalper":
        if macro in ("RISK_OFF", "BEARISH"):
            recs.append({"rule": "Tighten entry: min_5m_change should be higher in bearish macro", "priority": "high"})
        if wr < 35:
            recs.append({"rule": "Reduce concurrent scalps to 1 — cold streak", "priority": "high"})
        if wr > 60:
            recs.append({"rule": "Increase concurrent scalps to 3 — hot streak", "priority": "medium"})

    elif engine == "hypothesis":
        if macro in ("RISK_OFF", "BEARISH"):
            recs.append({"rule": "Raise min_alignment_total to 2.5 — hostile macro", "priority": "high"})
            recs.append({"rule": "Consider only BULLISH leans with HIGH confidence", "priority": "medium"})
        if wr < 35:
            recs.append({"rule": "Raise min_bars_green to 6 — losing streak", "priority": "high"})

    elif engine == "sniper":
        if macro in ("RISK_OFF", "BEARISH"):
            recs.append({"rule": "Market context penalty active — new tokens face headwinds", "priority": "medium"})
        recs.append({"rule": f"Current ALPHA threshold at 50 — monitor score distribution", "priority": "low"})

    elif engine == "swing":
        if macro in ("RISK_OFF", "BEARISH"):
            recs.append({"rule": "Swing engine should wait for RISK_ON or NEUTRAL macro", "priority": "high"})
        recs.append({"rule": "Require HIGH confidence + ACT_NOW posture for entry", "priority": "medium"})

    return recs


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_verdict(text):
    """Extract VERDICT or DIRECTIONAL_LEAN from enrichment mode output."""
    if not text:
        return None
    # Try VERDICT first, then DIRECTIONAL_LEAN
    for pattern in [r"VERDICT:\s*(\S+)", r"DIRECTIONAL_LEAN:\s*(\S+)"]:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).upper()
    return None


def _get_sector(asset):
    """Map asset to sector."""
    sectors = {
        "BTC": "L1", "ETH": "L1", "SOL": "L1", "AVAX": "L1", "DOT": "L1",
        "ATOM": "L1", "NEAR": "L1", "APT": "L1", "SUI": "L1", "SEI": "L1",
        "AAVE": "DeFi", "MKR": "DeFi", "UNI": "DeFi", "LINK": "DeFi",
        "DOGE": "Meme", "SHIB": "Meme", "PEPE": "Meme", "WIF": "Meme", "BONK": "Meme",
    }
    return sectors.get(asset, "Other")


def assemble_generation_context(engine: str) -> dict:
    """Gather all context needed to generate a ruleset."""
    return {
        "current_macro": get_enrichment_cache("MACRO"),
        "current_regime": get_regime_state(),
        "recent_performance": get_win_rates(engine, "24h"),
        "pattern_insights": get_pattern_learner_insights(),
        "enrichment_accuracy": get_enrichment_accuracy(),
        "active_signals": get_cross_engine_signals(),
        "active_directives": get_active_directives(),
        "historical_rulesets": db.get_ruleset_history(engine, limit=5),
    }


def get_active_rulesets() -> list:
    return db.get_active_rulesets()
