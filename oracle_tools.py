"""ORACLE Tools — functions Qwen can call to query live data from the Nikita Network.

All tools return dicts. All handle connection errors gracefully.
"""

import json
import time
import threading
import sys
import requests
from pathlib import Path


# Thread-local context for tracking
_context = threading.local()

def set_tool_context(session_id=None, llm_instance=None):
    _context.session_id = session_id
    _context.llm_instance = llm_instance

def get_tool_context():
    return {
        "session_id": getattr(_context, "session_id", None),
        "llm_instance": getattr(_context, "llm_instance", None),
    }

def track_tool(tier="read"):
    """Decorator that logs every tool call for telemetry."""
    def decorator(func):
        import functools
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import time
            start = time.time()
            success = True
            error = None
            result = None
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                latency = int((time.time() - start) * 1000)
                size = sys.getsizeof(result) if result else 0
                ctx = get_tool_context()
                try:
                    import oracle_db
                    oracle_db.log_tool_call(
                        func.__name__, tier, ctx.get("session_id"),
                        latency, success, size, ctx.get("llm_instance"), error
                    )
                except Exception:
                    pass
        return wrapper
    return decorator


def write_tool(safe_ranges=None):
    """Decorator for write tools. Checks parameters against safe ranges.
    If in range: execute immediately (autonomous).
    If out of range: create approval request (pending).
    """
    def decorator(func):
        import functools
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Load safe ranges from config
            ranges = safe_ranges or {}
            try:
                config_path = Path(__file__).parent / "write_config.json"
                if config_path.exists():
                    with open(config_path) as f:
                        all_ranges = json.load(f)
                    # Merge config ranges (they override hardcoded)
                    # The caller should pass the engine section key
            except Exception:
                pass

            # Check if any param is outside safe range
            out_of_range = False
            for param_name, value in kwargs.items():
                if param_name in ranges:
                    r = ranges[param_name]
                    if "min" in r and "max" in r:
                        if value < r["min"] or value > r["max"]:
                            out_of_range = True
                            break
                    elif "allowed" in r:
                        if value not in r["allowed"]:
                            out_of_range = True
                            break

            if out_of_range:
                # Queue for approval
                import oracle_db
                proposal_id = oracle_db.create_approval_request(
                    func.__name__, kwargs, "Parameter outside safe range",
                    str(ranges), str(kwargs)
                )
                oracle_db.log_write_action(func.__name__, kwargs, "approval_required", "pending", {}, kwargs)
                return {"status": "pending_approval", "proposal_id": proposal_id, "reason": "Parameter outside safe range"}

            # In range — execute autonomously
            result = func(*args, **kwargs)

            # Log the write action
            try:
                import oracle_db
                oracle_db.log_write_action(func.__name__, kwargs, "autonomous", "success", {}, kwargs)
            except Exception:
                pass

            return result
        return wrapper
    return decorator


# Load config
_config = {}
_config_path = Path(__file__).parent / "oracle_config.json"


def _load_config():
    global _config
    with open(_config_path) as f:
        _config = json.load(f)


def _get(service: str, path: str, timeout: int = 30) -> dict | list | None:
    """HTTP GET to a service. Returns parsed JSON or None on failure."""
    if not _config:
        _load_config()
    base_url = _config.get("services", {}).get(service, {}).get("base_url", "")
    if not base_url:
        return {"error": f"Unknown service: {service}"}
    try:
        r = requests.get(f"{base_url}{path}", timeout=timeout)
        if r.ok:
            return r.json()
        return {"error": f"HTTP {r.status_code}", "detail": r.text[:200]}
    except Exception as e:
        return {"error": f"{service} unreachable: {e}"}


def _post(service: str, path: str, data: dict, timeout: int = 30) -> dict | None:
    """HTTP POST to a service."""
    if not _config:
        _load_config()
    base_url = _config.get("services", {}).get(service, {}).get("base_url", "")
    try:
        r = requests.post(f"{base_url}{path}", json=data, timeout=timeout)
        if r.ok:
            return r.json()
        return {"error": f"HTTP {r.status_code}", "detail": r.text[:200]}
    except Exception as e:
        return {"error": f"{service} unreachable: {e}"}


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 1 — Llama Interface
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_coin(coin: str, mode: str = "DECISION", priority: str = "REALTIME") -> dict:
    """Submit a coin for analysis to The Llama."""
    return _post("llama", "/analyze", {
        "mode": mode,
        "priority": priority,
        "caller": "oracle",
        "context": {"asset": coin, "timeframe": "1H", "caller": "oracle"},
        "payload": {},  # Llama enriches from its own cache
    }, timeout=120)


def get_enrichment_cache(mode: str = None) -> dict:
    """Get cached enrichment data from The Llama."""
    if mode:
        data = _get("llama", "/cache/enrichment")
        if isinstance(data, dict) and mode in data:
            return data[mode]
        return {"error": f"Mode {mode} not in cache"}
    return _get("llama", "/cache/enrichment")


def get_enrichment_freshness() -> dict:
    """Check which enrichment modes are fresh vs stale."""
    cache = _get("llama", "/cache/enrichment")
    if not isinstance(cache, dict) or "error" in cache:
        return cache
    summary = {}
    for mode, entry in cache.items():
        age = entry.get("age_minutes", 999)
        fresh = entry.get("is_fresh", False)
        score = max(0, min(100, int(100 - age * 2))) if age < 50 else 0
        summary[mode] = {"fresh": fresh, "age_minutes": round(age, 1), "score": score}
    return summary


def get_llama_queue_status() -> dict:
    """Get current Llama queue depth and priority breakdown."""
    return _get("llama", "/queue/status")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 2 — Engine Control
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine_status() -> dict:
    """Get state of all trading engines and feature toggles."""
    dashboard = _get("mechanicus", "/api/dashboard")
    if not dashboard or "error" in dashboard:
        return dashboard or {"error": "Dashboard unreachable"}

    config = dashboard.get("executor_config", {})
    scalper = dashboard.get("scalper_config", {})

    return {
        "hypothesis_engine": {
            "armed": config.get("enabled", False),
            "trades": dashboard.get("executor_stats", {}).get("total_trades", 0),
            "win_rate": dashboard.get("executor_stats", {}).get("win_rate", 0),
        },
        "scalper": {
            "armed": scalper.get("enabled", False),
            "trades": dashboard.get("scalper_stats", {}).get("total", 0),
        },
        "scan_summary": dashboard.get("executor_scan_summary", {}),
        "regime": dashboard.get("regime", {}),
    }


def toggle_engine(engine_name: str, enabled: bool) -> dict:
    """Toggle a feature on or off via Mechanicus."""
    if engine_name in ("hypothesis_engine", "executor"):
        return _post("mechanicus", "/api/executor/config", {"enabled": enabled})
    elif engine_name == "meme_scalper":
        return _post("nikita", "/api/scalper/config", {"enabled": enabled})
    elif engine_name == "position_guardian":
        return _post("nikita", "/api/guardian/config", {"enabled": enabled})
    return {"error": f"Unknown engine: {engine_name}"}


def get_cross_engine_signals() -> dict:
    """Get all active cross-engine signals."""
    return _get("nikita", "/api/engine-signals")


def get_active_directives() -> dict:
    """Get active Portfolio Manager directives."""
    return _get("mechanicus", "/api/portfolio/directives")


def get_regime_state() -> dict:
    """Get current market regime from Mechanicus."""
    dashboard = _get("mechanicus", "/api/dashboard")
    if dashboard and "regime" in dashboard:
        return dashboard["regime"]
    return {"error": "Regime data unavailable"}


def get_module_status() -> dict:
    """Get last run time and status for all Mechanicus modules."""
    dashboard = _get("mechanicus", "/api/dashboard")
    if not dashboard:
        return {"error": "Mechanicus unreachable"}
    return {
        "market_cycle": dashboard.get("market_cycle_status", {}),
        "health": dashboard.get("health", {}),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 3 — Portfolio & Signals
# ═══════════════════════════════════════════════════════════════════════════════

def get_open_positions() -> dict:
    """Get all open positions across all engines."""
    snapshot = _get("nikita", "/api/snapshot")
    if not snapshot or "error" in snapshot:
        return snapshot or {"error": "Nikita unreachable"}
    positions = snapshot.get("positions", [])
    assets = snapshot.get("assets", {})
    result = []
    for pos in positions:
        asset_sym = pos.get("asset", "")
        entry = pos.get("entry", 0)
        current = assets.get(asset_sym, {}).get("price", 0)
        pnl_pct = ((current - entry) / entry * 100) if entry > 0 else 0
        result.append({
            "asset": asset_sym,
            "side": pos.get("side", "buy"),
            "entry": round(entry, 6),
            "current": round(current, 6),
            "pnl_pct": round(pnl_pct, 2),
            "amount": pos.get("amount", 0),
            "engine": pos.get("engine", "manual"),
            "guardian_enabled": pos.get("guardian_enabled", False),
        })
    return {"positions": result, "count": len(result), "balance": snapshot.get("balance", 0)}


def get_portfolio_summary() -> dict:
    """Get portfolio value, drawdown, win rates per engine."""
    snapshot = _get("nikita", "/api/snapshot")
    if not snapshot:
        return {"error": "Nikita unreachable"}
    positions = snapshot.get("positions", [])
    balance = snapshot.get("balance", 0)
    exposure = sum(p.get("amount", 0) for p in positions)
    return {
        "balance": round(balance, 2),
        "positions": len(positions),
        "exposure": round(exposure, 2),
        "total_equity": round(balance + exposure, 2),
    }


def get_trade_history(engine: str = "all", limit: int = 20) -> dict:
    """Get recent closed trades."""
    return _get("nikita", f"/api/pnl/history?period=24h")


def get_win_rates(engine: str = "all", timeframe: str = "24h") -> dict:
    """Get win rates by engine."""
    data = _get("nikita", f"/api/pnl/history?period={timeframe}")
    if data and "trades" in data:
        return {
            "total_trades": data.get("total_trades", 0),
            "win_rate": data.get("win_rate", 0),
            "total_pnl": data.get("total_pnl", 0),
        }
    return data


def get_guardian_status() -> dict:
    """Get Position Guardian status and recent modifications."""
    config = _get("nikita", "/api/guardian/config")
    log = _get("nikita", "/api/guardian/log")
    return {"config": config, "recent_modifications": log}


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 4 — Debug & Inspect
# ═══════════════════════════════════════════════════════════════════════════════

def get_enrichment_accuracy() -> dict:
    """Get per-mode enrichment accuracy scores."""
    return _get("mechanicus", "/api/dashboard")  # accuracy in dashboard data


def get_pattern_learner_insights() -> dict:
    """Get recent Pattern Learner discoveries."""
    dashboard = _get("mechanicus", "/api/dashboard")
    if dashboard:
        return {
            "learned_rules": dashboard.get("executor_learned_rules", []),
            "scalper_rules": dashboard.get("scalper_rules", []),
        }
    return {"error": "Mechanicus unreachable"}


def get_scalp_fingerprints() -> dict:
    """Get top winning and losing 8-dimension fingerprints."""
    dashboard = _get("mechanicus", "/api/dashboard")
    if dashboard:
        return {"scalper_rules": dashboard.get("scalper_rules", [])}
    return {"error": "Mechanicus unreachable"}


def get_deployer_dna(address: str) -> dict:
    """Get deployer reputation data."""
    return _get("mechanicus", f"/api/deployer/{address}")


def get_hypothesis_history(limit: int = 20) -> dict:
    """Get recent hypothesis engine strikes and outcomes."""
    return _get("mechanicus", f"/api/executor/trades?limit={limit}")


def inspect_trade(trade_id: str) -> dict:
    """Full breakdown of a specific trade."""
    trades = _get("mechanicus", f"/api/executor/trades?limit=50")
    if isinstance(trades, list):
        for t in trades:
            if str(t.get("id")) == str(trade_id):
                return t
    return {"error": f"Trade {trade_id} not found"}


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 5 — Ruleset Tools
# ═══════════════════════════════════════════════════════════════════════════════

def get_scalper_current_ruleset() -> dict:
    """Get the scalper's currently active configuration."""
    return _get("nikita", "/api/scalper/config")


def get_ruleset_history_tool(engine: str, limit: int = 10) -> dict:
    """Get archived rulesets for an engine from ORACLE's own DB."""
    import oracle_db
    return oracle_db.get_ruleset_history(engine, limit)


def propose_ruleset_change(engine: str, change: dict, reasoning: str) -> dict:
    """Create a pending rule proposal in ORACLE's database."""
    import oracle_db
    import uuid
    proposal_id = f"prop_{uuid.uuid4().hex[:8]}"
    oracle_db.execute(
        """INSERT INTO rule_proposals (proposal_id, engine, proposed_rule, source, context_snapshot)
        VALUES (?, ?, ?, ?, ?)""",
        (proposal_id, engine, json.dumps(change), "oracle_proactive", json.dumps({"reasoning": reasoning})),
    )
    return {"proposal_id": proposal_id, "status": "pending_approval"}


def get_pending_approvals() -> dict:
    """Get all rule proposals awaiting human approval."""
    import oracle_db
    return oracle_db.get_pending_proposals()


# ═══════════════════════════════════════════════════════════════════════════════
# TIER 6 — Action Tools (ORACLE can execute these via chat)
# ═══════════════════════════════════════════════════════════════════════════════

def update_scalper_config(changes: dict) -> dict:
    """ADVISORY: Recommend scalper config changes. Logged as observation, not applied.
    The Hive agents make their own trading decisions based on their personality traits."""
    import oracle_db as _db
    _db.add_observation("oracle_advisory", {
        "target": "scalper", "recommended_changes": changes,
    }, severity="info", action_taken=f"Advisory: recommends scalper config: {changes}")
    return {"ok": True, "advisory": True, "intended_changes": changes,
            "note": "Logged as recommendation. The Hive decides."}


def update_executor_config(changes: dict) -> dict:
    """ADVISORY: Recommend hypothesis engine config changes. Logged as observation, not applied.
    The Hive agents make their own trading decisions based on their personality traits."""
    import oracle_db as _db
    _db.add_observation("oracle_advisory", {
        "target": "hypothesis", "recommended_changes": changes,
    }, severity="info", action_taken=f"Advisory: recommends hypothesis config: {changes}")
    return {"ok": True, "advisory": True, "intended_changes": changes,
            "note": "Logged as recommendation. The Hive decides."}


def emit_oracle_signal(signal_type: str, asset: str = None, data: dict = None, ttl_minutes: int = 30) -> dict:
    """Emit a cross-engine signal from ORACLE. Safety rails (TRADE_BLOCK, CAUTION) are still enforced."""
    return _post("nikita", "/api/engine-signals", {
        "engine": "oracle",
        "signal_type": signal_type,
        "asset": asset,
        "data": data or {},
        "ttl_minutes": ttl_minutes,
    })


def close_position(position_id: int) -> dict:
    """ADVISORY: Recommend closing a position. Logged as observation, not executed.
    Exception: TRADE_BLOCK violations may still be force-closed as a safety rail."""
    import oracle_db as _db
    _db.add_observation("oracle_advisory", {
        "target": "position", "position_id": position_id,
        "action": "close_recommended",
    }, severity="warning", action_taken=f"Advisory: recommends closing position {position_id}")
    return {"ok": True, "advisory": True, "position_id": position_id,
            "note": "Logged as recommendation. Operator or safety rail decides."}


def regenerate_ruleset(engine: str) -> dict:
    """Force regenerate a ruleset for an engine (scalper, hypothesis, sniper, swing)."""
    try:
        from ruleset.ruleset_manager import _generate_ruleset_for_engine
        result = _generate_ruleset_for_engine(engine, "user_requested")
        return {"ok": True, "engine": engine} if result else {"error": "Generation failed"}
    except Exception as e:
        return {"error": str(e)}


def _get_base(service: str) -> str:
    """Get base URL for a service."""
    if not _config:
        _load_config()
    return _config.get("services", {}).get(service, {}).get("base_url", "")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER A — System Intelligence
# ═══════════════════════════════════════════════════════════════════════════════

def get_analytics_dashboard() -> dict:
    """Get full analytics from Nikita: trade stats, auto-trade stats, indicator accuracy, token outcomes, whale impact, chain/hour breakdowns."""
    return _get("nikita", "/api/analytics/dashboard")


def get_trade_stats_detailed() -> dict:
    """Get detailed trade statistics: win rate, avg PnL, best/worst trades, total trades, breakdown by chain and source."""
    return _get("nikita", "/api/analytics/dashboard")


def get_portfolio_state() -> dict:
    """Get full portfolio state: positions with sectors, balance, total equity, fear & greed."""
    return _get("nikita", "/api/portfolio/state")


def get_portfolio_equity_history() -> dict:
    """Get portfolio equity curve over time with balance, position count, PnL, win rate snapshots."""
    return _get("nikita", "/api/pnl/history?period=7d")


def get_system_health() -> dict:
    """Get upstream service health: Nikita, Llama, Social Scraper connectivity status."""
    return _get("mechanicus", "/api/health")


def get_data_sources_health() -> dict:
    """Get health status of all 22+ data sources (API connectivity, rate limits, errors)."""
    return _get("nikita", "/api/sources/health")


def get_mirror_stats() -> dict:
    """Get trader mirror behavioral stats: signal alignment, tilt score, discipline label, streaks."""
    return _get("nikita", "/api/mirror/stats")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER B — Whale Intelligence
# ═══════════════════════════════════════════════════════════════════════════════

def get_whale_intel() -> dict:
    """Get all whale intelligence: copy trades, stealth accumulations, CEX flows, wash trade scores."""
    return _get("nikita", "/api/whale/intel")


def get_whale_history(limit: int = 20) -> dict:
    """Get recent whale events with timestamps and metadata."""
    return _get("nikita", f"/api/whale/history?limit={limit}")


def get_whale_copy_trades() -> dict:
    """Get detected copy trade pairs: leader/follower wallet relationships."""
    return _get("nikita", "/api/whale/copy-trades")


def get_whale_accumulations() -> dict:
    """Get active stealth accumulation patterns per asset."""
    return _get("nikita", "/api/whale/accumulations")


def get_whale_flows() -> dict:
    """Get CEX flow summary per token: exchange inflows vs outflows."""
    return _get("nikita", "/api/whale/flows")


def get_whale_leaderboard() -> dict:
    """Get whale wallets ranked by precog score (predictive accuracy)."""
    return _get("nikita", "/api/whale/leaderboard")


def get_whale_graph() -> dict:
    """Get wallet relationship network: nodes and edges showing transfer/copy/co-investor patterns."""
    return _get("nikita", "/api/whale/graph")


def get_whale_detail(whale_id: str) -> dict:
    """Get full details for a specific whale event including LLM analysis."""
    return _get("nikita", f"/api/whale/{whale_id}/detail")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER C — Token Discovery
# ═══════════════════════════════════════════════════════════════════════════════

def get_launch_tokens(chain: str = None) -> dict:
    """Get all discovered launch tokens with classification (discovered/scam/quick_profit)."""
    path = f"/api/launches/tokens?chain={chain}" if chain else "/api/launches/tokens"
    return _get("nikita", path)


def get_launch_token_detail(address: str) -> dict:
    """Get full details for a launch token: price, liquidity, holders, risk score."""
    return _get("nikita", f"/api/launches/token/{address}")


def get_launch_status() -> dict:
    """Get launch pipeline status: WebSocket connections, token counts, classification stats."""
    return _get("nikita", "/api/launches/status")


def get_contract_risk(chain: str, address: str) -> dict:
    """Get smart contract risk score: GoPlus + TokenSniffer + deployer reputation."""
    return _get("nikita", f"/api/risk/{chain}/{address}")


def get_sniper_history() -> dict:
    """Get recent sniper evaluations with scores, tiers, and signals."""
    return _get("nikita", "/api/sniper/history")


def get_sniper_stats() -> dict:
    """Get sniper performance: total evaluated, alpha/beta/watch counts, top scores."""
    return _get("nikita", "/api/sniper/stats")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER D — Scalper Intelligence
# ═══════════════════════════════════════════════════════════════════════════════

def get_scalper_active_trades() -> dict:
    """Get currently active scalp trades with entry price, hold time, P&L."""
    return _get("nikita", "/api/scalper/active")


def get_scalper_trade_history(limit: int = 20) -> dict:
    """Get historical scalp trades with outcomes, fingerprints, hold times."""
    return _get("nikita", f"/api/scalper/trades?limit={limit}")


def get_scalper_stats_full() -> dict:
    """Get full scalper stats: win rate, trades, avg PnL, scan cycles, rejection breakdown."""
    return _get("nikita", "/api/scalper/stats")


def get_scalper_hypotheses() -> dict:
    """Get scalper hypothesis testing data from Mechanicus."""
    return _get("mechanicus", "/api/scalper/hypotheses")


def trigger_scalp_optimization() -> dict:
    """Manually trigger scalp learner optimization cycle."""
    return _post("mechanicus", "/api/scalper/optimize", {})


def get_scalper_scan_stats() -> dict:
    """Get scalper scan funnel: tokens scanned, filter pass rates, rejection reasons."""
    stats = _get("nikita", "/api/scalper/stats")
    if stats and "scan" in stats:
        return stats["scan"]
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# TIER E — Executor Intelligence
# ═══════════════════════════════════════════════════════════════════════════════

def get_executor_active_trade() -> dict:
    """Get currently active hypothesis engine trade with alignment scores and poll count."""
    return _get("mechanicus", "/api/executor/active")


def get_executor_log(limit: int = 20) -> dict:
    """Get executor decision log: strikes, failures, reasons."""
    return _get("mechanicus", f"/api/executor/log?limit={limit}")


def get_executor_alignment() -> dict:
    """Get current alignment snapshot: all 7 bar scores for each scanned asset."""
    return _get("mechanicus", "/api/executor/alignment")


def get_executor_hypotheses() -> dict:
    """Get hypothesis testing data: fingerprints, win rates, sample sizes."""
    return _get("mechanicus", "/api/executor/hypotheses")


def get_executor_llama_context(asset: str) -> dict:
    """Get The Llama's trade context for a specific asset."""
    return _get("mechanicus", f"/api/executor/llama-context?asset={asset}")


def get_executor_scan_summary() -> dict:
    """Get latest scan summary: scanned, tradeable, passed, rejection breakdown."""
    dashboard = _get("mechanicus", "/api/dashboard")
    if dashboard:
        return dashboard.get("executor_scan_summary", {})
    return {}


# ═══════════════════════════════════════════════════════════════════════════════
# TIER F — Market Context
# ═══════════════════════════════════════════════════════════════════════════════

def get_market_data_bulk() -> dict:
    """Get bulk market data: fear_greed, hyperliquid, okx, santiment, coinalyze, correlation data."""
    return _get("nikita", "/api/market-data/bulk", timeout=15)


def get_enrichment_by_symbol(symbol: str) -> dict:
    """Get all enrichment data for a specific symbol."""
    return _get("nikita", f"/api/enrichment/{symbol}")


def get_indicators(symbol: str) -> dict:
    """Get classified indicators for a symbol: RSI, MACD, Bollinger, ADX, composite score."""
    return _get("nikita", f"/api/indicators/{symbol}")


def get_sentiment(symbol: str) -> dict:
    """Get sentiment consensus for a symbol across all sources."""
    return _get("nikita", f"/api/sentiment/{symbol}")


def get_intel_liquidity(symbol: str) -> dict:
    """Get liquidity analysis: liquidation clusters, price magnets, hunt probability."""
    return _get("nikita", f"/api/intel/liquidity/{symbol}")


def get_intel_divergence(symbol: str) -> dict:
    """Get divergence analysis: BTC/sector decorrelation, significance."""
    return _get("nikita", f"/api/intel/divergence/{symbol}")


def get_intel_social(symbol: str) -> dict:
    """Get social analysis: volume anomaly, sentiment quality, pre-pump signature."""
    return _get("nikita", f"/api/intel/social/{symbol}")


def get_intel_portfolio() -> dict:
    """Get portfolio-wide intelligence: concentration, correlation clusters, vulnerability."""
    return _get("nikita", "/api/intel/portfolio")


def get_intel_background() -> dict:
    """Get all background enrichment results with freshness status."""
    return _get("nikita", "/api/intel/background")


def get_coin_detail(symbol: str) -> dict:
    """Get comprehensive coin intelligence: market cap, volume, holders, risk, sentiment, whale activity."""
    return _get("nikita", f"/api/coin/{symbol}/detail")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER G — Execution
# ═══════════════════════════════════════════════════════════════════════════════

def place_order(asset: str, side: str, amount: float, sl_pct: float = 10, tp_pct: float = 20) -> dict:
    """Place a buy or sell order on Nikita. side: 'buy' or 'sell'."""
    return _post("nikita", "/api/order", {"asset": asset, "side": side, "amount": amount, "slPct": sl_pct, "tpPct": tp_pct, "_engine": "oracle"})


def modify_position(position_id: int, sl: float = None, tp: float = None) -> dict:
    """Modify SL/TP on an open position."""
    data = {"reason": "oracle_adjustment"}
    if sl is not None:
        data["sl"] = sl
    if tp is not None:
        data["tp"] = tp
    return _post("nikita", f"/api/position/{position_id}/modify", data)


def half_close_position(position_id: int) -> dict:
    """Close 50% of a position, keep the rest running."""
    return _post("nikita", "/api/position/half", {"id": position_id})


def clear_engine_signal(signal_id: int) -> dict:
    """Deactivate a specific cross-engine signal."""
    try:
        r = requests.delete(f"{_get_base('nikita')}/api/engine-signals/{signal_id}", timeout=5)
        return {"ok": True} if r.ok else {"error": r.text}
    except Exception as e:
        return {"error": str(e)}


def trigger_llama_review() -> dict:
    """Manually trigger a decision review cycle on The Llama."""
    return _post("llama", "/decisions/review", {})


def write_enrichment_cache(mode: str, result: dict, freshness_minutes: int = 60) -> dict:
    """Write a custom enrichment entry to The Llama's cache."""
    return _post("llama", "/cache/enrichment", {"mode": mode, "result": result, "freshness_minutes": freshness_minutes})


def trigger_pattern_learning() -> dict:
    """Manually trigger the Pattern Learner analysis in Mechanicus."""
    return _post("mechanicus", "/api/lessons/run", {})


# ═══════════════════════════════════════════════════════════════════════════════
# TIER H — Deep Analytics
# ═══════════════════════════════════════════════════════════════════════════════

def get_divergences(limit: int = 20) -> dict:
    """Get recent cross-system divergence events from Mechanicus."""
    return _get("mechanicus", f"/api/divergences?limit={limit}")


def get_active_divergences() -> dict:
    """Get currently active divergence alerts."""
    return _get("mechanicus", "/api/divergences/active")


def get_top_deployers() -> dict:
    """Get best deployers ranked by success rate."""
    return _get("mechanicus", "/api/deployers/top")


def get_worst_deployers() -> dict:
    """Get worst deployers ranked by failure/rug rate."""
    return _get("mechanicus", "/api/deployers/worst")


def get_deployer_stats() -> dict:
    """Get aggregate deployer statistics across the network."""
    return _get("mechanicus", "/api/deployers/stats")


def get_recommended_llama_mode() -> dict:
    """Get recommended LLM analysis mode based on current market regime."""
    return _get("mechanicus", "/api/regime/recommended-mode")


def get_regime_history(limit: int = 20) -> dict:
    """Get market regime change history."""
    return _get("mechanicus", f"/api/regime/history?limit={limit}")


def get_synthetic_trades(limit: int = 20) -> dict:
    """Get synthetic portfolio trades (virtual trades for testing)."""
    return _get("mechanicus", f"/api/synthetic/trades?limit={limit}")


def get_synthetic_stats() -> dict:
    """Get synthetic portfolio performance stats."""
    return _get("mechanicus", "/api/synthetic/stats")


def get_profitable_signals() -> dict:
    """Get most profitable signal combinations from synthetic testing."""
    return _get("mechanicus", "/api/synthetic/profitable-signals")


def get_market_intel(asset: str = None) -> dict:
    """Get latest market intelligence from Mechanicus. Optionally filter by asset."""
    if asset:
        return _get("mechanicus", f"/api/market/intel/{asset}")
    return _get("mechanicus", "/api/market/intel")


def get_market_cycle_status() -> dict:
    """Get current market analysis cycle status."""
    return _get("mechanicus", "/api/market/cycle-status")


def get_indicator_rankings() -> dict:
    """Get indicator accuracy rankings from Nikita intel ingest."""
    return _get("mechanicus", "/api/nikita/indicators")


def get_whale_impact_analysis() -> dict:
    """Get whale event impact analysis: event counts, avg impact, price correlation."""
    return _get("mechanicus", "/api/nikita/whale-impact")


def get_trade_stats_by_engine() -> dict:
    """Get trade statistics broken down by engine."""
    return _get("mechanicus", "/api/nikita/trade-stats")


def get_best_trading_hours() -> dict:
    """Get best performing hours for trading per engine."""
    return _get("mechanicus", "/api/nikita/best-hours")


def get_nikita_enrichment_analysis() -> dict:
    """Get enrichment analysis by data source type."""
    return _get("mechanicus", "/api/nikita/enrichment")


def get_brain_summary() -> dict:
    """Get full Mechanicus brain summary: all modules, insights, learned patterns."""
    return _get("mechanicus", "/api/nikita/brain")


def get_nikita_portfolio_history() -> dict:
    """Get portfolio history from Mechanicus perspective."""
    return _get("mechanicus", "/api/nikita/portfolio")


def get_portfolio_risk_log() -> dict:
    """Get portfolio risk event log from Portfolio Manager."""
    return _get("mechanicus", "/api/portfolio/risk-log")


def clear_portfolio_directive(directive_id: str) -> dict:
    """Clear a specific Portfolio Manager directive."""
    return _post("mechanicus", f"/api/portfolio/clear-directive/{directive_id}", {})


def get_llama_status() -> dict:
    """Get The Llama service status: queue depth, uptime, processing state."""
    return _get("llama", "/status")


def get_llama_decisions(limit: int = 20) -> dict:
    """Get The Llama's decision log."""
    return _get("llama", f"/decisions?limit={limit}")


def get_llama_decision_stats() -> dict:
    """Get The Llama's decision accuracy stats: correct, incorrect, inconclusive."""
    return _get("llama", "/decisions/stats")


def get_social_candidates() -> dict:
    """Get social media candidates: trending tokens from Twitter, Reddit, Discord."""
    return _get("nikita", "/api/social/candidates")


def get_social_signals() -> dict:
    """Get aggregated social signals by symbol."""
    return _get("nikita", "/api/social/signals")


def get_social_status() -> dict:
    """Get social data source connectivity status."""
    return _get("nikita", "/api/social/status")


def get_gas_anomalies() -> dict:
    """Get gas price anomalies and spikes."""
    return _get("nikita", "/api/gas/anomalies")


def check_mev(tx_hash: str) -> dict:
    """Check a transaction for MEV/sandwich attacks."""
    return _get("nikita", f"/api/mev/{tx_hash}")


def get_moralis_token(chain: str, address: str) -> dict:
    """Get full Moralis token intelligence: holders, transfers, market cap."""
    return _get("nikita", f"/api/moralis/{chain}/{address}")


def get_chart_data(symbol: str, timeframe: str = "1D") -> dict:
    """Get historical price chart data with trade/whale event overlays."""
    return _get("nikita", f"/api/chart/{symbol}?tf={timeframe}&events=1")


def get_indicator_settings() -> dict:
    """Get current indicator weight settings and available presets."""
    return _get("nikita", "/api/settings")


def update_indicator_settings(settings: dict) -> dict:
    """Update indicator weight settings or apply a preset."""
    return _post("nikita", "/api/settings", settings)


def get_trading_rules() -> dict:
    """Get all custom trading rules/alerts."""
    return _get("nikita", "/api/rules")


def create_trading_rule(rule: dict) -> dict:
    """Create a new custom trading rule."""
    return _post("nikita", "/api/rules", rule)


def get_backtest_result(strategy: str, symbol: str, period: str = "30d") -> dict:
    """Run a backtest for a strategy on a symbol."""
    return _post("nikita", "/api/backtest/run", {"strategy": strategy, "symbol": symbol, "period": period})


def get_metrics(metric: str = None) -> dict:
    """Get system metrics. If metric specified, returns that metric only."""
    if metric:
        return _get("nikita", f"/api/metrics/{metric}")
    return _get("nikita", "/api/metrics")


def get_profiles() -> dict:
    """Get all saved trading profiles with metadata."""
    return _get("nikita", "/api/profiles")


def get_mirror_report() -> dict:
    """Get detailed trader mirror coaching report."""
    return _get("nikita", "/api/mirror/report")


def get_fear_greed() -> dict:
    """Get current Fear & Greed index value and classification."""
    data = _get("nikita", "/api/market-data/bulk")
    if data and "fear_greed" in data:
        return data["fear_greed"]
    return {"error": "Fear & Greed data unavailable"}


def get_btc_price() -> dict:
    """Get current BTC price and 24h change quickly."""
    snapshot = _get("nikita", "/api/snapshot")
    if snapshot and "assets" in snapshot:
        btc = snapshot["assets"].get("BTC", {})
        return {"price": btc.get("price", 0), "change24h": btc.get("change24h", 0)}
    return {"error": "BTC price unavailable"}


def get_funding_rates() -> dict:
    """Get current funding rates from Hyperliquid and OKX."""
    data = _get("nikita", "/api/market-data/bulk")
    result = {}
    if data:
        result["hyperliquid"] = data.get("hyperliquid", {})
        result["okx"] = data.get("okx", {})
    return result


def get_defi_tvl() -> dict:
    """Get DeFi TVL data from DeFiLlama: total TVL, per-chain, per-protocol."""
    data = _get("nikita", "/api/market-data/bulk")
    if data:
        return data.get("defillama", {})
    return {}


def get_btc_dominance() -> dict:
    """Get BTC dominance, ETH dominance, total market cap."""
    data = _get("nikita", "/api/market-data/bulk")
    if data:
        return data.get("coinlore_global", {})
    return {}


def get_pullback_candidates() -> dict:
    """Get tokens in RECOVERY phase (first pullback after pump) — sniper opportunities."""
    return _get("nikita", "/api/launches/pullback-candidates")


def get_microstructure(symbol: str) -> dict:
    """Get buy/sell pressure analysis for a symbol across 1M/5M/15M windows."""
    return _get("nikita", f"/api/microstructure/{symbol}")


def get_moralis_status() -> dict:
    """Get Moralis API connectivity status and cache statistics."""
    return _get("nikita", "/api/moralis/status")


# ═══════════════════════════════════════════════════════════════════════════════
# TIER W — Write Tools (autonomous within safe ranges, approval outside)
# ═══════════════════════════════════════════════════════════════════════════════

@write_tool(safe_ranges={"value": {"min": 1.5, "max": 2.8}})
def set_hypothesis_alignment_total(value: float) -> dict:
    """Set min_alignment_total for hypothesis engine. Safe range: 1.5-2.8."""
    return _post("mechanicus", "/api/executor/config", {"min_alignment_total": value})

@write_tool(safe_ranges={"value": {"min": 4, "max": 6}})
def set_hypothesis_bars_green(value: int) -> dict:
    """Set min_bars_green for hypothesis engine. Safe range: 4-6."""
    return _post("mechanicus", "/api/executor/config", {"min_bars_green": value})

def reset_hypothesis_thresholds() -> dict:
    """Reset hypothesis engine thresholds to defaults (alignment=2.0, bars=5)."""
    return _post("mechanicus", "/api/executor/config", {"min_alignment_total": 2.0, "min_bars_green": 5})

@write_tool(safe_ranges={"value": {"min": 1.0, "max": 4.0}})
def set_scalper_min_change(value: float) -> dict:
    """Set min_5m_change for scalper. Safe range: 1.0-4.0%."""
    return _post("nikita", "/api/scalper/config", {"min_5m_change": value})

@write_tool(safe_ranges={"value": {"min": 1, "max": 5}})
def set_scalper_max_concurrent(value: int) -> dict:
    """Set max_concurrent_scalps. Safe range: 1-5."""
    return _post("nikita", "/api/scalper/config", {"max_concurrent_scalps": value})

def reset_scalper_thresholds() -> dict:
    """Reset scalper thresholds to defaults (min_change=2.0, concurrent=2)."""
    return _post("nikita", "/api/scalper/config", {"min_5m_change": 2.0, "max_concurrent_scalps": 2})

@write_tool(safe_ranges={"value": {"allowed": ["HIGH", "VERY_HIGH"]}})
def set_swing_min_confidence(value: str) -> dict:
    """Set minimum confidence for swing engine. Safe values: HIGH or VERY_HIGH only."""
    return _post("mechanicus", "/api/executor/config", {"swing_min_confidence": value})

@write_tool(safe_ranges={"value": {"min": 15, "max": 60}})
def set_swing_reeval_interval(value: int) -> dict:
    """Set swing re-evaluation interval in minutes. Safe range: 15-60."""
    return _post("mechanicus", "/api/executor/config", {"swing_reeval_interval": value})

@write_tool(safe_ranges={"value": {"min": 12, "max": 24}})
def set_swing_max_hold_hours(value: int) -> dict:
    """Set swing max hold duration in hours. Safe range: 12-24."""
    return _post("mechanicus", "/api/executor/config", {"swing_max_hold_hours": value})

@write_tool(safe_ranges={"value": {"min": 0.3, "max": 0.7}})
def set_swing_sl_tighten_factor(value: float) -> dict:
    """Set swing SL tighten factor on MACRO flip. Safe range: 0.3-0.7."""
    return _post("mechanicus", "/api/executor/config", {"swing_sl_tighten_factor": value})

def reset_swing_thresholds() -> dict:
    """Reset all swing engine thresholds to defaults."""
    return _post("mechanicus", "/api/executor/config", {
        "swing_min_confidence": "HIGH", "swing_reeval_interval": 30,
        "swing_max_hold_hours": 24, "swing_sl_tighten_factor": 0.5
    })

@write_tool(safe_ranges={"duration_minutes": {"min": 1, "max": 30}})
def pause_engine(engine_name: str, duration_minutes: int = 10) -> dict:
    """Pause an engine for N minutes. Safe range: 1-30 min autonomous, 31+ needs approval."""
    return _post("nikita", "/api/engine-signals", {
        "engine": "oracle", "signal_type": "ORACLE_ENGINE_PAUSE",
        "asset": engine_name, "data": {"duration": duration_minutes, "reason": "oracle_pause"},
        "ttl_minutes": duration_minutes,
    })

def resume_engine(engine_name: str) -> dict:
    """Resume a paused engine by clearing its pause signal. Always autonomous."""
    try:
        sigs = requests.get(f"{_get_base('nikita')}/api/engine-signals?type=ORACLE_ENGINE_PAUSE", timeout=5).json()
        for s in sigs:
            if s.get("asset") == engine_name:
                requests.delete(f"{_get_base('nikita')}/api/engine-signals/{s['id']}", timeout=5)
        return {"ok": True, "engine": engine_name, "action": "resumed"}
    except Exception as e:
        return {"error": str(e)}

def inject_enrichment_cache(mode: str = "ORACLE_BRIEFING", payload: dict = None) -> dict:
    """Write to The Llama's enrichment cache. Safe mode: ORACLE_BRIEFING only."""
    safe_modes = ["ORACLE_BRIEFING"]
    if mode not in safe_modes:
        import oracle_db
        pid = oracle_db.create_approval_request("inject_enrichment_cache", {"mode": mode}, "Unsafe cache mode", safe_modes, mode)
        return {"status": "pending_approval", "proposal_id": pid}
    return _post("llama", "/cache/enrichment", {"mode": mode, "result": payload or {}, "freshness_minutes": 35})

def write_intelligence_briefing(content: str) -> dict:
    """Write intelligence briefing to intelligence_summary.json. Always autonomous."""
    import time as _time
    try:
        with open("D:/mechanicias/intelligence_summary.json", "w") as f:
            json.dump({"summary_text": content, "oracle_confidence": 0.8, "generated_at": _time.strftime("%Y-%m-%dT%H:%M:%S"), "source": "oracle"}, f, indent=2)
        return {"ok": True, "length": len(content)}
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL REGISTRY — maps tool names to functions for the LLM
# ═══════════════════════════════════════════════════════════════════════════════

TOOL_REGISTRY = {
    # Tier 1 — Llama Interface
    "analyze_coin": analyze_coin,
    "get_enrichment_cache": get_enrichment_cache,
    "get_enrichment_freshness": get_enrichment_freshness,
    "get_llama_queue_status": get_llama_queue_status,
    # Tier 2 — Engine Control
    "get_engine_status": get_engine_status,
    "toggle_engine": toggle_engine,
    "get_cross_engine_signals": get_cross_engine_signals,
    "get_active_directives": get_active_directives,
    "get_regime_state": get_regime_state,
    "get_module_status": get_module_status,
    # Tier 3 — Portfolio & Signals
    "get_open_positions": get_open_positions,
    "get_portfolio_summary": get_portfolio_summary,
    "get_trade_history": get_trade_history,
    "get_win_rates": get_win_rates,
    "get_guardian_status": get_guardian_status,
    # Tier 4 — Debug & Inspect
    "get_enrichment_accuracy": get_enrichment_accuracy,
    "get_pattern_learner_insights": get_pattern_learner_insights,
    "get_scalp_fingerprints": get_scalp_fingerprints,
    "get_deployer_dna": get_deployer_dna,
    "get_hypothesis_history": get_hypothesis_history,
    "inspect_trade": inspect_trade,
    # Tier 5 — Ruleset Tools
    "get_scalper_current_ruleset": get_scalper_current_ruleset,
    "get_ruleset_history": get_ruleset_history_tool,
    "propose_ruleset_change": propose_ruleset_change,
    "get_pending_approvals": get_pending_approvals,
    # Tier 6 — Action Tools
    "update_scalper_config": update_scalper_config,
    "update_executor_config": update_executor_config,
    "emit_oracle_signal": emit_oracle_signal,
    "close_position": close_position,
    "regenerate_ruleset": regenerate_ruleset,
    # Tier A — System Intelligence
    "get_analytics_dashboard": get_analytics_dashboard,
    "get_trade_stats_detailed": get_trade_stats_detailed,
    "get_portfolio_state": get_portfolio_state,
    "get_portfolio_equity_history": get_portfolio_equity_history,
    "get_system_health": get_system_health,
    "get_data_sources_health": get_data_sources_health,
    "get_mirror_stats": get_mirror_stats,
    # Tier B — Whale Intelligence
    "get_whale_intel": get_whale_intel,
    "get_whale_history": get_whale_history,
    "get_whale_copy_trades": get_whale_copy_trades,
    "get_whale_accumulations": get_whale_accumulations,
    "get_whale_flows": get_whale_flows,
    "get_whale_leaderboard": get_whale_leaderboard,
    "get_whale_graph": get_whale_graph,
    "get_whale_detail": get_whale_detail,
    # Tier C — Token Discovery
    "get_launch_tokens": get_launch_tokens,
    "get_launch_token_detail": get_launch_token_detail,
    "get_launch_status": get_launch_status,
    "get_contract_risk": get_contract_risk,
    "get_sniper_history": get_sniper_history,
    "get_sniper_stats": get_sniper_stats,
    # Tier D — Scalper Intelligence
    "get_scalper_active_trades": get_scalper_active_trades,
    "get_scalper_trade_history": get_scalper_trade_history,
    "get_scalper_stats_full": get_scalper_stats_full,
    "get_scalper_hypotheses": get_scalper_hypotheses,
    "trigger_scalp_optimization": trigger_scalp_optimization,
    "get_scalper_scan_stats": get_scalper_scan_stats,
    # Tier E — Executor Intelligence
    "get_executor_active_trade": get_executor_active_trade,
    "get_executor_log": get_executor_log,
    "get_executor_alignment": get_executor_alignment,
    "get_executor_hypotheses": get_executor_hypotheses,
    "get_executor_llama_context": get_executor_llama_context,
    "get_executor_scan_summary": get_executor_scan_summary,
    # Tier F — Market Context
    "get_market_data_bulk": get_market_data_bulk,
    "get_enrichment_by_symbol": get_enrichment_by_symbol,
    "get_indicators": get_indicators,
    "get_sentiment": get_sentiment,
    "get_intel_liquidity": get_intel_liquidity,
    "get_intel_divergence": get_intel_divergence,
    "get_intel_social": get_intel_social,
    "get_intel_portfolio": get_intel_portfolio,
    "get_intel_background": get_intel_background,
    "get_coin_detail": get_coin_detail,
    # Tier G — Execution
    "place_order": place_order,
    "modify_position": modify_position,
    "half_close_position": half_close_position,
    "clear_engine_signal": clear_engine_signal,
    "trigger_llama_review": trigger_llama_review,
    "write_enrichment_cache": write_enrichment_cache,
    "trigger_pattern_learning": trigger_pattern_learning,
    # Tier H — Deep Analytics
    "get_divergences": get_divergences,
    "get_active_divergences": get_active_divergences,
    "get_top_deployers": get_top_deployers,
    "get_worst_deployers": get_worst_deployers,
    "get_deployer_stats": get_deployer_stats,
    "get_recommended_llama_mode": get_recommended_llama_mode,
    "get_regime_history": get_regime_history,
    "get_synthetic_trades": get_synthetic_trades,
    "get_synthetic_stats": get_synthetic_stats,
    "get_profitable_signals": get_profitable_signals,
    "get_market_intel": get_market_intel,
    "get_market_cycle_status": get_market_cycle_status,
    "get_indicator_rankings": get_indicator_rankings,
    "get_whale_impact_analysis": get_whale_impact_analysis,
    "get_trade_stats_by_engine": get_trade_stats_by_engine,
    "get_best_trading_hours": get_best_trading_hours,
    "get_nikita_enrichment_analysis": get_nikita_enrichment_analysis,
    "get_brain_summary": get_brain_summary,
    "get_nikita_portfolio_history": get_nikita_portfolio_history,
    "get_portfolio_risk_log": get_portfolio_risk_log,
    "clear_portfolio_directive": clear_portfolio_directive,
    "get_llama_status": get_llama_status,
    "get_llama_decisions": get_llama_decisions,
    "get_llama_decision_stats": get_llama_decision_stats,
    "get_social_candidates": get_social_candidates,
    "get_social_signals": get_social_signals,
    "get_social_status": get_social_status,
    "get_gas_anomalies": get_gas_anomalies,
    "check_mev": check_mev,
    "get_moralis_token": get_moralis_token,
    "get_chart_data": get_chart_data,
    "get_indicator_settings": get_indicator_settings,
    "update_indicator_settings": update_indicator_settings,
    "get_trading_rules": get_trading_rules,
    "create_trading_rule": create_trading_rule,
    "get_backtest_result": get_backtest_result,
    "get_metrics": get_metrics,
    "get_profiles": get_profiles,
    "get_mirror_report": get_mirror_report,
    "get_fear_greed": get_fear_greed,
    "get_btc_price": get_btc_price,
    "get_funding_rates": get_funding_rates,
    "get_defi_tvl": get_defi_tvl,
    "get_btc_dominance": get_btc_dominance,
    "get_pullback_candidates": get_pullback_candidates,
    "get_microstructure": get_microstructure,
    "get_moralis_status": get_moralis_status,
    # Tier W — Write Tools
    "set_hypothesis_alignment_total": set_hypothesis_alignment_total,
    "set_hypothesis_bars_green": set_hypothesis_bars_green,
    "reset_hypothesis_thresholds": reset_hypothesis_thresholds,
    "set_scalper_min_change": set_scalper_min_change,
    "set_scalper_max_concurrent": set_scalper_max_concurrent,
    "reset_scalper_thresholds": reset_scalper_thresholds,
    "set_swing_min_confidence": set_swing_min_confidence,
    "set_swing_reeval_interval": set_swing_reeval_interval,
    "set_swing_max_hold_hours": set_swing_max_hold_hours,
    "set_swing_sl_tighten_factor": set_swing_sl_tighten_factor,
    "reset_swing_thresholds": reset_swing_thresholds,
    "pause_engine": pause_engine,
    "resume_engine": resume_engine,
    "inject_enrichment_cache": inject_enrichment_cache,
    "write_intelligence_briefing": write_intelligence_briefing,
}


def get_tool_descriptions() -> str:
    """Generate tool descriptions for the system prompt."""
    lines = []
    for name, func in TOOL_REGISTRY.items():
        doc = func.__doc__ or "No description"
        # Extract first line of docstring
        first_line = doc.strip().split("\n")[0]
        lines.append(f"- {name}(): {first_line}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# TWO-STAGE TOOL CATEGORIES
# ═══════════════════════════════════════════════════════════════════════════════

TOOL_CATEGORIES = {
    "SYSTEM_INTELLIGENCE": {
        "description": "System health, analytics, portfolio state, trade stats, mirror stats, engine status",
        "tools": ["get_analytics_dashboard", "get_portfolio_summary", "get_open_positions",
                  "get_portfolio_state", "get_portfolio_equity_history", "get_system_health",
                  "get_data_sources_health", "get_mirror_stats", "get_mirror_report",
                  "get_engine_status", "get_module_status", "get_trade_history"],
    },
    "WHALE_INTELLIGENCE": {
        "description": "Whale events, copy trades, stealth accumulations, CEX flows, whale leaderboard, wallet graph",
        "tools": ["get_whale_intel", "get_whale_history", "get_whale_copy_trades",
                  "get_whale_accumulations", "get_whale_flows", "get_whale_leaderboard",
                  "get_whale_graph", "get_whale_detail"],
    },
    "TOKEN_DISCOVERY": {
        "description": "Launch tokens, sniper scores, contract risk, deployer DNA, rugcheck, Moralis token data",
        "tools": ["get_launch_tokens", "get_launch_token_detail", "get_launch_status",
                  "get_contract_risk", "get_sniper_history", "get_sniper_stats",
                  "get_deployer_dna", "get_moralis_token"],
    },
    "SCALPER_INTELLIGENCE": {
        "description": "Scalper active trades, trade history, scan funnel stats, hypotheses, optimization, config",
        "tools": ["get_scalper_active_trades", "get_scalper_trade_history", "get_scalper_stats_full",
                  "get_scalper_hypotheses", "get_scalper_scan_stats", "get_scalper_current_ruleset",
                  "trigger_scalp_optimization", "update_scalper_config"],
    },
    "EXECUTOR_INTELLIGENCE": {
        "description": "Hypothesis engine trades, alignment bars, scan summary, Llama context, hypotheses, win rates",
        "tools": ["get_executor_active_trade", "get_executor_log", "get_executor_alignment",
                  "get_executor_hypotheses", "get_executor_llama_context", "get_executor_scan_summary",
                  "get_hypothesis_history", "get_win_rates", "update_executor_config", "inspect_trade"],
    },
    "MARKET_CONTEXT": {
        "description": "Market data, enrichment per symbol, indicators, sentiment, liquidity, divergence, Fear & Greed, BTC price, funding rates",
        "tools": ["get_market_data_bulk", "get_enrichment_by_symbol", "get_indicators",
                  "get_sentiment", "get_intel_liquidity", "get_intel_divergence",
                  "get_intel_social", "get_intel_portfolio", "get_intel_background",
                  "get_coin_detail", "get_enrichment_cache", "get_enrichment_freshness",
                  "get_fear_greed", "get_btc_price", "get_funding_rates", "get_btc_dominance"],
    },
    "EXECUTION_ACTIONS": {
        "description": "Place orders, close/modify positions, toggle engines, emit signals, regenerate rulesets, update configs, write tools",
        "tools": ["place_order", "close_position", "modify_position", "half_close_position",
                  "toggle_engine", "emit_oracle_signal", "clear_engine_signal",
                  "regenerate_ruleset", "propose_ruleset_change", "write_enrichment_cache",
                  "trigger_llama_review", "trigger_pattern_learning",
                  "update_indicator_settings", "create_trading_rule",
                  "set_hypothesis_alignment_total", "set_hypothesis_bars_green",
                  "reset_hypothesis_thresholds", "set_scalper_min_change",
                  "set_scalper_max_concurrent", "reset_scalper_thresholds",
                  "set_swing_min_confidence", "set_swing_reeval_interval",
                  "set_swing_max_hold_hours", "set_swing_sl_tighten_factor",
                  "reset_swing_thresholds", "pause_engine", "resume_engine",
                  "inject_enrichment_cache", "write_intelligence_briefing"],
    },
    "DEEP_ANALYTICS": {
        "description": "Deployer rankings, regime history, synthetic trades, social signals, backtesting, divergences, gas anomalies, MEV, charts, metrics",
        "tools": [],
    },
    "HIVE": {
        "description": "The Hive — 22 AI personality agents, observer mode, hourly reports, consensus",
        "tools": [
            "read_hive_status", "read_hive_leaderboard", "read_hive_agents",
            "read_hive_consensus", "read_hive_recent_trades", "broadcast_hive_signal",
            "read_hive_report", "read_hive_observer_status",
            "read_hive_elo_leaderboard", "read_hive_regime_specialists",
            "read_hive_lineage_performance",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# HIVE TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

_HIVE_BASE = "http://192.168.158.203:5001"
_HIVE_TIMEOUT = 8


def read_hive_status() -> dict:
    """Read The Hive service status, agent count, and uptime."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/status", timeout=_HIVE_TIMEOUT)
        return r.json() if r.status_code == 200 else {"error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": str(e), "hive_offline": True}


def read_hive_leaderboard() -> dict:
    """Read The Hive agent leaderboard — all 20 agents ranked by P&L and win rate."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/personalities/leaderboard", timeout=_HIVE_TIMEOUT)
        agents = r.json() if r.status_code == 200 else []
        return {"agents": agents[:10], "total_agents": len(agents)}
    except Exception as e:
        return {"error": str(e)}


def read_hive_agents() -> dict:
    """Read all active Hive agents with balances and open positions."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/agents", timeout=_HIVE_TIMEOUT)
        return {"agents": r.json()} if r.status_code == 200 else {"error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def read_hive_consensus(asset: str = "BTC") -> dict:
    """Read performance-weighted agent consensus for an asset. Returns LONG/SHORT/NEUTRAL."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/consensus/{asset.upper()}", timeout=_HIVE_TIMEOUT)
        return r.json() if r.status_code == 200 else {"error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def read_hive_recent_trades() -> dict:
    """Read recent trade activity across all Hive agents."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/personalities/leaderboard", timeout=_HIVE_TIMEOUT)
        if r.status_code != 200:
            return {"error": f"HTTP {r.status_code}"}
        agents = r.json()
        return {"trade_summary": [{
            "name": a.get("agent_name", "?"), "rank": a.get("rank", 0),
            "total_pnl": a.get("total_pnl", 0), "win_rate": a.get("win_rate", 0),
            "open_trades": a.get("open_count", 0), "trade_count": a.get("trade_count", 0),
        } for a in agents]}
    except Exception as e:
        return {"error": str(e)}


def broadcast_hive_signal(asset: str = "BTC", direction: str = "BUY",
                          confidence: str = "high", trend: str = "bullish",
                          price: float = 0) -> dict:
    """WRITE TOOL — Broadcast signal to all Hive agents. Confidence must be high or very high."""
    if confidence.lower() not in ("high", "very high"):
        return {"error": "Confidence must be 'high' or 'very high'"}
    if direction.upper() not in ("BUY", "SELL"):
        return {"error": "Direction must be BUY or SELL"}
    payload = {"asset": asset.upper(), "direction": direction.upper(),
               "confidence": confidence.lower(), "trend": trend.lower()}
    if price and float(price) > 0:
        payload["price"] = float(price)
    try:
        r = requests.post(f"{_HIVE_BASE}/api/broadcast_signal", json=payload, timeout=_HIVE_TIMEOUT)
        return {"signal_sent": payload, "status": "ok" if r.status_code == 200 else f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def read_hive_report() -> dict:
    """Read the latest hourly batch report from The Hive observer.
    Contains leaderboard, trait insights, consensus, and config recommendations."""
    try:
        r = requests.get(f"{_get_base('nikita')}/api/hive/report", timeout=_HIVE_TIMEOUT)
        return r.json() if r.status_code == 200 else {"error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def read_hive_observer_status() -> dict:
    """Read The Hive observer status — is it polling Nikita, how many trades detected."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/status", timeout=_HIVE_TIMEOUT)
        data = r.json() if r.status_code == 200 else {}
        return data.get("observer", data)
    except Exception as e:
        return {"error": str(e)}


def read_hive_elo_leaderboard() -> dict:
    """Read Hive agent Elo leaderboard — agents ranked by Elo rating with tier badges,
    regime-specific ratings, 7-day momentum, and lineage info.
    Use this to understand which agents are genuinely performing vs lucky."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/elo/leaderboard", timeout=_HIVE_TIMEOUT)
        if r.status_code == 200:
            return {"agents": r.json()}
        # Fallback to Nikita cache
        r2 = requests.get(f"{_get_base('nikita')}/api/hive/elo", timeout=_HIVE_TIMEOUT)
        return r2.json() if r2.status_code == 200 else {"error": "No Elo data available"}
    except Exception as e:
        return {"error": str(e)}


def read_hive_regime_specialists(regime: str = "CHOP") -> dict:
    """Read top Hive agents for a specific market regime. Returns agents ranked by
    their regime-specific Elo rating — these are the agents to trust when ORACLE
    detects this regime is active."""
    try:
        r = requests.get(f"{_HIVE_BASE}/api/elo/regime/{regime.upper()}", timeout=_HIVE_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        # Fallback to Nikita cache
        r2 = requests.get(f"{_get_base('nikita')}/api/hive/elo/regime/{regime.upper()}", timeout=_HIVE_TIMEOUT)
        return r2.json() if r2.status_code == 200 else {"error": f"No data for regime {regime}"}
    except Exception as e:
        return {"error": str(e)}


def read_hive_lineage_performance(lineage: str = "") -> dict:
    """Read Hive lineage family tree performance. Shows which genetic lines are
    historically successful — avg Elo, peak rating, agent count per lineage.
    Informs breeding decisions during evolution."""
    try:
        url = f"{_HIVE_BASE}/api/elo/lineage/{lineage}" if lineage else f"{_get_base('nikita')}/api/hive/elo"
        r = requests.get(url, timeout=_HIVE_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            return data.get("elo_lineage_report", data) if isinstance(data, dict) else data
        return {"error": "No lineage data available"}
    except Exception as e:
        return {"error": str(e)}


# Compute DEEP_ANALYTICS as everything not in other categories
_categorized = set()
for _cat_name, _cat in TOOL_CATEGORIES.items():
    if _cat_name != "DEEP_ANALYTICS":
        _categorized.update(_cat.get("tools", []))
TOOL_CATEGORIES["DEEP_ANALYTICS"]["tools"] = [name for name in TOOL_REGISTRY if name not in _categorized]


def get_category_descriptions() -> str:
    """Generate category list for Stage 1 prompt."""
    lines = []
    for i, (name, cat) in enumerate(TOOL_CATEGORIES.items(), 1):
        lines.append(f"{i}. {name} ({len(cat['tools'])} tools) — {cat['description']}")
    return "\n".join(lines)


def get_tools_for_categories(categories: list) -> tuple:
    """Get filtered tool descriptions and registry for selected categories.

    Returns: (tool_descriptions_text, filtered_registry_dict)
    """
    selected_tools = set()
    for cat_name in categories:
        cat = TOOL_CATEGORIES.get(cat_name, {})
        for tool_name in cat.get("tools", []):
            selected_tools.add(tool_name)

    # Build filtered descriptions and registry
    lines = []
    filtered = {}
    for name in selected_tools:
        if name in TOOL_REGISTRY:
            func = TOOL_REGISTRY[name]
            doc = func.__doc__ or "No description"
            first_line = doc.strip().split("\n")[0]
            lines.append(f"- {name}(): {first_line}")
            filtered[name] = func

    return "\n".join(sorted(lines)), filtered
