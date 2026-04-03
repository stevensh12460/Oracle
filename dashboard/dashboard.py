"""
ORACLE Dashboard — Flask Blueprint for the web-based control interface.

Provides chat, ruleset management, approval workflow, observations timeline,
and historical archive.
"""

import time
import json
from flask import Blueprint, render_template, request, jsonify

import oracle_db as db
from ruleset.ruleset_manager import get_active_rulesets, assemble_generation_context
from ruleset.ruleset_store import activate_ruleset, archive_ruleset, get_performance
from modes.reactive_mode import handle_engine_proposal
from modes.generative_mode import generate_ruleset

# Try to import LLM for chat
try:
    from oracle_llm import call_qwen
except ImportError:
    call_qwen = None

dashboard_bp = Blueprint(
    "dashboard",
    __name__,
    template_folder="templates",
    static_folder="static",
)


@dashboard_bp.route("/")
def index():
    return render_template("dashboard.html")


@dashboard_bp.route("/api/chat", methods=["POST"])
def chat_route():
    """Handle chat messages — delegates to oracle_conversation."""
    data = request.get_json() or {}
    message = data.get("message", "").strip()
    session_id = data.get("session_id", "default")

    if not message:
        return jsonify({"error": "No message provided"}), 400

    try:
        from oracle_conversation import chat
        result = chat(session_id, message)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "answer": f"Error: {e}", "tools_called": []}), 500


@dashboard_bp.route("/api/rulesets")
def rulesets():
    """Return all active rulesets with performance data."""
    active = get_active_rulesets()
    now = time.time()

    result = []
    for r in active:
        # Parse rules JSON
        rules_raw = r.get("rules", "")
        rules_parsed = {}
        if rules_raw:
            try:
                rules_parsed = json.loads(rules_raw) if isinstance(rules_raw, str) else rules_raw
            except Exception:
                pass

        # Get performance from ruleset_performance table (fail-safe)
        trades, wins, total_pnl = 0, 0, 0.0
        try:
            perf_row = db.query(
                "SELECT trades_taken, wins, losses, win_rate, total_pnl FROM ruleset_performance WHERE ruleset_id = ? ORDER BY id DESC LIMIT 1",
                (r.get("ruleset_id", ""),), one=True,
            )
            if perf_row:
                trades = perf_row.get("trades_taken", 0) or 0
                wins = perf_row.get("wins", 0) or 0
                total_pnl = perf_row.get("total_pnl", 0) or 0
        except Exception:
            pass

        # Compute TTL remaining
        ttl_hours = r.get("ttl_hours", 24) or 24
        activated = r.get("activated_at") or r.get("generated_at") or ""
        ttl_remaining_s = 0
        try:
            import datetime
            act_time = datetime.datetime.strptime(activated, "%Y-%m-%d %H:%M:%S")
            age_s = (datetime.datetime.now() - act_time).total_seconds()
            ttl_remaining_s = max(0, ttl_hours * 3600 - age_s)
        except Exception:
            ttl_remaining_s = ttl_hours * 3600

        wr_decimal = round(wins / max(trades, 1), 4)  # JS expects 0.0-1.0

        result.append({
            "ruleset_id": r.get("ruleset_id"),
            "engine": r.get("engine"),
            "status": r.get("status"),
            "created_at": r.get("generated_at"),
            "activated_at": activated,
            "reasoning": r.get("reasoning", ""),
            "rules": rules_parsed,
            "recommendations": rules_parsed.get("recommendations", []) if isinstance(rules_parsed, dict) else [],
            "config_snapshot": rules_parsed.get("config_snapshot", {}) if isinstance(rules_parsed, dict) else {},
            "market_context": r.get("market_context", ""),
            "hive_insights": rules_parsed.get("hive_insights", {}) if isinstance(rules_parsed, dict) else {},
            "llm_generated": rules_parsed.get("llm_generated", False) if isinstance(rules_parsed, dict) else False,
            "trades": trades,
            "wins": wins,
            "losses": trades - wins,
            "pnl": round(total_pnl, 2),
            "win_rate": wr_decimal,
            "ttl_hours": ttl_hours,
            "ttl_remaining_s": ttl_remaining_s,
        })

    return jsonify(result)


@dashboard_bp.route("/api/approvals")
def approvals():
    """Return pending approval items."""
    pending = db.get_pending_proposals()
    return jsonify(pending)


@dashboard_bp.route("/api/approvals/<approval_id>/decide", methods=["POST"])
def decide_approval(approval_id):
    """Approve or reject a pending proposal."""
    data = request.get_json() or {}
    decision = data.get("decision", "").lower()

    if decision not in ("approve", "reject"):
        return jsonify({"error": "Decision must be 'approve' or 'reject'"}), 400

    if decision == "approve":
        # Activate the ruleset
        success = activate_ruleset(approval_id)
        if success:
            # db.resolve_approval(approval_id, "approved")
            return jsonify({"status": "approved", "ruleset_id": approval_id})
        else:
            return jsonify({"error": "Failed to activate ruleset"}), 500
    else:
        # db.resolve_approval(approval_id, "rejected")
        return jsonify({"status": "rejected", "ruleset_id": approval_id})


@dashboard_bp.route("/api/observations")
def observations():
    """Return recent observations."""
    limit = request.args.get("limit", 50, type=int)
    obs = db.get_recent_observations(limit=limit)
    return jsonify(obs)


@dashboard_bp.route("/api/status")
def status():
    """Return ORACLE health status."""
    active = get_active_rulesets()
    pending = db.get_pending_proposals()
    observations_count = len(db.get_recent_observations(limit=100))

    llm_status = "connected" if call_qwen is not None else "disconnected"

    return jsonify({
        "oracle_status": "online",
        "llm_status": llm_status,
        "active_rulesets": len(active),
        "pending_approvals": len(pending),
        "recent_observations": observations_count,
        "uptime_s": time.time() - _start_time,
    })


@dashboard_bp.route("/api/archive")
def archive():
    """Return archived rulesets with performance history."""
    archived = db.get_ruleset_history("all", limit=50)
    result = []
    for r in archived:
        perf = r.get("performance", {})
        trades = perf.get("trades", 0)
        wins = perf.get("wins", 0)
        result.append({
            "ruleset_id": r.get("ruleset_id"),
            "engine": r.get("engine"),
            "status": r.get("status"),
            "created_at": r.get("created_at"),
            "activated_at": r.get("activated_at"),
            "expires_at": r.get("expires_at"),
            "trades": trades,
            "wins": wins,
            "losses": perf.get("losses", 0),
            "pnl": perf.get("pnl", 0.0),
            "win_rate": wins / max(trades, 1),
            "reasoning": r.get("reasoning", ""),
        })
    return jsonify(result)


@dashboard_bp.route("/api/generate", methods=["POST"])
def generate():
    """Generate a new ruleset for an engine."""
    data = request.get_json() or {}
    engine = data.get("engine", "").strip()
    user_request = data.get("request", "").strip() or None

    if not engine:
        return jsonify({"error": "Engine name required"}), 400

    result = generate_ruleset(engine, user_request)
    if "error" in result:
        return jsonify(result), 500

    return jsonify(result)


# ─── CMD CENTER Endpoints ─────────────────────────────────────────────────────

@dashboard_bp.route("/api/tool-stats")
def tool_stats():
    """Tool telemetry stats for Command Center."""
    window = request.args.get("window", 1, type=int)
    return jsonify(db.get_tool_stats(window))


@dashboard_bp.route("/api/llm-stats")
def llm_stats():
    """LLM routing stats for dual monitor."""
    return jsonify(db.get_llm_stats())


@dashboard_bp.route("/api/sessions")
def sessions():
    """Recent reasoning sessions."""
    limit = request.args.get("limit", 50, type=int)
    return jsonify(db.get_recent_sessions(limit))


@dashboard_bp.route("/api/write-history")
def write_history():
    """Recent write tool actions for audit log."""
    limit = request.args.get("limit", 100, type=int)
    return jsonify(db.get_write_history(limit))


_start_time = time.time()
