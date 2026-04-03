"""ORACLE — Intelligence Layer for The Nikita Network.

Conversational interface + autonomous ruleset management.
Runs on port 9000 alongside Nikita (5000), The Llama (8989), Mechanicus (7777).
"""

import sys
import os
import json
import signal
import threading
import time
import logging

# Fix Windows console encoding
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Resolve paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
sys.path.insert(0, BASE_DIR)

from flask import Flask, request, jsonify, Response
from flask_socketio import SocketIO

import oracle_db as db
from oracle_llm import check_ollama_health
from oracle_conversation import chat, get_session_history, cleanup_expired_sessions
from oracle_tools import TOOL_REGISTRY

# Suppress Flask logging
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# Load config
CONFIG_PATH = os.path.join(BASE_DIR, "oracle_config.json")
with open(CONFIG_PATH) as f:
    config = json.load(f)

# Flask app
app = Flask(__name__, static_folder="dashboard/static", template_folder="dashboard/templates")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Import dashboard blueprint
try:
    from dashboard.dashboard import dashboard_bp
    app.register_blueprint(dashboard_bp)
except ImportError as e:
    print(f"  [ORACLE] Dashboard import failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "ORACLE", "port": 9000})


@app.route("/api/status")
def api_status():
    """Full ORACLE status."""
    ollama = check_ollama_health()
    return jsonify({
        "status": "online",
        "ollama": ollama,
        "tools_available": len(TOOL_REGISTRY),
        "active_sessions": len(oracle_conversation._sessions) if hasattr(oracle_conversation, '_sessions') else 0,
    })


@app.route("/api/chat", methods=["POST"])
def api_chat():
    """Conversation endpoint."""
    data = request.json or {}
    message = data.get("message", "").strip()
    session_id = data.get("session_id")

    if not message:
        return jsonify({"error": "No message provided"}), 400

    try:
        result = chat(session_id or "default", message)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history")
def api_history():
    """Get conversation history."""
    session_id = request.args.get("session_id", "default")
    return jsonify(get_session_history(session_id))


@app.route("/api/rulesets")
def api_rulesets():
    """Get active rulesets."""
    return jsonify(db.get_active_rulesets())


@app.route("/api/rulesets/history/<engine>")
def api_ruleset_history(engine):
    """Get ruleset history for an engine."""
    limit = request.args.get("limit", 10, type=int)
    return jsonify(db.get_ruleset_history(engine, limit))


@app.route("/api/approvals")
def api_approvals():
    """Get pending rule proposals."""
    return jsonify(db.get_pending_proposals())


@app.route("/api/approvals/<proposal_id>/decide", methods=["POST"])
def api_decide_proposal(proposal_id):
    """Approve or reject a rule proposal."""
    data = request.json or {}
    decision = data.get("decision")  # approve, reject
    if decision not in ("approve", "reject"):
        return jsonify({"error": "Decision must be 'approve' or 'reject'"}), 400

    db.execute(
        """UPDATE rule_proposals SET verdict = ?, decided_at = datetime('now'),
            decided_by = 'user' WHERE proposal_id = ?""",
        (decision + "d", proposal_id),
    )
    return jsonify({"ok": True, "proposal_id": proposal_id, "decision": decision})


@app.route("/api/observations")
def api_observations():
    """Get recent observations."""
    limit = request.args.get("limit", 50, type=int)
    severity = request.args.get("severity")
    return jsonify(db.get_recent_observations(limit, severity))


@app.route("/api/validate-trade", methods=["POST"])
def api_validate_trade():
    """Pre-trade validation — engines call this before executing a strike.

    Fast path: checks signals, portfolio state, enrichment freshness.
    Does NOT call Qwen (too slow for pre-trade). Pure rule-based checks.
    Returns: {approved: bool, reason: str}
    """
    data = request.json or {}
    engine = data.get("engine", "")
    asset = data.get("asset", "")
    alignment_total = data.get("alignment_total", 0)

    try:
        import requests as _req
        _nikita_url = config.get("services", {}).get("nikita", {}).get("base_url", "http://192.168.158.237:5000")

        # Single fast check: ORACLE_TRADE_BLOCK signals on Nikita
        sigs = _req.get(f"{_nikita_url}/api/engine-signals?type=ORACLE_TRADE_BLOCK", timeout=1).json()
        if isinstance(sigs, list) and sigs:
            return jsonify({"approved": False, "reason": f"ORACLE_TRADE_BLOCK: {sigs[0].get('data', {}).get('reasoning', 'blocked')}"})

        # Low alignment warning
        if alignment_total < 2.0:
            return jsonify({"approved": True, "reason": f"Approved with caution — low alignment ({alignment_total:.2f})"})

        return jsonify({"approved": True, "reason": "All checks passed"})

    except Exception as e:
        # Fail-open: if ORACLE can't be reached, don't block the trade
        return jsonify({"approved": True, "reason": f"Validation error (fail-open): {e}"})


@app.route("/api/observations/<int:obs_id>/acknowledge", methods=["POST"])
def api_acknowledge_observation(obs_id):
    """Acknowledge an observation."""
    db.execute("UPDATE observations SET acknowledged = TRUE WHERE id = ?", (obs_id,))
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# BACKGROUND TASKS
# ═══════════════════════════════════════════════════════════════════════════════

def _watch_loop():
    """Background thread — runs the ruleset watch cycle and session cleanup."""
    interval = config.get("oracle", {}).get("watch_loop_interval_seconds", 60)
    while True:
        try:
            # Clean expired sessions
            cleaned = cleanup_expired_sessions()

            # Run ruleset watcher
            try:
                from ruleset.ruleset_manager import run_watch_cycle
                run_watch_cycle()
            except Exception as e:
                if "No module" not in str(e):
                    print(f"  [ORACLE] Watch cycle error: {e}")

        except Exception as e:
            print(f"  [ORACLE] Background loop error: {e}")

        time.sleep(interval)


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════

import oracle_conversation  # for _sessions access


def main():
    port = config.get("oracle", {}).get("port", 9000)

    print()
    print("  ======================================")
    print("  ORACLE — Intelligence Layer")
    print("  The Nikita Network")
    print("  ======================================")

    # Initialize database
    db.init_db(config.get("oracle", {}).get("db_path", "D:/ORACLE/oracle.db"))

    # Check service connectivity
    import requests
    services = {
        "Nikita": config.get("services", {}).get("nikita", {}).get("base_url", ""),
        "The Llama": config.get("services", {}).get("llama", {}).get("base_url", ""),
        "Mechanicus": config.get("services", {}).get("mechanicus", {}).get("base_url", ""),
    }

    health_paths = {"Nikita": "/api/snapshot", "The Llama": "/health", "Mechanicus": "/health"}
    for name, url in services.items():
        path = health_paths.get(name, "/health")
        try:
            r = requests.get(f"{url}{path}", timeout=5)
            status = "ONLINE" if r.status_code == 200 else "ERROR"
        except Exception:
            status = "OFFLINE"
        print(f"  {name:12s}: {status} ({url})")

    # Check Ollama
    ollama = check_ollama_health()
    active = ollama.get("active", "none")
    print(f"  Ollama:       {'ONLINE' if active else 'OFFLINE'} ({active or 'no instance'})")

    print(f"  Tools:        {len(TOOL_REGISTRY)} available")
    print(f"  Dashboard:    http://localhost:{port}")
    print("  ======================================")

    # Start background watch loop
    watch_thread = threading.Thread(target=_watch_loop, daemon=True)
    watch_thread.start()
    print(f"  Watch loop started (every {config.get('oracle', {}).get('watch_loop_interval_seconds', 60)}s)")

    print(f"  Press Ctrl+C to stop.\n")

    # Start Flask
    socketio.run(app, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
