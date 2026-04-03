"""ORACLE Database — SQLite persistence for conversations, rulesets, proposals, observations."""

import sqlite3
import json
import threading
from pathlib import Path

_db_path = None
_lock = threading.Lock()


def init_db(db_path: str = "D:/ORACLE/oracle.db"):
    """Initialize database with all tables."""
    global _db_path
    _db_path = db_path

    conn = sqlite3.connect(_db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            tools_called TEXT,
            system_context TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_conv_session ON conversations(session_id);

        CREATE TABLE IF NOT EXISTS rulesets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ruleset_id TEXT UNIQUE NOT NULL,
            engine TEXT NOT NULL,
            version TEXT NOT NULL,
            status TEXT NOT NULL,
            market_context TEXT NOT NULL,
            rules TEXT NOT NULL,
            reasoning TEXT,
            generated_at DATETIME DEFAULT (datetime('now', 'localtime')),
            activated_at DATETIME,
            archived_at DATETIME,
            ttl_hours INTEGER DEFAULT 24,
            expiry_action TEXT DEFAULT 'revalidate',
            created_by TEXT DEFAULT 'oracle'
        );
        CREATE INDEX IF NOT EXISTS idx_rs_engine ON rulesets(engine);
        CREATE INDEX IF NOT EXISTS idx_rs_status ON rulesets(status);

        CREATE TABLE IF NOT EXISTS ruleset_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ruleset_id TEXT NOT NULL,
            engine TEXT NOT NULL,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            trades_taken INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0.0,
            avg_gain REAL DEFAULT 0.0,
            avg_loss REAL DEFAULT 0.0,
            total_pnl REAL DEFAULT 0.0,
            macro_regime TEXT,
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rp_ruleset ON ruleset_performance(ruleset_id);

        CREATE TABLE IF NOT EXISTS rule_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id TEXT UNIQUE NOT NULL,
            engine TEXT NOT NULL,
            proposed_rule TEXT NOT NULL,
            source TEXT NOT NULL,
            context_snapshot TEXT NOT NULL,
            verdict TEXT,
            verdict_reasoning TEXT,
            modified_rule TEXT,
            proposed_at DATETIME DEFAULT (datetime('now', 'localtime')),
            decided_at DATETIME,
            decided_by TEXT,
            applied BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_prop_engine ON rule_proposals(engine);
        CREATE INDEX IF NOT EXISTS idx_prop_verdict ON rule_proposals(verdict);

        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            observation_type TEXT NOT NULL,
            engine TEXT,
            severity TEXT DEFAULT 'info',
            data TEXT NOT NULL,
            action_taken TEXT,
            acknowledged BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_obs_type ON observations(observation_type);
        CREATE INDEX IF NOT EXISTS idx_obs_severity ON observations(severity);

        CREATE TABLE IF NOT EXISTS llm_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            prompt_length INTEGER,
            response_length INTEGER,
            tools_called TEXT,
            latency_ms INTEGER,
            endpoint TEXT,
            success BOOLEAN DEFAULT TRUE,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS tool_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tool_name TEXT NOT NULL,
            tool_tier TEXT NOT NULL,
            session_id TEXT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            latency_ms INTEGER,
            success BOOLEAN DEFAULT TRUE,
            response_size_bytes INTEGER,
            llm_instance TEXT,
            error_message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tc_tool ON tool_calls(tool_name);
        CREATE INDEX IF NOT EXISTS idx_tc_ts ON tool_calls(timestamp);

        CREATE TABLE IF NOT EXISTS llm_routing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            instance_selected TEXT,
            selection_reason TEXT,
            queue_depth INTEGER,
            latency_ms INTEGER,
            first_token_latency_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS write_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            tool_name TEXT NOT NULL,
            parameters_json TEXT,
            execution_mode TEXT,
            approved_by TEXT,
            result TEXT,
            previous_value_json TEXT,
            new_value_json TEXT,
            rolled_back BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_wa_tool ON write_actions(tool_name);

        CREATE TABLE IF NOT EXISTS oracle_trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            window TEXT DEFAULT '1h'
        );
        CREATE INDEX IF NOT EXISTS idx_trends_metric ON oracle_trends(metric, timestamp);

        CREATE TABLE IF NOT EXISTS oracle_interventions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            intervention_type TEXT NOT NULL,
            target_engine TEXT,
            action_taken TEXT NOT NULL,
            metrics_before TEXT,
            metrics_after TEXT,
            evaluated_at DATETIME,
            outcome TEXT,
            score REAL
        );
        CREATE INDEX IF NOT EXISTS idx_interv_type ON oracle_interventions(intervention_type);

        CREATE TABLE IF NOT EXISTS oracle_health_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
            health_score REAL NOT NULL,
            details TEXT
        );
    """)
    conn.commit()
    conn.close()
    print(f"  [ORACLE DB] Initialized at {db_path}")


def get_conn():
    """Get a new SQLite connection."""
    conn = sqlite3.connect(_db_path)
    conn.row_factory = sqlite3.Row
    return conn


def execute(query: str, params: tuple = ()):
    """Execute a write query."""
    with _lock:
        conn = get_conn()
        try:
            conn.execute(query, params)
            conn.commit()
        finally:
            conn.close()


def query(query_str: str, params: tuple = (), one: bool = False):
    """Execute a read query."""
    conn = get_conn()
    try:
        rows = conn.execute(query_str, params).fetchall()
        if one:
            return dict(rows[0]) if rows else None
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─── Convenience Functions ────────────────────────────────────────────────────

def log_conversation(session_id: str, role: str, content: str,
                     tools_called: list = None, system_context: dict = None):
    """Log a conversation turn."""
    execute(
        """INSERT INTO conversations (session_id, role, content, tools_called, system_context)
        VALUES (?, ?, ?, ?, ?)""",
        (session_id, role, content,
         json.dumps(tools_called) if tools_called else None,
         json.dumps(system_context) if system_context else None),
    )


def log_llm_call(prompt_length: int, response_length: int, tools_called: list,
                 latency_ms: int, endpoint: str, success: bool = True, error: str = None):
    """Log an LLM API call for monitoring."""
    execute(
        """INSERT INTO llm_calls (prompt_length, response_length, tools_called,
            latency_ms, endpoint, success, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (prompt_length, response_length, json.dumps(tools_called) if tools_called else None,
         latency_ms, endpoint, success, error),
    )


def add_observation(obs_type: str, data: dict, engine: str = None,
                    severity: str = "info", action_taken: str = None):
    """Record an observation from the watch loop."""
    execute(
        """INSERT INTO observations (observation_type, engine, severity, data, action_taken)
        VALUES (?, ?, ?, ?, ?)""",
        (obs_type, engine, severity, json.dumps(data), action_taken),
    )


def get_recent_observations(limit: int = 50, severity: str = None):
    """Get recent observations, optionally filtered by severity."""
    if severity:
        return query(
            "SELECT * FROM observations WHERE severity = ? ORDER BY id DESC LIMIT ?",
            (severity, limit),
        )
    return query("SELECT * FROM observations ORDER BY id DESC LIMIT ?", (limit,))


def get_conversation_history(session_id: str, limit: int = 50):
    """Get conversation history for a session."""
    return query(
        "SELECT * FROM conversations WHERE session_id = ? ORDER BY id ASC LIMIT ?",
        (session_id, limit),
    )


def get_pending_proposals():
    """Get all proposals awaiting decision."""
    return query(
        "SELECT * FROM rule_proposals WHERE verdict IS NULL OR verdict = 'flagged' ORDER BY proposed_at DESC"
    )


def get_active_rulesets():
    """Get all currently active rulesets."""
    return query("SELECT * FROM rulesets WHERE status = 'active' ORDER BY engine")


def get_ruleset_history(engine: str, limit: int = 10):
    """Get archived rulesets for an engine."""
    return query(
        "SELECT * FROM rulesets WHERE engine = ? ORDER BY generated_at DESC LIMIT ?",
        (engine, limit),
    )


# ─── Tool Telemetry ──────────────────────────────────────────────────────────

def log_tool_call(tool_name, tier, session_id, latency_ms, success, response_size, llm_instance=None, error=None):
    """Log a tool invocation for telemetry."""
    execute(
        """INSERT INTO tool_calls (tool_name, tool_tier, session_id, latency_ms, success, response_size_bytes, llm_instance, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (tool_name, tier, session_id, latency_ms, success, response_size, llm_instance, error),
    )


def get_tool_stats(window_hours=1):
    """Get per-tool call stats within a time window."""
    return query(
        """SELECT tool_name, tool_tier,
            COUNT(*) as call_count,
            ROUND(AVG(CASE WHEN success THEN 1.0 ELSE 0.0 END), 3) as success_rate,
            ROUND(AVG(latency_ms), 0) as avg_latency_ms,
            MAX(timestamp) as last_call,
            MAX(CASE WHEN success THEN timestamp END) as last_success
        FROM tool_calls
        WHERE timestamp > datetime('now', 'localtime', ?)
        GROUP BY tool_name
        ORDER BY call_count DESC""",
        (f"-{window_hours} hours",),
    )


def log_llm_routing(session_id, instance, reason, queue_depth, latency_ms, first_token_ms=None):
    """Log an LLM routing decision."""
    execute(
        """INSERT INTO llm_routing (session_id, instance_selected, selection_reason, queue_depth, latency_ms, first_token_latency_ms)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (session_id, instance, reason, queue_depth, latency_ms, first_token_ms),
    )


def get_llm_stats():
    """Get per-instance LLM call stats."""
    instances = query(
        """SELECT instance_selected,
            COUNT(*) as calls_1h,
            ROUND(AVG(latency_ms), 0) as avg_latency,
            SUM(CASE WHEN selection_reason = 'failover' THEN 1 ELSE 0 END) as failovers
        FROM llm_routing
        WHERE timestamp > datetime('now', 'localtime', '-1 hour')
        GROUP BY instance_selected"""
    )
    latencies = query(
        """SELECT instance_selected, latency_ms FROM llm_routing
        WHERE timestamp > datetime('now', 'localtime', '-1 hour')
        ORDER BY timestamp DESC LIMIT 40"""
    )
    return {"instances": instances, "recent_latencies": latencies}


def get_recent_sessions(limit=50):
    """Get recent conversation sessions."""
    return query(
        """SELECT session_id, MIN(timestamp) as start_time,
            COUNT(*) as message_count,
            GROUP_CONCAT(tools_called) as all_tools
        FROM conversations
        GROUP BY session_id
        ORDER BY MAX(timestamp) DESC LIMIT ?""",
        (limit,),
    )


# ─── Write Actions ────────────────────────────────────────────────────────────

def log_write_action(tool_name, params, mode, result, prev_value, new_value):
    """Log a write tool execution."""
    execute(
        """INSERT INTO write_actions (tool_name, parameters_json, execution_mode, result, previous_value_json, new_value_json)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (tool_name, json.dumps(params, default=str), mode, result,
         json.dumps(prev_value, default=str), json.dumps(new_value, default=str)),
    )


def get_write_history(limit=100):
    """Get recent write actions."""
    return query("SELECT * FROM write_actions ORDER BY id DESC LIMIT ?", (limit,))


def create_approval_request(tool_name, params, reason, current_value, proposed_value):
    """Create an approval request for an out-of-range write action."""
    import uuid
    proposal_id = f"write_{uuid.uuid4().hex[:8]}"
    execute(
        """INSERT INTO rule_proposals (proposal_id, engine, proposed_rule, source, context_snapshot, verdict_reasoning)
        VALUES (?, ?, ?, 'oracle_write_tool', ?, ?)""",
        (proposal_id, tool_name, json.dumps({"params": params, "proposed_value": proposed_value}, default=str),
         json.dumps({"current_value": current_value, "reason": reason}, default=str),
         f"Write tool {tool_name} parameter outside safe range. Current: {current_value}, Proposed: {proposed_value}"),
    )
    return proposal_id
