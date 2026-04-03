"""ORACLE Conversation Manager — session handling, history, context window management."""

import uuid
import time
import json

import oracle_db as db
from oracle_llm import run_with_tools, check_ollama_health
from oracle_tools import TOOL_REGISTRY
from prompts.system_prompt import build_system_prompt


# Active sessions
_sessions = {}  # session_id -> {last_activity, history}


def get_or_create_session(session_id: str = None) -> str:
    """Get existing session or create a new one."""
    if session_id and session_id in _sessions:
        _sessions[session_id]["last_activity"] = time.time()
        return session_id

    # Create new session
    new_id = session_id or f"session_{uuid.uuid4().hex[:8]}"
    _sessions[new_id] = {
        "last_activity": time.time(),
        "history": [],
    }
    return new_id


def chat(session_id: str, user_message: str) -> dict:
    """Process a user message and return ORACLE's response.

    Returns:
        {answer: str, tools_called: list, tool_results: dict, session_id: str}
    """
    session_id = get_or_create_session(session_id)
    session = _sessions[session_id]

    # Build current system state for the prompt
    current_state = _get_current_state()

    # Build system prompt
    system_prompt = build_system_prompt(current_state)

    # Log user message
    db.log_conversation(session_id, "user", user_message)

    # Add to session history
    session["history"].append({"role": "user", "content": user_message})

    # Run through LLM with tools
    result = run_with_tools(
        user_message=user_message,
        system_prompt=system_prompt,
        tool_registry=TOOL_REGISTRY,
        conversation_history=session["history"],
    )

    # Log assistant response
    db.log_conversation(
        session_id, "assistant", result["answer"],
        tools_called=result.get("tools_called"),
        system_context=current_state,
    )

    # Add to session history (summarized for context window)
    answer_summary = result["answer"][:500] if len(result["answer"]) > 500 else result["answer"]
    session["history"].append({"role": "assistant", "content": answer_summary})

    # Trim history to max turns
    max_turns = 20  # 10 full exchanges
    if len(session["history"]) > max_turns:
        # Keep first 2 turns + last max_turns-2
        session["history"] = session["history"][:2] + session["history"][-(max_turns-2):]

    session["last_activity"] = time.time()

    return {
        "answer": result["answer"],
        "tools_called": result.get("tools_called", []),
        "tool_results": result.get("tool_results", {}),
        "session_id": session_id,
    }


def get_session_history(session_id: str) -> list:
    """Get conversation history for a session."""
    if session_id in _sessions:
        return _sessions[session_id]["history"]
    # Try loading from DB
    return db.get_conversation_history(session_id)


def cleanup_expired_sessions():
    """Remove sessions that have been inactive for > timeout."""
    timeout = 30 * 60  # 30 minutes
    now = time.time()
    expired = [sid for sid, s in _sessions.items() if now - s["last_activity"] > timeout]
    for sid in expired:
        del _sessions[sid]
    return len(expired)


def _get_current_state() -> dict:
    """Quickly gather current system state for the prompt."""
    state = {
        "active_engines": "checking...",
        "macro_regime": "checking...",
        "open_positions": 0,
        "active_signals": "none",
        "active_directives": "none",
    }

    try:
        from oracle_tools import get_engine_status, get_open_positions, get_cross_engine_signals
        engines = get_engine_status()
        if engines and "error" not in engines:
            armed = []
            if engines.get("hypothesis_engine", {}).get("armed"):
                armed.append("Hypothesis")
            if engines.get("scalper", {}).get("armed"):
                armed.append("Scalper")
            state["active_engines"] = ", ".join(armed) if armed else "None armed"

        positions = get_open_positions()
        if positions and "error" not in positions:
            state["open_positions"] = positions.get("count", 0)

        signals = get_cross_engine_signals()
        if isinstance(signals, list) and signals:
            state["active_signals"] = ", ".join(s.get("signal_type", "") for s in signals[:3])
    except Exception:
        pass

    return state
