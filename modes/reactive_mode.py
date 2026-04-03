"""
ORACLE Mode 1 — Reactive Mode

Engine proposes a rule change, ORACLE validates it via second-opinion system.
This is the most common flow: engines self-optimize and ORACLE acts as gatekeeper.
"""

from ruleset.ruleset_validator import request_second_opinion


def handle_engine_proposal(engine: str, proposed_rule: dict, context: dict = None) -> dict:
    """Process a rule change proposed by an engine.

    Args:
        engine: Which engine proposed the change (e.g., 'scalper', 'sniper')
        proposed_rule: The proposed rule definition
        context: Optional additional context from the engine

    Returns:
        Verdict dict with: decision, confidence, risk_level, reasoning, approval_path
    """
    # Merge any engine-provided context into the proposal for richer validation
    if context:
        proposed_rule = {**proposed_rule, "_engine_context": context}

    verdict = request_second_opinion(proposed_rule, engine, source="engine_proposed")
    return verdict
