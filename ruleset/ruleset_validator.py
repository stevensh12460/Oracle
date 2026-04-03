"""
ORACLE Ruleset Validator — second-opinion system for proposed rule changes.

Uses full market context + LLM to validate proposals before they go live.
Determines approval path: auto-approve (low risk + high confidence) or human review.
"""

import time
import json
import oracle_db as db
from ruleset.ruleset_manager import assemble_generation_context

# Try to import LLM caller; allow graceful fallback
try:
    from oracle_llm import call_qwen
except ImportError:
    call_qwen = None

AUTO_APPROVE_CONFIDENCE = 0.80
AUTO_APPROVE_MAX_RISK = "low"

RISK_LEVELS = ["low", "medium", "high", "critical"]


def _build_validation_prompt(proposed_rule: dict, engine: str, context: dict) -> str:
    """Build the LLM prompt for second-opinion validation."""
    return f"""You are ORACLE, an autonomous trading oversight system. A rule change has been proposed.

ENGINE: {engine}
PROPOSED RULE:
{json.dumps(proposed_rule, indent=2)}

CURRENT MARKET CONTEXT:
- Macro regime: {json.dumps(context.get('current_macro'), default=str)}
- Market regime: {json.dumps(context.get('current_regime'), default=str)}
- Recent performance (24h): {json.dumps(context.get('recent_performance'), default=str)}
- Active directives: {json.dumps(context.get('active_directives'), default=str)}
- Pattern insights: {json.dumps(context.get('pattern_insights'), default=str)}
- Enrichment accuracy: {json.dumps(context.get('enrichment_accuracy'), default=str)}
- Cross-engine signals: {json.dumps(context.get('active_signals'), default=str)}

HISTORICAL RULESETS (last 5):
{json.dumps(context.get('historical_rulesets', []), indent=2, default=str)}

Evaluate this proposed rule change. Consider:
1. Does it align with the current market regime?
2. Does it conflict with active directives?
3. Could it cause excessive risk or drawdown?
4. Does historical performance support this type of rule?
5. Are there cross-engine signals that contradict it?

Respond in EXACTLY this JSON format:
{{
    "decision": "APPROVE" or "REJECT" or "MODIFY",
    "confidence": 0.0 to 1.0,
    "risk_level": "low" or "medium" or "high" or "critical",
    "reasoning": "detailed explanation",
    "suggested_modifications": null or {{...}}
}}"""


def _parse_verdict(llm_response: str) -> dict:
    """Parse the LLM response into a structured verdict."""
    # Try to extract JSON from the response
    try:
        # Look for JSON block in response
        text = llm_response.strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end])
            # Validate required fields
            verdict = {
                "decision": parsed.get("decision", "REJECT").upper(),
                "confidence": float(parsed.get("confidence", 0.0)),
                "risk_level": parsed.get("risk_level", "high").lower(),
                "reasoning": parsed.get("reasoning", "No reasoning provided"),
                "suggested_modifications": parsed.get("suggested_modifications"),
            }
            # Clamp confidence
            verdict["confidence"] = max(0.0, min(1.0, verdict["confidence"]))
            # Validate decision
            if verdict["decision"] not in ("APPROVE", "REJECT", "MODIFY"):
                verdict["decision"] = "REJECT"
            # Validate risk level
            if verdict["risk_level"] not in RISK_LEVELS:
                verdict["risk_level"] = "high"
            return verdict
    except (json.JSONDecodeError, ValueError, TypeError):
        pass

    # Fallback: could not parse, reject with explanation
    return {
        "decision": "REJECT",
        "confidence": 0.0,
        "risk_level": "high",
        "reasoning": f"Failed to parse LLM validation response: {llm_response[:200]}",
        "suggested_modifications": None,
    }


def _determine_approval_path(verdict: dict) -> str:
    """Determine whether this can be auto-approved or needs human review.

    Auto-approve if:
      - decision is APPROVE
      - risk_level is 'low'
      - confidence >= 0.80
    Otherwise requires human approval.
    """
    if (
        verdict["decision"] == "APPROVE"
        and verdict["risk_level"] == AUTO_APPROVE_MAX_RISK
        and verdict["confidence"] >= AUTO_APPROVE_CONFIDENCE
    ):
        return "auto_approved"
    elif verdict["decision"] == "REJECT":
        return "auto_rejected"
    else:
        return "pending_human_review"


def request_second_opinion(proposed_rule: dict, engine: str, source: str) -> dict:
    """Validate a proposed rule change using full context + LLM.

    Args:
        proposed_rule: The rule being proposed (dict with rule definition)
        engine: Which engine proposed it (e.g., 'scalper', 'sniper')
        source: Origin of proposal (e.g., 'engine_proposed', 'pattern_learner', 'user')

    Returns:
        dict with: decision, confidence, risk_level, reasoning, approval_path
    """
    now = time.time()

    # 1. Assemble full context
    context = assemble_generation_context(engine)

    # 2. Build validation prompt
    prompt = _build_validation_prompt(proposed_rule, engine, context)

    # 3. Call Qwen with low temperature for deterministic analysis
    if call_qwen is not None:
        try:
            llm_response = call_qwen(prompt, temperature=0.2)
        except Exception as e:
            llm_response = None
            verdict = {
                "decision": "REJECT",
                "confidence": 0.0,
                "risk_level": "high",
                "reasoning": f"LLM unavailable: {str(e)}. Defaulting to REJECT for safety.",
                "suggested_modifications": None,
            }
    else:
        llm_response = None
        verdict = {
            "decision": "REJECT",
            "confidence": 0.0,
            "risk_level": "high",
            "reasoning": "LLM module not available. Cannot validate without second opinion.",
            "suggested_modifications": None,
        }

    # 4. Parse verdict from LLM response
    if llm_response is not None:
        verdict = _parse_verdict(llm_response)

    # 5. Determine approval path
    approval_path = _determine_approval_path(verdict)
    verdict["approval_path"] = approval_path

    # 6. Store in DB
    record = {
        "engine": engine,
        "source": source,
        "proposed_rule": proposed_rule,
        "verdict": verdict,
        "context_snapshot": {
            "regime": context.get("current_regime"),
            "macro": context.get("current_macro"),
        },
        "timestamp": now,
    }
    try:
        db.store_validation(record)
    except Exception:
        pass  # Don't fail the validation if DB write fails

    return verdict
