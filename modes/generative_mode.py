"""
ORACLE Mode 3 — Generative Mode

Builds full rulesets from scratch on demand. Uses complete market context,
historical performance, and LLM reasoning to generate optimized rule configurations.
"""

import time
import json
import oracle_db as db
from ruleset.ruleset_manager import assemble_generation_context
from ruleset.ruleset_store import create_ruleset

# Try to import LLM caller; allow graceful fallback
try:
    from oracle_llm import call_qwen
except ImportError:
    call_qwen = None


def _build_generation_prompt(engine: str, context: dict, user_request: str = None) -> str:
    """Build the LLM prompt for full ruleset generation."""
    user_section = ""
    if user_request:
        user_section = f"\nUSER REQUEST: {user_request}\n"

    return f"""You are ORACLE, an autonomous trading oversight system. Generate a complete ruleset for the {engine} engine.
{user_section}
CURRENT MARKET CONTEXT:
- Macro environment: {json.dumps(context.get('current_macro'), default=str)}
- Market regime: {json.dumps(context.get('current_regime'), default=str)}
- Recent performance (24h): {json.dumps(context.get('recent_performance'), default=str)}
- Pattern learner insights: {json.dumps(context.get('pattern_insights'), default=str)}
- Enrichment accuracy: {json.dumps(context.get('enrichment_accuracy'), default=str)}
- Active cross-engine signals: {json.dumps(context.get('active_signals'), default=str)}
- Active directives: {json.dumps(context.get('active_directives'), default=str)}

HISTORICAL RULESETS (last 5):
{json.dumps(context.get('historical_rulesets', []), indent=2, default=str)}

Generate a complete ruleset optimized for current conditions. Consider:
1. What worked and what failed in historical rulesets
2. Current market regime and appropriate strategy adjustments
3. Cross-engine signals that should influence rules
4. Risk management appropriate for the environment
5. Any active directives from the operator

Respond in EXACTLY this JSON format:
{{
    "rules": {{
        "entry_conditions": [...],
        "exit_conditions": [...],
        "position_sizing": {{...}},
        "risk_limits": {{...}},
        "filters": [...],
        "bias": "bullish" or "bearish" or "neutral"
    }},
    "reasoning": "detailed explanation of why these rules fit current conditions",
    "recommended_ttl_hours": 24,
    "confidence": 0.0 to 1.0
}}"""


def _parse_ruleset_response(llm_response: str) -> dict:
    """Parse the LLM response into a structured ruleset."""
    try:
        text = llm_response.strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end])
            return {
                "rules": parsed.get("rules", {}),
                "reasoning": parsed.get("reasoning", "No reasoning provided"),
                "recommended_ttl_hours": parsed.get("recommended_ttl_hours", 24),
                "confidence": max(0.0, min(1.0, float(parsed.get("confidence", 0.5)))),
            }
    except (json.JSONDecodeError, ValueError, TypeError):
        pass

    return None


def generate_ruleset(engine: str, user_request: str = None) -> dict:
    """Generate a full ruleset from scratch for the given engine.

    Args:
        engine: Target engine (e.g., 'scalper', 'sniper')
        user_request: Optional user instructions to guide generation

    Returns:
        dict with: ruleset_id, rules, reasoning, status, confidence
        or error dict if generation failed
    """
    if call_qwen is None:
        return {
            "error": "LLM module not available. Cannot generate rulesets without Qwen.",
            "status": "failed",
        }

    # 1. Assemble full context
    context = assemble_generation_context(engine)

    # 2. Build generation prompt
    prompt = _build_generation_prompt(engine, context, user_request)

    # 3. Call Qwen
    try:
        llm_response = call_qwen(prompt, temperature=0.4)
    except Exception as e:
        return {
            "error": f"LLM call failed: {str(e)}",
            "status": "failed",
        }

    # 4. Parse ruleset from response
    parsed = _parse_ruleset_response(llm_response)
    if parsed is None:
        return {
            "error": "Failed to parse LLM response into valid ruleset",
            "status": "failed",
            "raw_response": llm_response[:500],
        }

    # 5. Store in DB as pending_approval
    ttl = parsed.get("recommended_ttl_hours", 24)
    ruleset_id = create_ruleset(
        engine=engine,
        rules=parsed["rules"],
        reasoning=parsed["reasoning"],
        market_context={
            "regime": context.get("current_regime"),
            "macro": context.get("current_macro"),
            "generated_at": time.time(),
            "user_request": user_request,
        },
        ttl_hours=ttl,
    )

    return {
        "ruleset_id": ruleset_id,
        "rules": parsed["rules"],
        "reasoning": parsed["reasoning"],
        "confidence": parsed["confidence"],
        "ttl_hours": ttl,
        "status": "pending_approval",
    }
