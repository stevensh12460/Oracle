"""ORACLE LLM Interface — Qwen 2.5 Coder 14B via Ollama with tool call parsing.

Handles prompt construction, tool call extraction, retry logic, and failover.
"""

import json
import re
import time
import requests
from pathlib import Path

import oracle_db as db

# Load config
_config = {}
_config_path = Path(__file__).parent / "oracle_config.json"


def _load_config():
    global _config
    with open(_config_path) as f:
        _config = json.load(f)


def _get_ollama_url():
    """Get the active Ollama endpoint URL."""
    if not _config:
        _load_config()
    ollama = _config.get("ollama", {})

    # Try primary first
    primary = ollama.get("oracle_primary", {})
    if primary.get("active"):
        return f"http://{primary['host']}:{primary['port']}"

    # Fallback to secondary
    secondary = ollama.get("oracle_secondary", {})
    if secondary.get("active"):
        return f"http://{secondary['host']}:{secondary['port']}"

    # Default to primary anyway
    return f"http://{primary.get('host', '192.168.158.203')}:{primary.get('port', 5600)}"


def _get_model():
    """Get the configured model name."""
    if not _config:
        _load_config()
    primary = _config.get("ollama", {}).get("oracle_primary", {})
    return primary.get("model", "qwen2.5-coder:14b")


def call_qwen(prompt: str, system: str, temperature: float = 0.3,
              max_tokens: int = 2000) -> str:
    """Make a single Ollama call. Returns raw text response.

    Handles failover between primary and secondary Ollama instances.
    Logs every call for monitoring.
    """
    if not _config:
        _load_config()

    model = _get_model()
    timeout = _config.get("llm_settings", {}).get("timeout_seconds", 120)

    # Try primary, then secondary on failure
    endpoints = []
    ollama = _config.get("ollama", {})
    for key in ["oracle_primary", "oracle_secondary"]:
        inst = ollama.get(key, {})
        if inst.get("active", False) or key == "oracle_primary":
            endpoints.append(f"http://{inst['host']}:{inst['port']}")

    for url in endpoints:
        start_time = time.time()
        try:
            resp = requests.post(
                f"{url}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "system": system,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": max_tokens,
                        "top_p": 0.85,
                        "repeat_penalty": 1.15,
                    },
                },
                timeout=timeout,
            )
            resp.raise_for_status()
            result = resp.json().get("response", "").strip()
            latency = int((time.time() - start_time) * 1000)

            db.log_llm_call(
                prompt_length=len(prompt),
                response_length=len(result),
                tools_called=None,
                latency_ms=latency,
                endpoint=url,
                success=True,
            )

            return result

        except Exception as e:
            latency = int((time.time() - start_time) * 1000)
            db.log_llm_call(
                prompt_length=len(prompt),
                response_length=0,
                tools_called=None,
                latency_ms=latency,
                endpoint=url,
                success=False,
                error=str(e),
            )
            print(f"  [ORACLE LLM] {url} failed: {e}")
            continue

    return "[ORACLE ERROR: All Ollama endpoints unreachable. The trading system is unaffected and running normally.]"


def extract_tool_calls(response: str) -> list[dict]:
    """Extract tool call blocks from Qwen's response.

    Handles multiple formats:
    - <tool_call>{"tool": "...", "params": {...}}</tool_call>
    - ```json\n{"tool": "...", "params": {...}}\n```
    - Bare JSON objects with "tool" key
    """
    tool_calls = []

    # Pattern 1: <tool_call> tags
    pattern1 = r'<tool_call>\s*(.*?)\s*</tool_call>'
    for match in re.findall(pattern1, response, re.DOTALL):
        parsed = _try_parse_tool(match)
        if parsed:
            tool_calls.append(parsed)

    # Pattern 2: JSON code blocks with "tool" key
    if not tool_calls:
        pattern2 = r'```(?:json)?\s*(\{[^`]*?"tool"[^`]*?\})\s*```'
        for match in re.findall(pattern2, response, re.DOTALL):
            parsed = _try_parse_tool(match)
            if parsed:
                tool_calls.append(parsed)

    # Pattern 3: Bare JSON objects with "tool" key (last resort)
    if not tool_calls:
        pattern3 = r'\{\s*"tool"\s*:\s*"[^"]+"\s*,\s*"params"\s*:\s*\{[^}]*\}\s*\}'
        for match in re.findall(pattern3, response):
            parsed = _try_parse_tool(match)
            if parsed:
                tool_calls.append(parsed)

    return tool_calls


def _try_parse_tool(text: str) -> dict | None:
    """Try to parse a tool call from text."""
    text = text.strip()
    try:
        parsed = json.loads(text)
        if "tool" in parsed:
            return parsed
    except json.JSONDecodeError:
        fixed = _try_fix_json(text)
        if fixed and "tool" in fixed:
            return fixed
    return None


def _try_fix_json(text: str) -> dict | None:
    """Attempt to fix malformed JSON from Qwen."""
    # Remove trailing commas before closing braces
    text = re.sub(r',\s*}', '}', text)
    text = re.sub(r',\s*]', ']', text)

    # Fix single quotes to double quotes
    text = text.replace("'", '"')

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def run_with_tools(user_message: str, system_prompt: str, tool_registry: dict,
                   conversation_history: list = None, temperature: float = 0.3,
                   max_tokens: int = 3000) -> dict:
    """Two-stage tool calling:
    Stage 1: Qwen selects relevant categories (8 options, not 127)
    Stage 2: Qwen picks tools from selected categories only
    """
    if not _config:
        _load_config()
    max_retries = _config.get("llm_settings", {}).get("max_retries", 2)

    # Build history text
    history_text = ""
    if conversation_history:
        for turn in conversation_history[-10:]:
            role = turn.get("role", "user")
            content = turn.get("content", "")
            if len(content) > 500:
                content = content[:500] + "..."
            history_text += f"\n{role.upper()}: {content}\n"

    # ── STAGE 1: Category Selection ──
    from prompts.system_prompt import build_category_prompt
    from oracle_tools import get_tools_for_categories, TOOL_CATEGORIES
    from oracle_conversation import _get_current_state

    current_state = _get_current_state()
    category_prompt = build_category_prompt(current_state)

    stage1_prompt = f"{history_text}\nUSER: {user_message}\n\nSelect the categories needed:"

    category_response = call_qwen(stage1_prompt, category_prompt, temperature=0.2, max_tokens=200)

    # Parse categories from response
    selected_categories = []
    for cat_name in TOOL_CATEGORIES:
        if cat_name in category_response.upper():
            selected_categories.append(cat_name)

    # Fallback: if no categories parsed, use SYSTEM_INTELLIGENCE + MARKET_CONTEXT
    if not selected_categories:
        selected_categories = ["SYSTEM_INTELLIGENCE", "MARKET_CONTEXT"]

    # ── STAGE 2: Tool Selection with filtered tools ──
    from prompts.system_prompt import build_tool_prompt
    tool_descriptions, filtered_registry = get_tools_for_categories(selected_categories)
    stage2_system = build_tool_prompt(selected_categories, current_state)

    full_prompt = f"{history_text}\nUSER: {user_message}\n\nUse the available tools to answer. Call all needed tools:\n\nASSISTANT:"

    response = call_qwen(full_prompt, stage2_system, temperature=temperature, max_tokens=max_tokens)

    # Extract tool calls
    tool_calls = extract_tool_calls(response)

    # Retry if malformed
    retry_count = 0
    while not tool_calls and "<tool" in response.lower() and retry_count < max_retries:
        retry_prompt = full_prompt + "\n\n" + response + \
            "\n\nYour tool call was malformed. Use this format:\n<tool_call>\n{\"tool\": \"name\", \"params\": {}}\n</tool_call>"
        response = call_qwen(retry_prompt, stage2_system, temperature=0.2, max_tokens=max_tokens)
        tool_calls = extract_tool_calls(response)
        retry_count += 1

    # Execute tool calls (use filtered registry but fall back to full registry)
    tool_results = {}
    tools_called_names = []

    for tc in tool_calls:
        tool_name = tc.get("tool", "")
        params = tc.get("params", {})

        # Try filtered first, fall back to full registry
        func = filtered_registry.get(tool_name) or tool_registry.get(tool_name)

        if func:
            try:
                start = time.time()
                result = func(**params)
                latency = int((time.time() - start) * 1000)
                tool_results[tool_name] = {"data": _sanitize_result(result), "latency_ms": latency, "success": True}
                tools_called_names.append(tool_name)
                # Log telemetry
                try:
                    import oracle_db
                    oracle_db.log_tool_call(tool_name, "read", None, latency, True,
                                           len(str(result)) if result else 0)
                except Exception:
                    pass
            except Exception as e:
                tool_results[tool_name] = {"error": str(e), "success": False}
                tools_called_names.append(f"{tool_name}(FAILED)")
                try:
                    import oracle_db
                    oracle_db.log_tool_call(tool_name, "read", None, 0, False, 0, error=str(e))
                except Exception:
                    pass
        else:
            tool_results[tool_name] = {"error": f"Unknown tool: {tool_name}", "success": False}

    # Synthesis
    if tool_results:
        results_text = "\n".join(
            f"[Tool: {name}]\n{json.dumps(data.get('data', data.get('error', '')), indent=2, default=str)[:2000]}"
            for name, data in tool_results.items()
        )

        synthesis_prompt = f"USER: {user_message}\n\nI called tools and got:\n\n{results_text}\n\nAnswer directly using the data. No tool calls. Plain English with specific numbers.\n\nANSWER:"
        synthesis_system = "You are ORACLE. Answer using the data provided. Be concise. Use specific numbers. No JSON, no code blocks."

        final_answer = call_qwen(synthesis_prompt, synthesis_system,
                                 temperature=_config.get("llm_settings", {}).get("conversation_temperature", 0.6),
                                 max_tokens=max_tokens)
    else:
        final_answer = re.sub(r'<tool_call>.*$', '', response, flags=re.DOTALL).strip()
        if not final_answer:
            final_answer = response

    return {"answer": final_answer, "tools_called": tools_called_names, "tool_results": tool_results,
            "categories_selected": selected_categories}


def _sanitize_result(result) -> any:
    """Sanitize tool results for context window efficiency."""
    if not _config:
        _load_config()

    max_items = _config.get("conversation", {}).get("max_array_items_in_prompt", 20)
    max_chars = _config.get("conversation", {}).get("max_tool_result_chars", 2000)

    if isinstance(result, list):
        result = result[:max_items]
    elif isinstance(result, dict):
        # Remove None values
        result = {k: v for k, v in result.items() if v is not None}
        # Truncate nested lists
        for k, v in result.items():
            if isinstance(v, list) and len(v) > max_items:
                result[k] = v[:max_items]

    # Final size check
    text = json.dumps(result, default=str)
    if len(text) > max_chars:
        return {"summary": text[:max_chars] + "...[truncated]", "full_length": len(text)}

    return result


def check_ollama_health() -> dict:
    """Check if Ollama is reachable and which endpoint is active."""
    if not _config:
        _load_config()

    status = {"primary": False, "secondary": False, "active": None}
    ollama = _config.get("ollama", {})

    for key in ["oracle_primary", "oracle_secondary"]:
        inst = ollama.get(key, {})
        url = f"http://{inst.get('host', '')}:{inst.get('port', '')}"
        try:
            r = requests.get(f"{url}/api/tags", timeout=5)
            online = r.status_code == 200
            status[key.replace("oracle_", "")] = online
            if online and not status["active"]:
                status["active"] = key
        except Exception:
            pass

    return status
