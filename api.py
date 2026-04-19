"""HTTP client for Dify API (https://api.dify.ai/v1)."""

from __future__ import annotations

import os
import json
from typing import Any, Iterable, Literal
from urllib.parse import urljoin

import requests

DEFAULT_DIFY_BASE_URL = "https://api.dify.ai/v1"


def get_dify_api_key(explicit: str | None = None) -> str:
    """Resolve API key from argument or DIFY_API_KEY environment variable."""
    key = (explicit or os.getenv("DIFY_API_KEY", "")).strip()
    if not key:
        raise ValueError(
            "Dify API key is not set. Define environment variable DIFY_API_KEY "
            "(recommended) or pass api_key= to send_dify_chat_message."
        )
    return key


def send_dify_chat_message(
    prompt: str,
    *,
    stock_list: str,
    base_url: str = DEFAULT_DIFY_BASE_URL,
    api_key: str | None = None,
    user: str = "streamlit-dashboard",
    response_mode: Literal["streaming"] = "streaming",
    timeout: int = 60,
) -> Iterable[str]:
    """
    Run a Dify Workflow via POST /v1/workflows/run (streaming).

    Request body format (as configured in the Dify Workflow API docs):

    {
      "inputs": {"userinput": "...", "stock_list": "..."},
      "response_mode": "streaming",
      "user": "unique_user_id_123"
    }
    """
    key = get_dify_api_key(explicit=api_key)
    root = base_url.rstrip("/") + "/"
    url = urljoin(root, "workflows/run")

    payload: dict[str, Any] = {
        "inputs": {
            "userinput": prompt.strip(),
            "stock_list": stock_list,
        },
        "response_mode": response_mode,
        "user": user,
    }

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        url,
        json=payload,
        headers=headers,
        timeout=timeout,
        stream=True,
    )
    response.raise_for_status()

    def _iter_tokens() -> Iterable[str]:
        # Dify streaming responses are SSE (text/event-stream).
        for raw in response.iter_lines(decode_unicode=True):
            if not raw:
                continue
            line = raw.strip()
            if not line.startswith("data:"):
                continue
            data = line[len("data:") :].strip()
            if not data or data == "[DONE]":
                break
            try:
                event = json.loads(data)
            except Exception:
                # Best-effort fallback: emit raw payload text.
                yield data
                continue

            # Best-effort extraction across common Dify workflow streaming event shapes.
            if isinstance(event, dict):
                payload_obj = event.get("data", event)
                if isinstance(payload_obj, dict):
                    if isinstance(payload_obj.get("text"), str):
                        yield payload_obj["text"]
                        continue
                    if isinstance(payload_obj.get("delta"), str):
                        yield payload_obj["delta"]
                        continue
                    outputs = payload_obj.get("outputs")
                    if isinstance(outputs, dict):
                        for k in ("text", "answer", "output", "result"):
                            if isinstance(outputs.get(k), str) and outputs[k]:
                                yield outputs[k]
                                break
                        continue

            # If we can't extract a token, ignore the event (still valid SSE traffic).
            continue

    return _iter_tokens()
