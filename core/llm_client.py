"""
LLM HTTP Client - Compatible Ollama, OpenAI, LM Studio, vLLM, text-generation-webui
Zero external dependencies - pure stdlib urllib
"""
import json
import urllib.request
import urllib.error
import urllib.parse
import time
from typing import List, Dict, Optional, Any


class LLMError(Exception):
    pass


class LLMClient:
    """
    Client HTTP universel pour LLM local.
    Supporte: Ollama (/api/chat), OpenAI-compatible (/v1/chat/completions),
              ou tout endpoint HTTP personnalisé.
    """

    def __init__(self, config: dict):
        self.base_url = config["base_url"].rstrip("/")
        self.api_type  = config.get("api_type", "openai")   # "ollama" | "openai" | "custom"
        self.model     = config["model"]
        self.temperature = float(config.get("temperature", 0.1))
        self.max_tokens  = int(config.get("max_tokens", 4096))
        self.timeout     = int(config.get("timeout", 120))
        self.api_key     = config.get("api_key", "not-needed")
        self.custom_endpoint = config.get("custom_endpoint", "")  # only for api_type=custom

        # Endpoint endpoints
        self._endpoints = {
            "ollama": "/api/chat",
            "openai": "/v1/chat/completions",
        }

    # ------------------------------------------------------------------ #
    #  Public interface                                                    #
    # ------------------------------------------------------------------ #

    def complete(self, messages: List[Dict[str, str]], stop: Optional[List[str]] = None) -> str:
        """Send messages → return assistant text reply (string)."""
        if self.api_type == "ollama":
            return self._ollama_complete(messages)
        else:
            return self._openai_complete(messages, stop=stop)

    def complete_json(self, messages: List[Dict[str, str]]) -> Any:
        """
        Like complete() but forces the LLM to return valid JSON.
        Retries up to 3 times if parsing fails.
        """
        for attempt in range(3):
            raw = self.complete(messages)
            parsed = self._extract_json(raw)
            if parsed is not None:
                return parsed
            # Ask the LLM to fix its output
            messages = messages + [
                {"role": "assistant", "content": raw},
                {"role": "user",
                 "content": "Your previous response was not valid JSON. "
                             "Reply ONLY with a valid JSON object, no markdown fences."},
            ]
        raise LLMError(f"LLM failed to produce valid JSON after 3 attempts. Last raw: {raw}")

    def ping(self) -> bool:
        """Test that the LLM endpoint is reachable."""
        try:
            self.complete([{"role": "user", "content": "ping"}])
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Internal implementations                                           #
    # ------------------------------------------------------------------ #

    def _post(self, url: str, payload: dict) -> dict:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {self.api_key}")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise LLMError(f"HTTP {e.code} from LLM: {body}") from e
        except urllib.error.URLError as e:
            raise LLMError(f"Cannot reach LLM at {url}: {e.reason}") from e

    def _openai_complete(self, messages: List[Dict], stop: Optional[List[str]] = None) -> str:
        url = self.base_url + (self.custom_endpoint or self._endpoints["openai"])
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        if stop:
            payload["stop"] = stop

        result = self._post(url, payload)
        choice = result["choices"][0]
        return (choice.get("message") or {}).get("content", "").strip()

    def _ollama_complete(self, messages: List[Dict]) -> str:
        url = self.base_url + self._endpoints["ollama"]
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            },
        }
        result = self._post(url, payload)
        return result.get("message", {}).get("content", "").strip()

    @staticmethod
    def _extract_json(text: str) -> Optional[Any]:
        """Extract first valid JSON object or array from text."""
        text = text.strip()
        # Strip markdown code fences
        for fence in ("```json", "```JSON", "```"):
            if fence in text:
                parts = text.split(fence)
                for p in parts[1:]:
                    end = p.find("```")
                    candidate = p[:end].strip() if end != -1 else p.strip()
                    try:
                        return json.loads(candidate)
                    except Exception:
                        pass
        # Direct parse
        try:
            return json.loads(text)
        except Exception:
            pass
        # Find first { or [ and try from there
        for start_char, end_char in [('{', '}'), ('[', ']')]:
            idx = text.find(start_char)
            if idx != -1:
                # Find matching closing bracket (simple heuristic)
                for end_idx in range(len(text) - 1, idx, -1):
                    if text[end_idx] == end_char:
                        candidate = text[idx:end_idx + 1]
                        try:
                            return json.loads(candidate)
                        except Exception:
                            pass
        return None
