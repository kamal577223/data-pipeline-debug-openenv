"""
Baseline inference runner for data-pipeline-debug-openenv.

Required environment variables:
    API_BASE_URL   The API endpoint for the LLM.
    MODEL_NAME     The model identifier to use for inference.
    HF_TOKEN       Your Hugging Face / API key.

This script emits required structured lines:
    [START] task=<task_id>
    [STEP] step=<n> reward=<float>
    [END] task=<task_id> score=<float> steps=<n>
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import requests
from openai import OpenAI

API_BASE_URL = os.getenv("API_BASE_URL", "").strip()
MODEL_NAME = os.getenv("MODEL_NAME", "").strip()
HF_TOKEN = os.getenv("HF_TOKEN", "").strip()
ENV_BASE_URL = os.getenv(
    "ENV_BASE_URL",
    "https://kamal2525-data-pipeline-debug-openenv.hf.space",
).rstrip("/")

TASKS = [
    ("easy", "easy_csv_null_type"),
    ("medium", "medium_schema_drift"),
    ("hard", "hard_dependency_chain"),
]

HTTP = requests.Session()
HTTP.trust_env = False


def _require_env(var_name: str, value: str) -> None:
    if not value:
        print(f"ERROR: required env var not set: {var_name}")
        sys.exit(1)


def _post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = HTTP.post(f"{ENV_BASE_URL}{path}", json=payload, timeout=90)
    response.raise_for_status()
    return response.json()


def _get(path: str) -> dict[str, Any]:
    response = HTTP.get(f"{ENV_BASE_URL}{path}", timeout=60)
    response.raise_for_status()
    return response.json()


def _extract_code_block(text: str) -> str:
    marker = "```"
    if marker not in text:
        return text.strip()
    parts = text.split(marker)
    for chunk in parts:
        normalized = chunk.strip()
        if not normalized:
            continue
        if normalized.startswith("python"):
            return normalized[len("python") :].strip()
        if "def run_pipeline(" in normalized:
            return normalized.strip()
    return text.strip()


def _build_prompt(observation: dict[str, Any]) -> str:
    return (
        "You must fix a broken data pipeline.\n"
        "Return only valid Python code that defines run_pipeline with no extra prose.\n\n"
        f"TASK_ID: {observation.get('task_id')}\n"
        f"DIFFICULTY: {observation.get('difficulty')}\n"
        f"PROMPT: {observation.get('prompt')}\n"
        "EXPECTED_CONTRACT:\n"
        f"{json.dumps(observation.get('expected_contract', {}), indent=2)}\n\n"
        "BROKEN_PIPELINE:\n"
        f"{observation.get('broken_pipeline', '')}\n"
    )


def _propose_fix(client: OpenAI, observation: dict[str, Any]) -> str:
    prompt = _build_prompt(observation)
    completion = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0.0,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an expert Python data engineer. "
                    "Return only executable Python code."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )
    content = completion.choices[0].message.content or ""
    return _extract_code_block(content)


def run_task(client: OpenAI, difficulty: str, task_id: str) -> dict[str, Any]:
    print(f"[START] task={task_id}", flush=True)
    reset_payload = {"episode_id": f"inference-{task_id}-{int(time.time())}"}
    reset_result = _post("/reset", reset_payload)
    observation = reset_result.get("observation", {})

    # Force requested task if server supports extra params.
    if observation.get("task_id") != task_id:
        reset_result = _post(
            "/reset",
            {
                "episode_id": f"inference-{task_id}-{int(time.time())}",
                "difficulty": difficulty,
                "task_id": task_id,
            },
        )
        observation = reset_result.get("observation", {})

    candidate_pipeline = _propose_fix(client, observation)
    step_result = _post(
        "/step",
        {
            "action": {
                "candidate_pipeline": candidate_pipeline,
                "metadata": {"difficulty": difficulty, "task_id": task_id},
            }
        },
    )
    reward = float(step_result.get("reward", 0.0) or 0.0)
    done = bool(step_result.get("done", False))
    final_observation = step_result.get("observation", {})
    score = float(final_observation.get("reward", reward))

    print(f"[STEP] step=1 reward={reward:.4f}", flush=True)
    print(f"[END] task={task_id} score={score:.4f} steps=1", flush=True)

    return {
        "task_id": task_id,
        "score": score,
        "done": done,
        "passed": bool(final_observation.get("passed", False)),
        "feedback": final_observation.get("feedback"),
    }


def main() -> None:
    _require_env("API_BASE_URL", API_BASE_URL)
    _require_env("MODEL_NAME", MODEL_NAME)
    _require_env("HF_TOKEN", HF_TOKEN)

    health = HTTP.get(f"{ENV_BASE_URL}/health", timeout=30)
    health.raise_for_status()
    _get("/schema")
    _get("/state")

    client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)
    started = time.time()

    results: list[dict[str, Any]] = []
    for difficulty, task_id in TASKS:
        results.append(run_task(client, difficulty=difficulty, task_id=task_id))

    avg_score = sum(item["score"] for item in results) / len(results)
    duration_s = time.time() - started

    print("\nSummary")
    print(json.dumps({"results": results, "avg_score": avg_score, "duration_s": duration_s}, indent=2))

    if duration_s > 20 * 60:
        print("ERROR: inference exceeded 20 minutes")
        sys.exit(2)


if __name__ == "__main__":
    main()
