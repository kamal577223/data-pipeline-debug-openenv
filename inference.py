"""
Inference Script - data-pipeline-debug-openenv
=============================================
MANDATORY VARIABLES
- API_BASE_URL
- MODEL_NAME
- OPENAI_API_KEY (preferred) / HF_TOKEN (fallback)
- LOCAL_IMAGE_NAME (only used when running via local Docker image workflow)

STDOUT FORMAT (strict):
[START] task=<task_name> env=<benchmark> model=<model_name>
[STEP] step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
[END] success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>
"""

from __future__ import annotations

import json
import os
import re
import textwrap
import time
from typing import Any

import requests
from openai import OpenAI

API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
HF_TOKEN = os.getenv("HF_TOKEN", "")
LOCAL_IMAGE_NAME = os.getenv("LOCAL_IMAGE_NAME", "")
ENV_BASE_URL = os.getenv("ENV_BASE_URL", "https://kamal2525-data-pipeline-debug-openenv.hf.space").rstrip("/")
BENCHMARK = os.getenv("BENCHMARK", "data_pipeline_debug_env")
MAX_STEPS = 1

TASKS: list[tuple[str, str]] = [
    ("easy", "easy_csv_null_type"),
    ("medium", "medium_schema_drift"),
    ("hard", "hard_dependency_chain"),
]

HTTP = requests.Session()
HTTP.trust_env = False

SYSTEM_PROMPT = textwrap.dedent(
    """
    You are a data pipeline debugging assistant.
    Return ONLY executable Python code.
    Your code must define run_pipeline and satisfy the provided contract.
    """
).strip()


def _bool_lower(value: bool) -> str:
    return str(bool(value)).lower()


def _sanitize_single_line(value: str, limit: int = 200) -> str:
    sanitized = value.replace("\n", "\\n").replace("\r", " ").strip()
    if len(sanitized) > limit:
        return sanitized[:limit]
    return sanitized


def _safe_error(value: str | None) -> str:
    if value is None or value == "":
        return "null"
    return _sanitize_single_line(value, limit=240)


def _strict_unit_interval(value: float) -> float:
    epsilon = 0.01
    return float(max(epsilon, min(1.0 - epsilon, value)))


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: str | None) -> None:
    print(
        f"[STEP] step={step} action={_sanitize_single_line(action)} reward={reward:.2f} "
        f"done={_bool_lower(done)} error={_safe_error(error)}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: list[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={_bool_lower(success)} steps={steps} score={score:.2f} rewards={rewards_str}",
        flush=True,
    )


def _post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = HTTP.post(f"{ENV_BASE_URL}{path}", json=payload, timeout=90)
    response.raise_for_status()
    return response.json()


def _extract_python_code(text: str) -> str:
    fenced = re.findall(r"```(?:python)?\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced[0].strip()
    return text.strip()


def _build_user_prompt(observation: dict[str, Any]) -> str:
    return (
        "Fix this broken pipeline. Return only Python code.\n"
        f"TASK: {observation.get('task_id')}\n"
        f"DIFFICULTY: {observation.get('difficulty')}\n"
        f"PROMPT: {observation.get('prompt')}\n"
        f"EXPECTED_CONTRACT: {json.dumps(observation.get('expected_contract', {}), ensure_ascii=True)}\n\n"
        "BROKEN_PIPELINE:\n"
        f"{observation.get('broken_pipeline', '')}\n"
    )


def _propose_action(client: OpenAI, observation: dict[str, Any]) -> str:
    completion = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(observation)},
        ],
        temperature=0.1,
        max_tokens=1200,
    )
    raw = (completion.choices[0].message.content or "").strip()
    return _extract_python_code(raw)


def run_task(client: OpenAI, difficulty: str, task_id: str) -> None:
    rewards: list[float] = []
    steps_taken = 0
    score = 0.0
    success = False
    done = False
    last_error: str | None = None
    last_action = "noop"

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)

    try:
        reset = _post(
            "/reset",
            {
                "episode_id": f"inference-{task_id}-{int(time.time())}",
                "difficulty": difficulty,
                "task_id": task_id,
            },
        )
        observation = reset.get("observation", {})

        for step in range(1, MAX_STEPS + 1):
            candidate_pipeline = _propose_action(client, observation)
            last_action = candidate_pipeline

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
            obs = step_result.get("observation", {})
            last_error = None if obs.get("passed", False) else obs.get("feedback")

            rewards.append(reward)
            steps_taken = step
            log_step(step=step, action=last_action, reward=reward, done=done, error=last_error)

            score = float(obs.get("reward", reward))
            score = _strict_unit_interval(score)
            success = bool(obs.get("passed", False)) and 0.0 < score < 1.0
            break

    except Exception as exc:
        last_error = str(exc)
        if steps_taken == 0:
            log_step(step=1, action=last_action, reward=0.0, done=False, error=last_error)
            steps_taken = 1
            rewards.append(0.0)
        success = False
        score = _strict_unit_interval(0.0)
    finally:
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)


def main() -> None:
    api_key = OPENAI_API_KEY or HF_TOKEN
    if not api_key:
        raise SystemExit("OPENAI_API_KEY or HF_TOKEN is required")
    if not API_BASE_URL:
        raise SystemExit("API_BASE_URL is required")
    if not MODEL_NAME:
        raise SystemExit("MODEL_NAME is required")

    # LOCAL_IMAGE_NAME is required by the challenge checklist for env configuration.
    # This script runs against ENV_BASE_URL. If LOCAL_IMAGE_NAME is set, it is accepted
    # as configured but not auto-started from Python.
    _ = LOCAL_IMAGE_NAME

    # Basic health check before task loop.
    health = HTTP.get(f"{ENV_BASE_URL}/health", timeout=30)
    health.raise_for_status()

    client = OpenAI(base_url=API_BASE_URL, api_key=api_key)
    for difficulty, task_id in TASKS:
        run_task(client, difficulty, task_id)


if __name__ == "__main__":
    main()
