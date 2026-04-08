"""
Pre-submission validation for data-pipeline-debug-openenv.

Checks:
1. HF Space deploy + ping root and /reset
2. OpenEnv spec compliance (openenv.yaml + /schema + /state + /step + /reset)
3. Dockerfile presence and local docker build command viability
4. Baseline inference script existence and required env vars references
5. 3 tasks with graders and reward range 0.0..1.0
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests
import yaml

ROOT = Path(__file__).resolve().parent
ENV_DIR = ROOT / "envs" / "data_pipeline_debug_env"
SPACE_URL = "https://kamal2525-data-pipeline-debug-openenv.hf.space"
HTTP = requests.Session()
HTTP.trust_env = False


def ok(name: str, detail: str = "") -> None:
    print(f"[PASS] {name}" + (f" :: {detail}" if detail else ""))


def fail(name: str, detail: str = "") -> None:
    print(f"[FAIL] {name}" + (f" :: {detail}" if detail else ""))
    raise RuntimeError(name)


def request_json(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    last_error: Exception | None = None
    for _ in range(4):
        try:
            if method == "GET":
                response = HTTP.get(f"{SPACE_URL}{path}", timeout=30)
            else:
                response = HTTP.post(f"{SPACE_URL}{path}", json=payload or {}, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            time.sleep(1.2)
    raise RuntimeError(f"{method} {path} failed after retries: {last_error}")


def check_space_ping_and_reset() -> None:
    r_root = HTTP.get(f"{SPACE_URL}/", timeout=30)
    if r_root.status_code != 200:
        fail("HF Space root ping", f"status={r_root.status_code}")
    _ = request_json("POST", "/reset", {"episode_id": "pre-validation", "seed": 1})
    ok("HF Space deploys", "root and reset are healthy")


def check_spec_compliance() -> None:
    manifest = yaml.safe_load((ENV_DIR / "openenv.yaml").read_text(encoding="utf-8"))
    required = {
        "spec_version": 1,
        "type": "space",
        "runtime": "fastapi",
        "app": "server.app:app",
    }
    for key, value in required.items():
        if manifest.get(key) != value:
            fail("OpenEnv manifest", f"{key} expected={value} got={manifest.get(key)}")

    schema = request_json("GET", "/schema")
    if "action" not in schema or "observation" not in schema or "state" not in schema:
        fail("OpenEnv schema", "missing action/observation/state")
    if "candidate_pipeline" not in json.dumps(schema["action"]):
        fail("OpenEnv schema", "candidate_pipeline not found in action schema")

    _ = request_json("GET", "/state")
    _ = request_json("POST", "/reset", {"episode_id": "spec-compliance", "seed": 2})
    _ = request_json(
        "POST",
        "/step",
        {
            "action": {
                "candidate_pipeline": "def run_pipeline(x):\n    return []",
                "metadata": {"difficulty": "easy", "task_id": "easy_csv_null_type"},
            }
        },
    )
    ok("OpenEnv spec compliance", "manifest, typed models, and endpoints validated")


def check_dockerfile_build() -> None:
    dockerfile = ENV_DIR / "Dockerfile"
    if not dockerfile.exists():
        fail("Dockerfile builds", "missing envs/data_pipeline_debug_env/Dockerfile")
    # Only run build if Docker daemon is reachable.
    info = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        encoding="utf-8",
        errors="ignore",
    )
    if info.returncode != 0:
        fail("Dockerfile builds", "docker daemon unavailable")
    build = subprocess.run(
        [
            "docker",
            "build",
            "-t",
            "data-pipeline-debug-openenv:precheck",
            "-f",
            str(dockerfile),
            str(ENV_DIR),
        ],
        capture_output=True,
        encoding="utf-8",
        errors="ignore",
    )
    if build.returncode != 0:
        fail("Dockerfile builds", build.stderr[-400:])
    ok("Dockerfile builds", "local docker build succeeded")


def check_inference_script() -> None:
    inference = ROOT / "inference.py"
    if not inference.exists():
        fail("Baseline reproduces", "missing root inference.py")
    text = inference.read_text(encoding="utf-8")
    required_tokens = [
        "API_BASE_URL",
        "MODEL_NAME",
        "OPENAI_API_KEY",
        "HF_TOKEN",
        "LOCAL_IMAGE_NAME",
        "OpenAI(",
        "[START]",
        "[STEP]",
        "[END]",
        "task=",
        "env=",
        "model=",
        "action=",
        "reward=",
        "done=",
        "error=",
        "success=",
        "steps=",
        "score=",
        "rewards=",
    ]
    for required in required_tokens:
        if required not in text:
            fail("Baseline reproduces", f"inference.py missing token: {required}")
    ok("Baseline reproduces", "inference.py present with required env vars and log markers")


def check_tasks_with_graders() -> None:
    # Known deterministic tasks in this environment.
    task_ids = [
        ("easy", "easy_csv_null_type"),
        ("medium", "medium_schema_drift"),
        ("hard", "hard_dependency_chain"),
    ]
    for difficulty, task_id in task_ids:
        reset = request_json(
            "POST",
            "/reset",
            {"difficulty": difficulty, "task_id": task_id, "episode_id": f"grader-{task_id}"},
        )
        obs = reset.get("observation", {})
        broken = obs.get("broken_pipeline", "")
        step = request_json(
            "POST",
            "/step",
            {
                "action": {
                    "candidate_pipeline": broken,
                    "metadata": {"difficulty": difficulty, "task_id": task_id},
                }
            },
        )
        reward = float(step.get("reward", 0.0) or 0.0)
        if not (0.0 < reward < 1.0):
            fail("3+ tasks with graders", f"{task_id} reward must be strictly between 0 and 1: {reward}")
    ok("3+ tasks with graders", "enumerated all tasks, reward range is strictly (0.0, 1.0)")


def main() -> None:
    checks = [
        check_space_ping_and_reset,
        check_spec_compliance,
        check_dockerfile_build,
        check_inference_script,
        check_tasks_with_graders,
    ]
    for check in checks:
        check()
    print("[PASS] All pre-submission checks passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[FAIL] {exc}")
        sys.exit(1)
