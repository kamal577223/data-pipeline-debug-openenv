---
title: Data Pipeline Debug Environment
emoji: 🔧
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
app_port: 8000
base_path: /web
tags:
  - openenv
  - data-pipeline
  - debugging
  - evaluation
---

# Data Pipeline Debug Environment

An OpenEnv-style environment for debugging broken ETL/data pipelines with deterministic grading.

The package mirrors the canonical OpenEnv environment layout used by [`envs/reasoning_gym_env`](https://github.com/meta-pytorch/OpenEnv/tree/main/envs/reasoning_gym_env), but replaces single-step reasoning questions with executable pipeline repair tasks.

## What It Simulates

The agent receives a broken data pipeline and must submit repaired Python code. The environment executes the candidate pipeline against fixed sample data and grades it deterministically against:

- expected output values
- expected output schema
- required value types
- task-specific invariants such as row counts and tiering rules

## Difficulty Ladder

- `easy`: CSV cleanup with null, blank, and type conversion issues
- `medium`: Multi-step pipeline with schema drift and aggregation bugs
- `hard`: Dependency-chain ETL where stage compatibility matters

## Episode Format

Each episode supports up to 3 repair attempts:

1. `reset()` returns the task prompt, broken pipeline, and expected contract.
2. `step()` accepts a `DataPipelineDebugAction(candidate_pipeline=...)`.
3. The environment executes the repaired code and returns dense reward + score.
4. Episode ends when task passes or attempts are exhausted.

## Action / Observation / Reward Space

- `Action`: `candidate_pipeline` (required), optional `metadata` containing `difficulty` and `task_id`.
- `Observation`:
  - core task fields: `task_id`, `difficulty`, `prompt`, `broken_pipeline`, `expected_contract`
  - trajectory fields: `done`, `reward`, `score`, `attempts_remaining`, `feedback`, `passed`
  - grader transparency: `reward_breakdown`
- `Reward` (typed in code as `DataPipelineDebugReward`):
  - bounded in `[0.0, 1.0]`
  - deterministic weighted sum of schema/type/value quality
  - explicit penalties for unsafe patterns and runtime/compile failures

## Reward Function

Each step computes a dense reward:

- positive signal:
  - `schema_score` (35%)
  - `type_score` (25%)
  - `value_score` (40%)
- penalties:
  - `safety_penalty` for clearly undesirable patterns (`subprocess`, unsafe I/O, etc.)
  - `runtime_penalty` for compile/runtime failures

Final step reward is clamped to `[0.0, 1.0]`.

## Quick Start

```python
from data_pipeline_debug_env import DataPipelineDebugAction
from data_pipeline_debug_env.server.data_pipeline_debug_environment import DataPipelineDebugEnvironment

env = DataPipelineDebugEnvironment()

observation = env.reset(difficulty="easy")
print(observation.prompt)
print(observation.broken_pipeline)

result = env.step(
    DataPipelineDebugAction(
        candidate_pipeline=\"\"\"
def run_pipeline(rows):
    cleaned = []
    for row in rows:
        raw_name = row.get("name")
        if raw_name is None:
            continue
        age = int(row["age"]) if str(row.get("age", "")).strip() else 0
        salary_raw = str(row.get("salary", "")).strip()
        salary = float(salary_raw) if salary_raw else 0.0
        cleaned.append(
            {
                "id": int(row["id"]),
                "name": raw_name.strip().title(),
                "age": age,
                "salary": salary,
            }
        )
    return cleaned
\"\"\".strip()
    )
)

print(result.passed)
print(result.feedback)
```

## Building the Docker Image

```bash
docker build -t data-pipeline-debug-env:latest -f server/Dockerfile .
```

## Deploying to Hugging Face Spaces

From this environment directory:

```bash
openenv push
```

Optional examples:

```bash
openenv push --repo-id your-name/data-pipeline-debug-env
openenv push --private
```

Prerequisites:

- Docker Desktop installed and running
- OpenEnv installed locally
- Hugging Face login/token configured

## Development Phases

This environment was built and validated in gated phases:

1. `Phase 1`: Scaffold the canonical OpenEnv package shape and verify structure/import sanity.
2. `Phase 2`: Implement executable easy/medium/hard tasks and deterministic grading.
3. `Phase 3`: Finalize packaging, server wiring, and submission-style checks.

Proceed to the next phase only after the previous one passes its local checks.

## Submission Checks

Run these checks from the repository root:

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
python -m compileall envs/data_pipeline_debug_env tests/test_data_pipeline_debug_env.py
python pre_validation.py
```

## Baseline Inference

Root script: `inference.py`

- uses OpenAI client for all model calls
- reads:
  - `API_BASE_URL`
  - `MODEL_NAME`
  - `OPENAI_API_KEY` (preferred) or `HF_TOKEN` (fallback)
  - `LOCAL_IMAGE_NAME` (for local image workflow compatibility)
- emits strict log lines:
  - `[START] ...`
  - `[STEP] ...`
  - `[END] ...`

Example run:

```bash
python inference.py
```

Reference baseline (deterministic local reference solver from tests):

- easy: `1.00`
- medium: `1.00`
- hard: `1.00`
- mean: `1.00`
```

## Project Structure

```text
envs/data_pipeline_debug_env/
├── .dockerignore
├── __init__.py
├── README.md
├── client.py
├── models.py
├── openenv.yaml
├── pyproject.toml
└── server/
    ├── __init__.py
    ├── app.py
    ├── data_pipeline_debug_environment.py
    ├── Dockerfile
    └── requirements.txt
```
