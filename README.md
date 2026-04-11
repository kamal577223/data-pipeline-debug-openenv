# Data Pipeline Debug OpenEnv

Data Pipeline Debug OpenEnv is a real-world environment for evaluating whether an agent can diagnose and repair broken ETL/data pipelines. Instead of solving toy tasks, the agent must fix executable Python pipeline logic under schema drift, null/type issues, and dependency-chain failures.

## Important Links

- GitHub (this project): [kamal577223/data-pipeline-debug-openenv](https://github.com/kamal577223/data-pipeline-debug-openenv)
- Hugging Face Space page: [KAMAL2525/data-pipeline-debug-openenv](https://huggingface.co/spaces/KAMAL2525/data-pipeline-debug-openenv)
- Hugging Face runtime URL: [kamal2525-data-pipeline-debug-openenv.hf.space](https://kamal2525-data-pipeline-debug-openenv.hf.space)
- OpenEnv reference format used: [meta-pytorch/OpenEnv/envs/reasoning_gym_env](https://github.com/meta-pytorch/OpenEnv/tree/main/envs/reasoning_gym_env)
- Comparison reference repos:
- [AKJenaX/Bank_Shield](https://github.com/AKJenaX/Bank_Shield)
- [Adityabaskati-weeb/METAMINDS](https://github.com/Adityabaskati-weeb/METAMINDS)

## Why This Environment

Data teams repeatedly face production ETL breakages where one bug fix can expose or create another issue downstream. This environment mirrors that workflow:

- broken pipeline code is provided as part of the observation
- agent submits corrected Python code
- deterministic grader executes code and scores correctness and safety
- reward is dense and transparent, not only terminal pass/fail

## OpenEnv Interface

The environment exposes standard OpenEnv-style endpoints:

- `POST /reset` -> initial observation for selected task/difficulty
- `POST /step` -> returns `observation`, `reward`, `done`
- `GET /state` -> current episode state
- `GET /schema` -> action/observation/state schemas
- `GET /health` -> runtime health
- `GET /metadata` -> environment metadata summary

Manifest file: `envs/data_pipeline_debug_env/openenv.yaml`

## Project Layout

- `envs/data_pipeline_debug_env/models.py` -> typed Action/Observation/State/Reward models
- `envs/data_pipeline_debug_env/server/data_pipeline_debug_environment.py` -> environment logic and deterministic grading
- `envs/data_pipeline_debug_env/server/app.py` -> FastAPI/OpenEnv server
- `envs/data_pipeline_debug_env/server/requirements.txt` -> runtime dependencies
- `inference.py` -> submission baseline script with strict stdout format
- `pre_validation.py` -> submission pre-check script
- `scripts/run_reference_baseline.py` -> reproducible local baseline report generator
- `outputs/evals/baseline_report.json` -> latest reference baseline artifact
- `tests/test_data_pipeline_debug_env.py` -> tests

## Action Space

`DataPipelineDebugAction`:

- `candidate_pipeline: str` (required)
- optional `metadata` may include `difficulty` and `task_id`

## Observation Space

`DataPipelineDebugObservation` includes:

- task context: `task_id`, `difficulty`, `prompt`, `broken_pipeline`, `expected_contract`
- trajectory feedback: `last_submission`, `feedback`, `passed`, `attempts_remaining`
- scoring: `reward`, `score`, `reward_breakdown`, `done`

## State Space

`DataPipelineDebugState` tracks:

- `episode_id`, `step_count`
- `task_id`, `difficulty`
- `completed`
- `max_attempts`, `attempts_remaining`
- `best_score`

## Tasks (Easy -> Medium -> Hard)

1. `easy_csv_null_type`
- Fix nulls/blanks/type coercion in CSV-style row cleaning.

2. `medium_schema_drift`
- Repair multi-step customer payments pipeline with schema/status drift and string amounts.

3. `hard_dependency_chain`
- Fix staged orders ETL where upstream transformation compatibility affects downstream enrichment.

## Reward Design

Dense deterministic reward each step:

- schema score: `35%`
- type score: `25%`
- value score: `40%`
- penalties: unsafe code patterns + runtime/compile failures

To satisfy strict phase validation, reward/score values are kept in open interval `(0, 1)` and not equal to `0` or `1`.

## Setup

```bash
python -m venv .venv
# Linux/Mac:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1

pip install -r envs/data_pipeline_debug_env/server/requirements.txt
```

## Local Validation

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
python scripts/run_reference_baseline.py
python pre_validation.py
```

## Docker

```bash
docker build -t data-pipeline-debug-openenv -f envs/data_pipeline_debug_env/Dockerfile envs/data_pipeline_debug_env
docker run -p 8000:8000 data-pipeline-debug-openenv
```

## Baseline Inference (Submission)

Submission script: `inference.py`

Required environment variables:

- `API_BASE_URL`
- `MODEL_NAME`
- `OPENAI_API_KEY` (preferred) or `HF_TOKEN`
- `LOCAL_IMAGE_NAME` (included for local-image workflow compatibility)

The script emits strict logs:

```text
[START] task=<task_name> env=<benchmark> model=<model_name>
[STEP] step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
[END] success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>
```

## Reference Baseline Scores

From `outputs/evals/baseline_report.json`:

- easy: `0.99`
- medium: `0.99`
- hard: `0.99`
- average: `0.99`

## Deployment

Detailed deployment and final resubmission checklist:

- [deployment.md](deployment.md)

## Submission Readiness Checklist

- HF Space `/reset` responds with `200`
- `openenv.yaml` present and valid
- root `inference.py` present
- `Dockerfile` builds
- 3 tasks available with deterministic graders
- task scores strictly between `0` and `1`
- baseline and pre-validation run successfully
