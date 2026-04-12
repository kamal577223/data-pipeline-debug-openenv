# Pipesynx: Data Pipeline Debug OpenEnv

| key | value |
|---|---|
| title | Pipesynx |
| emoji | 🔧 |
| colorFrom | blue |
| colorTo | green |
| sdk | docker |
| app_port | 8000 |

**Pipesynx** is a real-world OpenEnv environment for diagnosing and repairing broken ETL/data pipelines.  
An agent receives broken Python pipeline logic, submits repaired code, and is graded deterministically with dense reward signals for schema, type, and value correctness.

---

## Important Links

- GitHub (this project): [kamal577223/data-pipeline-debug-openenv](https://github.com/kamal577223/data-pipeline-debug-openenv)
- Hugging Face Space page: [KAMAL2525/data-pipeline-debug-openenv](https://huggingface.co/spaces/KAMAL2525/data-pipeline-debug-openenv)
- Hugging Face runtime URL: [kamal2525-data-pipeline-debug-openenv.hf.space](https://kamal2525-data-pipeline-debug-openenv.hf.space)

---

## Why Pipesynx

Production data pipelines fail in chained ways: one fix can expose the next fault. Pipesynx simulates this workflow with deterministic tasks and executable grading.

It focuses on:
- schema drift handling
- null/blank/type coercion
- dependency-safe multi-stage transformations
- anti-overfit evaluation through hidden eval cases

---

## Environment API

Pipesynx exposes standard OpenEnv-style endpoints:

- `POST /reset` → initial observation for selected task
- `POST /step` → returns `observation`, `reward`, `done`
- `GET /state` → current episode state
- `GET /schema` → action/observation/state schemas
- `GET /health` → runtime health
- `GET /metadata` → environment metadata summary

Manifest: `envs/data_pipeline_debug_env/openenv.yaml`

---

## Project Layout (Current)

```text
.
├── envs/data_pipeline_debug_env/
│   ├── __init__.py
│   ├── client.py
│   ├── models.py
│   ├── openenv.yaml
│   ├── pyproject.toml
│   ├── README.md
│   └── server/
│       ├── __init__.py
│       ├── app.py
│       ├── data_pipeline_debug_environment.py
│       ├── Dockerfile
│       └── requirements.txt
├── inference.py
├── pre_validation.py
├── scripts/
│   ├── run_reference_baseline.py
│   └── validate-submission.sh
├── outputs/evals/baseline_report.json
└── tests/test_data_pipeline_debug_env.py
```

---

## Project Layout (Planned Enhanced Structure)

```text
Pipesynx/
├── envs/data_pipeline_debug_env/              # Core OpenEnv package
│   ├── server/                                # API + environment runtime
│   ├── models.py                              # Typed action/observation/state/reward
│   └── client.py                              # Typed client
├── app/                                       # (Planned) web/demo UX modules
│   ├── dashboard/                             # leaderboard + run explorer
│   ├── trace_viewer/                          # step-by-step pipeline diff view
│   └── contracts/                             # contract templates and validators
├── baselines/                                 # (Planned) heuristic and LLM baselines
├── training/                                  # (Planned) offline policy training scaffold
├── outputs/
│   ├── evals/                                 # benchmark and validation artifacts
│   └── runs/                                  # (Planned) episode transcripts
├── scripts/                                   # validation and utility scripts
├── tests/                                     # unit + integration checks
└── deployment.md                              # deployment guidance
```

---

## Action Space

`DataPipelineDebugAction`:
- `candidate_pipeline: str` (required)
- optional `metadata` may include task selectors (`difficulty`, `task_id`)

---

## Observation Space

`DataPipelineDebugObservation` includes:
- task context: `task_id`, `difficulty`, `prompt`, `broken_pipeline`, `expected_contract`
- trajectory: `last_submission`, `feedback`, `passed`, `attempts_remaining`
- grading: `reward`, `score`, `reward_breakdown`, `done`

---

## State Space

`DataPipelineDebugState` tracks:
- `episode_id`, `step_count`
- `task_id`, `difficulty`
- `completed`
- `max_attempts`, `attempts_remaining`
- `best_score`

---

## Tasks

1. **Easy — `easy_csv_null_type`**  
   Null/blank cleaning and type normalization for CSV-like rows.

2. **Medium — `medium_schema_drift`**  
   Multi-step payments pipeline with schema/status drift and string amounts.

3. **Hard — `hard_dependency_chain`**  
   Staged orders ETL requiring compatible extraction, transformation, and enrichment.

Each task uses:
- one public train case for transparent feedback
- hidden eval case(s) to reduce overfitting

---

## Reward Design

Dense deterministic scoring per step:
- schema score: `35%`
- type score: `25%`
- value score: `40%`
- penalties: safety + runtime/compile failures

Difficulty-calibrated score bands:
- easy: `(0.01, 0.95)`
- medium: `(0.02, 0.97)`
- hard: `(0.03, 0.99)`

This preserves strict validation compatibility (`0 < score < 1`) while differentiating difficulty levels.

---

## Setup

```bash
python -m venv .venv
# Linux/Mac:
source .venv/bin/activate
# Windows PowerShell:
# .venv\Scripts\Activate.ps1

pip install -r envs/data_pipeline_debug_env/server/requirements.txt
```

---

## Local Validation

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
python scripts/run_reference_baseline.py
python pre_validation.py
```

---

## Docker

> Note: The Docker image tag can be any name. Using the original tag keeps commands familiar and avoids confusion for existing users.

```bash
docker build -t data-pipeline-debug-openenv -f envs/data_pipeline_debug_env/Dockerfile envs/data_pipeline_debug_env
docker run -p 8000:8000 data-pipeline-debug-openenv
```

---

## Baseline Inference (Submission)

Submission script: `inference.py`

Required environment variables:
- `API_BASE_URL`
- `MODEL_NAME`
- `OPENAI_API_KEY` (preferred) or `HF_TOKEN`
- `LOCAL_IMAGE_NAME` (included for local-image workflow compatibility)

Expected log format:

```text
[START] task=<task_name> env=<benchmark> model=<model_name>
[STEP] step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
[END] success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>
```

---

## Reference Baseline Scores

From `outputs/evals/baseline_report.json`:
- easy: `0.95`
- medium: `0.97`
- hard: `0.99`
- average: `0.97`

---

## Testing Coverage

Current tests include:
- scaffold/file-shape checks
- deterministic pass/fail behavior per difficulty
- anti-overfit hidden eval check
- safety penalty behavior
- attempt-limit and post-episode protections
- manifest shape checks

Run:

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
```

---

## Deployment

Detailed deployment and final resubmission checklist:
- [deployment.md](deployment.md)

---

## Submission Readiness Checklist

- HF Space `/reset` responds with `200`
- `openenv.yaml` present and valid
- root `inference.py` present
- Docker image builds
- 3 deterministic tasks available
- scores strictly bounded in `(0, 1)`
- baseline and pre-validation scripts run successfully

---

## Suggested Next Improvements

- Add OpenEnv runtime validation in CI
- Expand hidden eval pools per task for stronger generalization testing
- Add run transcript artifacts for demo-day storytelling
- Add a visual pipeline trace UI (before/after row and schema diff)
- Add robustness leaderboard (public vs hidden score gap)
