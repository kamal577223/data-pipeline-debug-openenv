# Data Pipeline Debug OpenEnv

Submission-ready OpenEnv environment that simulates a real data-team workflow:
debugging broken ETL pipelines under schema drift, null/type errors, and stage dependency chains.

## Why this is useful

This is not a toy game. It models a daily engineering task in analytics and platform teams:

- inspect broken pipeline code
- submit a repaired implementation
- get deterministic grader feedback on schema, types, values, and safety

## Task set (easy -> hard)

- `easy_csv_null_type`: clean CSV-like rows with nulls/blanks and strict types
- `medium_schema_drift`: repair multi-step payments aggregation under schema/status drift
- `hard_dependency_chain`: fix staged ETL where upstream changes can break downstream stages

## Scoring model

Each step returns dense reward with transparent components:

- schema score: 35%
- type score: 25%
- value score: 40%
- penalties: unsafe patterns + runtime failures

To satisfy strict phase validators, task scores are enforced in the open interval `(0, 1)`.

## Repo layout

- environment package: `envs/data_pipeline_debug_env`
- API server: `envs/data_pipeline_debug_env/server`
- baseline inference: `inference.py`
- strict pre-submission validator: `pre_validation.py`
- local reference baseline runner: `scripts/run_reference_baseline.py`
- tests: `tests/test_data_pipeline_debug_env.py`

## Quick run

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
python scripts/run_reference_baseline.py
python pre_validation.py
```

## Inference requirements (submission)

Environment variables expected by `inference.py`:

- `API_BASE_URL`
- `MODEL_NAME`
- `OPENAI_API_KEY` (preferred) or `HF_TOKEN`
- `LOCAL_IMAGE_NAME` (for local image workflow compatibility)

The script emits strict `[START]`, `[STEP]`, `[END]` logs and runs all 3 tasks.

## Deployment

Detailed deployment + resubmission checklist is in `deployment.md`.
