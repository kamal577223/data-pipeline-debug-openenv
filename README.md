# Data Pipeline Debug Environment for OpenEnv

This repository contains a submission-ready OpenEnv environment for hackathon use:

- main environment package: [envs/data_pipeline_debug_env](C:\Users\kamal\Downloads\VS_WEB\Data Pipeline_CODEX\envs\data_pipeline_debug_env)
- local tests: [tests/test_data_pipeline_debug_env.py](C:\Users\kamal\Downloads\VS_WEB\Data Pipeline_CODEX\tests\test_data_pipeline_debug_env.py)

The environment is modeled after Meta's OpenEnv [`reasoning_gym_env`](https://github.com/meta-pytorch/OpenEnv/tree/main/envs/reasoning_gym_env), but the task family is developer-focused data pipeline debugging.

## What This Repo Implements

Agents receive a broken ETL/data pipeline and must repair it by submitting working Python code. The environment grades deterministically by executing the submitted code on fixed inputs and checking:

- output schema
- output types
- output values
- task-specific invariants

## Difficulty Progression

- `easy`: CSV cleanup with nulls and type issues
- `medium`: multi-step pipeline with schema drift
- `hard`: staged ETL where fixing one stage can break another

## Submission Directory

For OpenEnv-style submission, the main deliverable is the environment directory:

```text
envs/data_pipeline_debug_env
```

## Local Validation

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
python -m compileall envs/data_pipeline_debug_env tests/test_data_pipeline_debug_env.py
```

## What You Still Need Before Deployment

- Docker Desktop installed and running
- OpenEnv available locally
- a Hugging Face token with permission to create/push Spaces

Once those are available, you can deploy from the environment directory with `openenv push`.
