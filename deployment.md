# Deployment and Resubmission Checklist

## 1. Sync code

```bash
git pull
```

## 2. Run local quality checks

```bash
python -m unittest tests.test_data_pipeline_debug_env -v
python scripts/run_reference_baseline.py
python pre_validation.py
```

Expected:

- all tests pass
- baseline report is generated at `outputs/evals/baseline_report.json`
- pre-validation passes all checks

## 3. Push to GitHub

```bash
git add .
git commit -m "Submission improvements"
git push origin main
```

## 4. Update HF Space files

At minimum these files must match latest code:

- `server/app.py`
- `server/data_pipeline_debug_environment.py`
- `openenv.yaml`
- `Dockerfile`
- `inference.py` (if your Space repo mirrors submission root)
- `uv.lock`

## 5. Confirm runtime health

Run:

```bash
curl -X POST https://kamal2525-data-pipeline-debug-openenv.hf.space/reset -H "Content-Type: application/json" -d "{}"
```

Must return HTTP 200 and valid JSON.

## 6. Final resubmission checks

- `openenv validate` style checks pass
- scores are strictly between 0 and 1 for each task
- `inference.py` exists at repo root
- Docker build succeeds

Then resubmit from Scaler dashboard.
