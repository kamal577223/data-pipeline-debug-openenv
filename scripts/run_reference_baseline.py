"""Deterministic local baseline for data_pipeline_debug_env.

Runs one known-correct submission per task and writes reproducible scores.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "envs"))

from data_pipeline_debug_env.models import DataPipelineDebugAction
from data_pipeline_debug_env.server.data_pipeline_debug_environment import DataPipelineDebugEnvironment

OUT = ROOT / "outputs" / "evals"
OUT.mkdir(parents=True, exist_ok=True)


SOLVERS = {
    "easy": """
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
""".strip(),
    "medium": """
def run_pipeline(payload):
    customers = []
    for row in payload["customers"]:
        customers.append({"customer_id": row["customerId"], "name": row["name"].title()})
    grouped = {}
    for p in payload["payments"]:
        cid = p["customer_id"]
        grouped.setdefault(cid, {"total_paid": 0.0, "payment_count": 0})
        if str(p["status"]).strip().lower() == "paid":
            grouped[cid]["total_paid"] += float(p["amount"])
            grouped[cid]["payment_count"] += 1
    result = []
    for c in customers:
        stats = grouped.get(c["customer_id"], {"total_paid": 0.0, "payment_count": 0})
        result.append(
            {
                "customer_id": c["customer_id"],
                "name": c["name"],
                "total_paid": round(stats["total_paid"], 2),
                "payment_count": int(stats["payment_count"]),
            }
        )
    return sorted(result, key=lambda x: x["customer_id"])
""".strip(),
    "hard": """
def stage_extract(raw_orders):
    extracted = []
    for row in raw_orders:
        extracted.append(
            {
                "order_id": row["order_id"],
                "customer": row["customer"],
                "discount_pct": float(row.get("discount_pct") or 0.0),
                "items": row["items"],
            }
        )
    return extracted

def stage_transform(orders):
    transformed = []
    for order in orders:
        gross = 0.0
        for item in order["items"]:
            gross += float(item["qty"]) * float(item["unit_price"])
        net = round(gross * (1.0 - order["discount_pct"] / 100.0), 2)
        transformed.append({"order_id": order["order_id"], "customer": order["customer"], "revenue": net})
    return transformed

def stage_enrich(rows):
    enriched = []
    for row in rows:
        enriched.append(
            {
                "order_id": row["order_id"],
                "customer": row["customer"],
                "revenue": float(row["revenue"]),
                "tier": "gold" if row["revenue"] >= 100.0 else "standard",
            }
        )
    return enriched

def run_pipeline(raw_orders):
    return stage_enrich(stage_transform(stage_extract(raw_orders)))
""".strip(),
}


def main() -> None:
    env = DataPipelineDebugEnvironment()
    by_task = {}
    for difficulty in ("easy", "medium", "hard"):
        env.reset(difficulty=difficulty)
        obs = env.step(DataPipelineDebugAction(candidate_pipeline=SOLVERS[difficulty]))
        by_task[difficulty] = {
            "task_id": obs.task_id,
            "score": obs.score,
            "reward": obs.reward,
            "passed": obs.passed,
        }

    average = sum(item["score"] for item in by_task.values()) / 3.0
    report = {"benchmark": "data_pipeline_debug_env", "by_task": by_task, "average_score": round(average, 4)}
    out_file = OUT / "baseline_report.json"
    out_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nWrote: {out_file}")


if __name__ == "__main__":
    main()
