from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "envs"))

from data_pipeline_debug_env.models import DataPipelineDebugAction
from data_pipeline_debug_env.server.data_pipeline_debug_environment import DataPipelineDebugEnvironment


def test_environment_scaffold_exists():
    root = Path(__file__).resolve().parents[1] / "envs" / "data_pipeline_debug_env"
    expected = [
        root / "__init__.py",
        root / "client.py",
        root / "models.py",
        root / "openenv.yaml",
        root / "pyproject.toml",
        root / "server" / "app.py",
        root / "server" / "data_pipeline_debug_environment.py",
        root / "server" / "Dockerfile",
    ]
    missing = [str(path) for path in expected if not path.exists()]
    assert not missing, f"Missing expected files: {missing}"


class EnvironmentBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.env = DataPipelineDebugEnvironment()

    def test_easy_task_passes_with_valid_fix(self):
        self.env.reset(difficulty="easy")
        result = self.env.step(
            DataPipelineDebugAction(
                candidate_pipeline="""
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
""".strip()
            )
        )
        self.assertTrue(result.passed)
        self.assertEqual(result.reward, 1.0)
        self.assertTrue(result.done)
        self.assertEqual(result.score, 1.0)
        self.assertEqual(result.attempts_remaining, 2)

    def test_medium_task_fails_with_broken_submission(self):
        self.env.reset(difficulty="medium")
        result = self.env.step(
            DataPipelineDebugAction(
                candidate_pipeline="""
def run_pipeline(payload):
    return []
""".strip()
            )
        )
        self.assertFalse(result.passed)
        self.assertGreaterEqual(result.reward, 0.0)
        self.assertLessEqual(result.reward, 1.0)
        self.assertFalse(result.done)
        self.assertEqual(result.attempts_remaining, 2)
        self.assertIn("Output values do not match expected results.", result.feedback)

    def test_hard_task_passes_with_dependency_safe_fix(self):
        self.env.reset(difficulty="hard")
        result = self.env.step(
            DataPipelineDebugAction(
                candidate_pipeline="""
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
        net = round(gross * (1.0 - (order["discount_pct"] / 100.0)), 2)
        transformed.append(
            {
                "order_id": order["order_id"],
                "customer": order["customer"],
                "revenue": net,
            }
        )
    return transformed

def stage_enrich(orders):
    enriched = []
    for order in orders:
        enriched.append(
            {
                "order_id": order["order_id"],
                "customer": order["customer"],
                "revenue": float(order["revenue"]),
                "tier": "gold" if order["revenue"] >= 100.0 else "standard",
            }
        )
    return enriched

def run_pipeline(raw_orders):
    return stage_enrich(stage_transform(stage_extract(raw_orders)))
""".strip()
            )
        )
        self.assertTrue(result.passed)
        self.assertEqual(result.reward, 1.0)
        self.assertEqual(result.score, 1.0)
        self.assertTrue(result.done)

    def test_episode_ends_after_max_attempts(self):
        self.env.reset(difficulty="easy")
        bad = DataPipelineDebugAction(candidate_pipeline="def run_pipeline(rows):\n    return []")

        one = self.env.step(bad)
        two = self.env.step(bad)
        three = self.env.step(bad)

        self.assertFalse(one.done)
        self.assertFalse(two.done)
        self.assertTrue(three.done)
        self.assertEqual(three.attempts_remaining, 0)
        self.assertGreaterEqual(three.score, 0.0)
        self.assertLessEqual(three.score, 1.0)

    def test_safety_penalty_is_applied(self):
        self.env.reset(difficulty="easy")
        result = self.env.step(
            DataPipelineDebugAction(
                candidate_pipeline="""
import os
def run_pipeline(rows):
    return []
""".strip()
            )
        )
        self.assertIn("safety_penalty", result.reward_breakdown)
        self.assertGreater(result.reward_breakdown["safety_penalty"], 0.0)

    def test_default_reset_cycles_through_tasks(self):
        first = self.env.reset()
        second = self.env.reset()
        third = self.env.reset()
        self.assertEqual(
            [first.task_id, second.task_id, third.task_id],
            ["easy_csv_null_type", "medium_schema_drift", "hard_dependency_chain"],
        )


class MetadataTests(unittest.TestCase):
    def test_openenv_manifest_uses_expected_shape(self):
        manifest_path = ROOT / "envs" / "data_pipeline_debug_env" / "openenv.yaml"
        manifest = manifest_path.read_text()
        self.assertIn("spec_version: 1", manifest)
        self.assertIn("name: data_pipeline_debug_env", manifest)
        self.assertIn("type: space", manifest)
        self.assertIn("runtime: fastapi", manifest)
        self.assertIn("app: server.app:app", manifest)
        self.assertIn("port: 8000", manifest)


if __name__ == "__main__":
    unittest.main()
