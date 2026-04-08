"""Core environment logic for the data pipeline debugging environment."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
import math
from textwrap import dedent
from typing import Any, Generic, TypeVar
import json
import traceback

from pydantic import ValidationError

try:
    from openenv.core.env_server.interfaces import Environment
except ImportError:
    ActT = TypeVar("ActT")
    ObsT = TypeVar("ObsT")
    StateT = TypeVar("StateT")

    class Environment(Generic[ActT, ObsT, StateT]):
        """Minimal fallback for local development without openenv."""

try:
    from ..models import DataPipelineDebugAction, DataPipelineDebugObservation, DataPipelineDebugState
except ImportError:
    from models import DataPipelineDebugAction, DataPipelineDebugObservation, DataPipelineDebugState


SafeBuiltins = {
    "all": all,
    "any": any,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "isinstance": isinstance,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "zip": zip,
}


@dataclass(frozen=True)
class PipelineTask:
    task_id: str
    difficulty: str
    prompt: str
    broken_pipeline: str
    input_data: Any
    entrypoint: str
    expected_output: Any
    expected_contract: dict[str, Any]


def _normalize_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def _build_tasks() -> dict[str, PipelineTask]:
    easy_broken = dedent(
        """
        def run_pipeline(rows):
            cleaned = []
            for row in rows:
                cleaned.append(
                    {
                        "id": row["id"],
                        "name": row["name"].strip().title(),
                        "age": int(row["age"]),
                        "salary": int(row["salary"]),
                    }
                )
            return cleaned
        """
    ).strip()

    medium_broken = dedent(
        """
        def normalize_customers(customers):
            normalized = []
            for row in customers:
                normalized.append(
                    {
                        "customer_id": row["customerId"],
                        "name": row["name"].title(),
                    }
                )
            return normalized

        def summarize_payments(payments):
            totals = {}
            for row in payments:
                customer_id = row["customer_id"]
                totals.setdefault(customer_id, 0)
                if row["status"] == "paid":
                    totals[customer_id] += row["amount"]
            return totals

        def run_pipeline(payload):
            customers = normalize_customers(payload["customers"])
            totals = summarize_payments(payload["payments"])
            result = []
            for customer in customers:
                result.append(
                    {
                        "customer_id": customer["customer_id"],
                        "name": customer["name"],
                        "total_paid": totals[customer["customer_id"]],
                        "payment_count": 1,
                    }
                )
            return result
        """
    ).strip()

    hard_broken = dedent(
        """
        def stage_extract(raw_orders):
            extracted = []
            for row in raw_orders:
                extracted.append(
                    {
                        "order_id": row["order_id"],
                        "customer": row["customer"],
                        "items": row["items"],
                        "discount_pct": int(row.get("discount_pct", 0)),
                    }
                )
            return extracted

        def stage_transform(orders):
            transformed = []
            for order in orders:
                gross = 0
                for item in order["items"]:
                    gross += item["qty"] * item["unit_price"]
                transformed.append(
                    {
                        "order_id": order["order_id"],
                        "customer": order["customer"],
                        "gross": gross,
                        "net": gross - order["discount_pct"],
                    }
                )
            return transformed

        def stage_enrich(orders):
            enriched = []
            for order in orders:
                tier = "gold" if order["net"] > 100 else "standard"
                enriched.append(
                    {
                        "order_id": order["order_id"],
                        "customer": order["customer"],
                        "revenue": round(order["net"] / 100, 2),
                        "tier": tier,
                    }
                )
            return enriched

        def run_pipeline(raw_orders):
            extracted = stage_extract(raw_orders)
            transformed = stage_transform(extracted)
            return stage_enrich(transformed)
        """
    ).strip()

    return {
        "easy_csv_null_type": PipelineTask(
            task_id="easy_csv_null_type",
            difficulty="easy",
            prompt=(
                "Repair the CSV cleaning pipeline. The fixed code must define "
                "`run_pipeline(rows)` and return rows with schema "
                "`id:int, name:str, age:int, salary:float`. Skip rows where `name` is null, "
                "fill missing `age` with 0, and fill missing/blank `salary` with 0.0."
            ),
            broken_pipeline=easy_broken,
            input_data=[
                {"id": "1", "name": " alice ", "age": "31", "salary": "120000.50"},
                {"id": "2", "name": "bob", "age": "", "salary": ""},
                {"id": "3", "name": None, "age": "22", "salary": "81000"},
            ],
            entrypoint="run_pipeline",
            expected_output=[
                {"id": 1, "name": "Alice", "age": 31, "salary": 120000.5},
                {"id": 2, "name": "Bob", "age": 0, "salary": 0.0},
            ],
            expected_contract={
                "schema": {
                    "id": "int",
                    "name": "str",
                    "age": "int",
                    "salary": "float",
                },
                "row_count": 2,
            },
        ),
        "medium_schema_drift": PipelineTask(
            task_id="medium_schema_drift",
            difficulty="medium",
            prompt=(
                "Repair the multi-step customer payments pipeline. The fixed code must define "
                "`run_pipeline(payload)` and handle schema drift across `customerId` / `customer_id`, "
                "mixed-case payment statuses, and string amounts. Return one row per customer with "
                "`customer_id`, `name`, `total_paid`, and `payment_count`."
            ),
            broken_pipeline=medium_broken,
            input_data={
                "customers": [
                    {"customerId": "c1", "name": "acme corp"},
                    {"customerId": "c2", "name": "beta llc"},
                ],
                "payments": [
                    {"customer_id": "c1", "amount": "19.50", "status": "PAID"},
                    {"customer_id": "c1", "amount": "5.50", "status": "paid"},
                    {"customer_id": "c2", "amount": "9", "status": "failed"},
                    {"customer_id": "c2", "amount": "3", "status": "Paid"},
                ],
            },
            entrypoint="run_pipeline",
            expected_output=[
                {"customer_id": "c1", "name": "Acme Corp", "total_paid": 25.0, "payment_count": 2},
                {"customer_id": "c2", "name": "Beta Llc", "total_paid": 3.0, "payment_count": 1},
            ],
            expected_contract={
                "schema": {
                    "customer_id": "str",
                    "name": "str",
                    "total_paid": "float",
                    "payment_count": "int",
                },
                "sort_by": "customer_id",
            },
        ),
        "hard_dependency_chain": PipelineTask(
            task_id="hard_dependency_chain",
            difficulty="hard",
            prompt=(
                "Repair the staged orders ETL pipeline. The fixed code must define "
                "`run_pipeline(raw_orders)` plus any helper stages it needs. The pipeline should preserve "
                "stage compatibility, compute order revenue after percentage discounts, and assign customer "
                "tiers based on discounted revenue (`gold` if revenue >= 100.0 else `standard`)."
            ),
            broken_pipeline=hard_broken,
            input_data=[
                {
                    "order_id": "o1",
                    "customer": "Northwind",
                    "discount_pct": "10",
                    "items": [
                        {"sku": "A", "qty": 2, "unit_price": "40.00"},
                        {"sku": "B", "qty": 1, "unit_price": "30.00"},
                    ],
                },
                {
                    "order_id": "o2",
                    "customer": "Contoso",
                    "discount_pct": None,
                    "items": [
                        {"sku": "X", "qty": 5, "unit_price": "12.00"},
                    ],
                },
                {
                    "order_id": "o3",
                    "customer": "Fabrikam",
                    "discount_pct": "20",
                    "items": [
                        {"sku": "Y", "qty": 4, "unit_price": "35.00"},
                    ],
                },
            ],
            entrypoint="run_pipeline",
            expected_output=[
                {"order_id": "o1", "customer": "Northwind", "revenue": 99.0, "tier": "standard"},
                {"order_id": "o2", "customer": "Contoso", "revenue": 60.0, "tier": "standard"},
                {"order_id": "o3", "customer": "Fabrikam", "revenue": 112.0, "tier": "gold"},
            ],
            expected_contract={
                "schema": {
                    "order_id": "str",
                    "customer": "str",
                    "revenue": "float",
                    "tier": "str",
                },
                "tier_rule": "gold if revenue >= 100.0 else standard",
            },
        ),
    }


class DataPipelineDebugEnvironment(
    Environment[DataPipelineDebugAction, DataPipelineDebugObservation, DataPipelineDebugState]
):
    """Environment that grades repaired ETL/data-pipeline code deterministically."""

    SUPPORTS_CONCURRENT_SESSIONS = True

    def __init__(self):
        self._max_attempts = 3
        self._tasks = _build_tasks()
        self._task_order = [
            "easy_csv_null_type",
            "medium_schema_drift",
            "hard_dependency_chain",
        ]
        self._difficulty_to_task = {
            "easy": "easy_csv_null_type",
            "medium": "medium_schema_drift",
            "hard": "hard_dependency_chain",
        }
        self._task_cursor = 0
        self._current_task: PipelineTask | None = None
        self._state = DataPipelineDebugState()

    def reset(
        self,
        difficulty: str | None = None,
        task_id: str | None = None,
        seed: int | None = None,
        episode_id: str | None = None,
    ) -> DataPipelineDebugObservation:
        del seed
        selected_id = self._select_task_id(difficulty=difficulty, task_id=task_id)
        self._current_task = self._tasks[selected_id]
        self._state = DataPipelineDebugState(
            episode_id=episode_id or self._state.episode_id,
            step_count=0,
            task_id=self._current_task.task_id,
            difficulty=self._current_task.difficulty,
            completed=False,
            max_attempts=self._max_attempts,
            attempts_remaining=self._max_attempts,
            best_score=0.0,
        )
        return self._make_observation(
            score=0.0,
            attempts_remaining=self._state.attempts_remaining,
            reward_breakdown={},
        )

    def step(self, action: DataPipelineDebugAction) -> DataPipelineDebugObservation:
        if self._current_task is None:
            # NOTE: Keep this fallback for stateless HTTP invocations.
            # Some clients may call /step without sticky session state.
            # Recover by selecting a task from metadata or defaults.
            metadata = getattr(action, "metadata", {}) or {}
            self.reset(
                difficulty=metadata.get("difficulty"),
                task_id=metadata.get("task_id"),
            )

        self._state.step_count += 1

        try:
            result = self._grade_candidate(action.candidate_pipeline, self._current_task)
            passed = result["passed"]
            feedback = result["feedback"]
            score = float(result["score"])
            reward_breakdown = result["reward_breakdown"]
            reward = float(result["reward"])
        except ValidationError as exc:
            passed = False
            feedback = f"Action validation failed: {exc}"
            score = 0.0
            reward = 0.0
            reward_breakdown = {"validation_error_penalty": 0.0}

        self._state.best_score = max(self._state.best_score, score)
        self._state.attempts_remaining = max(0, self._state.attempts_remaining - 1)
        done = passed or self._state.attempts_remaining == 0

        self._state.completed = done
        return self._make_observation(
            last_submission=action.candidate_pipeline,
            feedback=feedback,
            passed=passed,
            reward=reward,
            done=done,
            score=score,
            attempts_remaining=self._state.attempts_remaining,
            reward_breakdown=reward_breakdown,
        )

    @property
    def state(self) -> DataPipelineDebugState:
        return self._state

    def _select_task_id(self, difficulty: str | None, task_id: str | None) -> str:
        if task_id is not None:
            if task_id not in self._tasks:
                raise ValueError(f"Unknown task_id: {task_id}")
            return task_id
        if difficulty is not None:
            if difficulty not in self._difficulty_to_task:
                raise ValueError(f"Unknown difficulty: {difficulty}")
            return self._difficulty_to_task[difficulty]
        selected_id = self._task_order[self._task_cursor % len(self._task_order)]
        self._task_cursor += 1
        return selected_id

    def _make_observation(
        self,
        last_submission: str | None = None,
        feedback: str | None = None,
        passed: bool = False,
        reward: float = 0.0,
        done: bool = False,
        score: float = 0.0,
        attempts_remaining: int = 0,
        reward_breakdown: dict[str, float] | None = None,
    ) -> DataPipelineDebugObservation:
        if self._current_task is None:
            raise RuntimeError("No active task available.")

        return DataPipelineDebugObservation(
            task_id=self._current_task.task_id,
            difficulty=self._current_task.difficulty,
            prompt=self._current_task.prompt,
            broken_pipeline=self._current_task.broken_pipeline,
            expected_contract=deepcopy(self._current_task.expected_contract),
            last_submission=last_submission,
            feedback=feedback,
            passed=passed,
            reward=reward,
            done=done,
            score=score,
            attempts_remaining=attempts_remaining,
            reward_breakdown=reward_breakdown or {},
        )

    def _grade_candidate(self, candidate_pipeline: str, task: PipelineTask) -> dict[str, Any]:
        score_components = {
            "schema_score": 0.0,
            "type_score": 0.0,
            "value_score": 0.0,
            "safety_penalty": 0.0,
            "runtime_penalty": 0.0,
        }
        issues: list[str] = []

        safety_penalty = self._static_safety_penalty(candidate_pipeline)
        score_components["safety_penalty"] = safety_penalty

        namespace: dict[str, Any] = {"__builtins__": SafeBuiltins}
        try:
            exec(candidate_pipeline, namespace, namespace)
        except Exception as exc:
            formatted = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            score_components["runtime_penalty"] = 0.35
            score = self._combine_score(score_components)
            return {
                "passed": False,
                "feedback": f"Compilation error: {formatted}",
                "score": score,
                "reward": score,
                "reward_breakdown": score_components,
            }

        if task.entrypoint not in namespace or not callable(namespace[task.entrypoint]):
            score_components["runtime_penalty"] = 0.25
            score = self._combine_score(score_components)
            return {
                "passed": False,
                "feedback": f"Your submission must define a callable `{task.entrypoint}`.",
                "score": score,
                "reward": score,
                "reward_breakdown": score_components,
            }

        try:
            actual_output = namespace[task.entrypoint](deepcopy(task.input_data))
        except Exception as exc:
            formatted = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            score_components["runtime_penalty"] = 0.30
            score = self._combine_score(score_components)
            return {
                "passed": False,
                "feedback": f"Pipeline raised an exception during execution: {formatted}",
                "score": score,
                "reward": score,
                "reward_breakdown": score_components,
            }

        validation = self._validate_output(actual_output, task.expected_output, task.expected_contract)
        issues = validation["issues"]
        score_components["schema_score"] = validation["schema_score"]
        score_components["type_score"] = validation["type_score"]
        score_components["value_score"] = validation["value_score"]

        score = self._combine_score(score_components)
        passed = bool(validation["passed"])

        if issues:
            return {
                "passed": passed,
                "feedback": " ; ".join(issues),
                "score": score,
                "reward": score,
                "reward_breakdown": score_components,
            }

        return {
            "passed": passed,
            "feedback": "All checks passed: output schema, values, and types match the expected contract.",
            "score": score,
            "reward": score,
            "reward_breakdown": score_components,
        }

    def _validate_output(
        self,
        actual_output: Any,
        expected_output: Any,
        expected_contract: dict[str, Any],
    ) -> dict[str, Any]:
        issues: list[str] = []
        schema_score = 0.0
        type_score = 0.0
        value_score = 0.0

        if not isinstance(actual_output, list):
            return {
                "passed": False,
                "issues": ["Output must be a list of dictionaries."],
                "schema_score": 0.0,
                "type_score": 0.0,
                "value_score": 0.0,
            }

        if not all(isinstance(row, dict) for row in actual_output):
            return {
                "passed": False,
                "issues": ["Every output row must be a dictionary."],
                "schema_score": 0.0,
                "type_score": 0.0,
                "value_score": 0.0,
            }

        schema = expected_contract.get("schema", {})
        expected_fields = len(schema)
        total_rows = max(1, len(expected_output))
        total_field_checks = max(1, total_rows * max(1, expected_fields))
        present_fields = 0
        typed_fields = 0

        for index, row in enumerate(actual_output):
            missing = [field for field in schema if field not in row]
            if missing:
                issues.append(f"Row {index} is missing fields: {missing}")
            present_fields += expected_fields - len(missing)
            for field, expected_type in schema.items():
                if field not in row:
                    continue
                value = row[field]
                if expected_type == "int" and not isinstance(value, int):
                    issues.append(f"Row {index} field `{field}` must be int.")
                elif expected_type == "float" and not isinstance(value, float):
                    issues.append(f"Row {index} field `{field}` must be float.")
                elif expected_type == "str" and not isinstance(value, str):
                    issues.append(f"Row {index} field `{field}` must be str.")
                else:
                    typed_fields += 1

        schema_score = min(1.0, present_fields / total_field_checks)
        type_score = min(1.0, typed_fields / total_field_checks)

        if "row_count" in expected_contract and len(actual_output) != expected_contract["row_count"]:
            issues.append(
                f"Expected {expected_contract['row_count']} rows but got {len(actual_output)}."
            )

        if expected_contract.get("sort_by"):
            sort_key = expected_contract["sort_by"]
            actual_output = sorted(actual_output, key=lambda row: row[sort_key])

        actual_norm = _normalize_json(actual_output)
        expected_norm = _normalize_json(expected_output)
        if actual_norm != expected_norm:
            issues.append("Output values do not match expected results.")
            value_score = self._approximate_value_score(actual_output, expected_output)
        else:
            value_score = 1.0

        return {
            "passed": len(issues) == 0,
            "issues": issues,
            "schema_score": schema_score,
            "type_score": type_score,
            "value_score": value_score,
        }

    def _static_safety_penalty(self, code: str) -> float:
        lowered = code.lower()
        blocked_tokens = [
            "import os",
            "import subprocess",
            "subprocess.",
            "open(",
            "__import__",
            "socket",
            "requests.",
        ]
        penalty_hits = sum(1 for token in blocked_tokens if token in lowered)
        if penalty_hits == 0:
            return 0.0
        return min(0.30, 0.10 * penalty_hits)

    def _approximate_value_score(self, actual_output: Any, expected_output: Any) -> float:
        if not isinstance(actual_output, list) or not isinstance(expected_output, list):
            return 0.0
        expected_len = max(1, len(expected_output))
        comparable_len = min(len(actual_output), len(expected_output))
        if comparable_len == 0:
            return 0.0
        matches = 0
        for idx in range(comparable_len):
            if _normalize_json(actual_output[idx]) == _normalize_json(expected_output[idx]):
                matches += 1
        return max(0.0, min(1.0, matches / expected_len))

    def _combine_score(self, components: dict[str, float]) -> float:
        # Dense reward signal with deterministic penalties.
        base = (
            0.35 * components["schema_score"]
            + 0.25 * components["type_score"]
            + 0.40 * components["value_score"]
        )
        penalties = components["safety_penalty"] + components["runtime_penalty"]
        score = base - penalties
        return float(max(0.0, min(1.0, score)))
