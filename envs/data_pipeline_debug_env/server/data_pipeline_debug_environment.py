"""Core environment logic for the data pipeline debugging environment."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
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
        )
        return self._make_observation()

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
            reward = 1.0 if passed else 0.0
        except ValidationError as exc:
            passed = False
            feedback = f"Action validation failed: {exc}"
            reward = 0.0

        self._state.completed = True
        return self._make_observation(
            last_submission=action.candidate_pipeline,
            feedback=feedback,
            passed=passed,
            reward=reward,
            done=True,
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
        )

    def _grade_candidate(self, candidate_pipeline: str, task: PipelineTask) -> dict[str, Any]:
        namespace: dict[str, Any] = {"__builtins__": SafeBuiltins}
        exec(candidate_pipeline, namespace, namespace)

        if task.entrypoint not in namespace or not callable(namespace[task.entrypoint]):
            return {
                "passed": False,
                "feedback": f"Your submission must define a callable `{task.entrypoint}`.",
            }

        try:
            actual_output = namespace[task.entrypoint](deepcopy(task.input_data))
        except Exception as exc:
            formatted = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            return {
                "passed": False,
                "feedback": f"Pipeline raised an exception during execution: {formatted}",
            }

        issues = self._validate_output(actual_output, task.expected_output, task.expected_contract)
        if issues:
            return {
                "passed": False,
                "feedback": " ; ".join(issues),
            }

        return {
            "passed": True,
            "feedback": "All checks passed: output schema, values, and types match the expected contract.",
        }

    def _validate_output(
        self,
        actual_output: Any,
        expected_output: Any,
        expected_contract: dict[str, Any],
    ) -> list[str]:
        issues: list[str] = []

        if not isinstance(actual_output, list):
            return ["Output must be a list of dictionaries."]

        if not all(isinstance(row, dict) for row in actual_output):
            return ["Every output row must be a dictionary."]

        schema = expected_contract.get("schema", {})
        for index, row in enumerate(actual_output):
            missing = [field for field in schema if field not in row]
            if missing:
                issues.append(f"Row {index} is missing fields: {missing}")
                continue
            for field, expected_type in schema.items():
                value = row[field]
                if expected_type == "int" and not isinstance(value, int):
                    issues.append(f"Row {index} field `{field}` must be int.")
                elif expected_type == "float" and not isinstance(value, float):
                    issues.append(f"Row {index} field `{field}` must be float.")
                elif expected_type == "str" and not isinstance(value, str):
                    issues.append(f"Row {index} field `{field}` must be str.")

        if "row_count" in expected_contract and len(actual_output) != expected_contract["row_count"]:
            issues.append(
                f"Expected {expected_contract['row_count']} rows but got {len(actual_output)}."
            )

        if expected_contract.get("sort_by"):
            sort_key = expected_contract["sort_by"]
            actual_output = sorted(actual_output, key=lambda row: row[sort_key])

        if _normalize_json(actual_output) != _normalize_json(expected_output):
            issues.append("Output values do not match expected results.")

        return issues
