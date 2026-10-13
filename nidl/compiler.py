from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .models import Workflow

REF = re.compile(r"^\$\{(params|steps)\.([a-zA-Z0-9_-]+)(?:\.([a-zA-Z0-9_.-]+))?\}$")
CRON = re.compile(r"^(\*|\*/[1-9][0-9]*|[0-9,-]+)(\s+(\*|\*/[1-9][0-9]*|[0-9,-]+)){4}$")


class CompilationError(ValueError):
    pass


def _ancestors(step_id: str, steps: dict[str, Any]) -> set[str]:
    result: set[str] = set()

    def collect(current: str):
        for dependency in steps[current].needs:
            if dependency not in result:
                result.add(dependency)
                collect(dependency)

    collect(step_id)
    return result


def _check_references(value: Any, step_id: str, workflow: Workflow, steps: dict[str, Any]):
    if isinstance(value, str):
        match = REF.match(value)
        if not match:
            return
        namespace, target, _ = match.groups()
        if namespace == "params" and target not in workflow.parameters:
            raise CompilationError(f"Unknown parameter {target} in {step_id}")
        if namespace == "steps" and (target not in steps or target not in _ancestors(step_id, steps)):
            raise CompilationError(f"Unmet step reference {target} in {step_id}")
    elif isinstance(value, dict):
        for nested in value.values():
            _check_references(nested, step_id, workflow, steps)
    elif isinstance(value, list):
        for nested in value:
            _check_references(nested, step_id, workflow, steps)


def _check_output_references(value: Any, workflow: Workflow, steps: dict[str, Any]):
    if isinstance(value, str):
        match = REF.match(value)
        if not match:
            return
        namespace, target, _ = match.groups()
        if namespace == "params" and target not in workflow.parameters:
            raise CompilationError(f"Unknown output parameter {target}")
        if namespace == "steps" and target not in steps:
            raise CompilationError(f"Unknown output step {target}")
    elif isinstance(value, dict):
        for nested in value.values():
            _check_output_references(nested, workflow, steps)
    elif isinstance(value, list):
        for nested in value:
            _check_output_references(nested, workflow, steps)


def compile_workflow(workflow: Workflow, registered: set[str]):
    steps = {step.id: step for step in workflow.steps}
    for trigger in workflow.triggers:
        if trigger.get("type") == "schedule" and not CRON.match(trigger.get("cron", "")):
            raise CompilationError("Invalid five-field schedule cron")
    for step in workflow.steps:
        if step.uses not in registered:
            raise CompilationError(f"Unregistered primitive: {step.uses}")

    result, visiting, visited = [], set(), set()

    def visit(step_id: str):
        if step_id in visiting:
            raise CompilationError("Cycle in workflow DAG")
        if step_id in visited:
            return
        visiting.add(step_id)
        for dependency in steps[step_id].needs:
            visit(dependency)
        visiting.remove(step_id)
        visited.add(step_id)
        result.append(steps[step_id])

    for step_id in steps:
        visit(step_id)
    for step in result:
        _check_references(step.with_, step.id, workflow, steps)
        _check_references(step.when, step.id, workflow, steps)
    _check_output_references(workflow.outputs, workflow, steps)
    return result


def compile_plan(workflow: Workflow, registered: set[str]) -> dict[str, Any]:
    ordered = compile_workflow(workflow, registered)
    canonical = json.dumps(workflow.model_dump(by_alias=True), sort_keys=True, separators=(",", ":"))
    warnings = []
    if not workflow.triggers:
        warnings.append("No trigger configured; runs must be started manually")
    if any(step.uses == "agent.investigate@1" for step in ordered):
        warnings.append("Plan contains a non-deterministic agent step")
    return {
        "plan_id": hashlib.sha256(canonical.encode()).hexdigest()[:24],
        "workflow": workflow.metadata["name"],
        "version": workflow.metadata["version"],
        "order": [step.id for step in ordered],
        "step_count": len(ordered),
        "side_effect_steps": [step.id for step in ordered if step.uses.startswith(("incident.", "notify."))],
        "estimated_cost_units": sum(5 if step.uses.startswith("starrocks.") else 10 if step.uses.startswith("agent.") else 1 for step in ordered),
        "warnings": warnings,
        "artifact_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
    }


def validate_parameters(workflow: Workflow, supplied: dict[str, Any]) -> dict[str, Any]:
    values = {name: definition["default"] for name, definition in workflow.parameters.items() if "default" in definition}
    values.update(supplied)
    unknown = set(values) - set(workflow.parameters)
    if unknown:
        raise ValueError(f"Unknown parameters: {', '.join(sorted(unknown))}")
    python_types = {"integer": int, "number": (int, float), "string": str, "boolean": bool, "market_ref": str}
    for name, definition in workflow.parameters.items():
        if definition.get("required") and name not in values:
            raise ValueError(f"Missing required parameter: {name}")
        if name not in values:
            continue
        expected = python_types.get(definition.get("type"))
        value = values[name]
        wrong_bool = expected is int and isinstance(value, bool)
        if expected and (wrong_bool or not isinstance(value, expected)):
            raise ValueError(f"Parameter {name} must be {definition['type']}")
        if "min" in definition and value < definition["min"]:
            raise ValueError(f"Parameter {name} is below minimum")
        if "max" in definition and value > definition["max"]:
            raise ValueError(f"Parameter {name} is above maximum")
        if "enum" in definition and value not in definition["enum"]:
            raise ValueError(f"Parameter {name} is not an allowed value")
    return values


def resolve(value: Any, params: dict[str, Any], steps: dict[str, Any]):
    if isinstance(value, str):
        match = REF.match(value)
        if not match:
            return value
        namespace, name, path = match.groups()
        result = params[name] if namespace == "params" else steps[name]
        for part in (path or "").split("."):
            if part:
                result = result[int(part)] if isinstance(result, list) else result[part]
        return result
    if isinstance(value, dict):
        return {key: resolve(nested, params, steps) for key, nested in value.items()}
    if isinstance(value, list):
        return [resolve(nested, params, steps) for nested in value]
    return value
