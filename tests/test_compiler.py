import json
from pathlib import Path

import pytest

from nidl.compiler import CompilationError, compile_plan, compile_workflow, validate_parameters
from nidl.models import Workflow
from nidl.primitives import REGISTRY


EXAMPLE = Path(__file__).parent.parent / "examples/harq.json"


def workflow():
    return Workflow.model_validate_json(EXAMPLE.read_text())


def test_compile_plan_is_stable_and_explains_cost():
    first = compile_plan(workflow(), set(REGISTRY))
    second = compile_plan(workflow(), set(REGISTRY))
    assert first["plan_id"] == second["plan_id"]
    assert first["step_count"] == 4
    assert first["side_effect_steps"] == ["incident"]
    assert first["estimated_cost_units"] == 4


def test_invalid_schedule_rejected():
    document = json.loads(EXAMPLE.read_text())
    document["triggers"] = [{"type": "schedule", "cron": "occasionally"}]
    with pytest.raises(CompilationError, match="cron"):
        compile_workflow(Workflow.model_validate(document), set(REGISTRY))


def test_parameter_contract_enforced():
    definition = workflow()
    assert validate_parameters(definition, {"market": "atlanta"})["min_users"] == 100
    with pytest.raises(ValueError, match="integer"):
        validate_parameters(definition, {"market": "atlanta", "min_users": "many"})
    with pytest.raises(ValueError, match="Unknown parameters"):
        validate_parameters(definition, {"market": "atlanta", "secret": True})


def test_parallel_step_may_be_exported():
    document = json.loads(EXAMPLE.read_text())
    document["steps"].append({"id": "parallel", "uses": "data.inline@1", "with": {"rows": []}})
    document["outputs"]["parallel"] = "${steps.parallel.rows}"
    plan = compile_plan(Workflow.model_validate(document), set(REGISTRY))
    assert "parallel" in plan["order"]
