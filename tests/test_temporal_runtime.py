import json
from pathlib import Path

import pytest

from nidl.models import Workflow
from nidl.store import Store
from nidl.temporal_runtime import TemporalService, TemporalSettings, build_execution_input


EXAMPLE = Path(__file__).parent.parent / "examples/harq.json"


def test_temporal_settings_are_environment_driven(monkeypatch):
    monkeypatch.setenv("NIDL_TEMPORAL_ADDRESS", "temporal.internal:7233")
    monkeypatch.setenv("NIDL_TEMPORAL_NAMESPACE", "telco")
    monkeypatch.setenv("NIDL_TEMPORAL_TASK_QUEUE", "market-evaluations")
    monkeypatch.setenv("NIDL_TEMPORAL_TLS", "true")
    settings = TemporalSettings.from_env()
    assert settings.address == "temporal.internal:7233"
    assert settings.namespace == "telco"
    assert settings.task_queue == "market-evaluations"
    assert settings.tls is True


def test_execution_input_is_compiled_validated_and_stable():
    definition = json.loads(EXAMPLE.read_text())
    payload = build_execution_input("wf-1", definition, {"market": "atlanta"}, "telco", "atlanta", request_id="request-1")
    assert payload["run_id"] == "request-1"
    assert payload["parameters"]["min_users"] == 100
    assert [step["id"] for step in payload["steps"]] == ["kpis", "detect", "impact", "incident"]
    assert payload["workflow_version"] == "1.0.0"


def test_explicit_run_id_makes_submission_record_idempotent(tmp_path):
    store = Store(str(tmp_path / "durable.db"))
    first = store.create_run("request-1", "wf-1", "telco", result={"submission": "pending"})
    second = store.create_run("request-1", "wf-1", "telco", result={"submission": "different"})
    assert first["id"] == second["id"]
    assert json.loads(second["result"])["submission"] == "pending"
    updated = store.update_run("request-1", "RUNNING", {"current_step": "detect"}, "telco")
    assert updated["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_service_uses_deterministic_temporal_workflow_id(monkeypatch):
    class Handle:
        id = "nidl-run-request-1"
        result_run_id = "temporal-run-1"

    class Client:
        def __init__(self):
            self.arguments = None

        async def start_workflow(self, *args, **kwargs):
            self.arguments = (args, kwargs)
            return Handle()

    client = Client()
    service = TemporalService(TemporalSettings(task_queue="queue-1"), client)
    result = await service.start({"run_id": "request-1"})
    assert result["temporal_workflow_id"] == "nidl-run-request-1"
    assert client.arguments[1]["id"] == "nidl-run-request-1"
    assert client.arguments[1]["task_queue"] == "queue-1"
