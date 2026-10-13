from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any


class TemporalNotInstalled(RuntimeError):
    pass


@dataclass(frozen=True)
class TemporalSettings:
    address: str = "localhost:7233"
    namespace: str = "default"
    task_queue: str = "nidl-workflows"
    tls: bool = False

    @classmethod
    def from_env(cls) -> "TemporalSettings":
        return cls(
            address=os.getenv("NIDL_TEMPORAL_ADDRESS", "localhost:7233"),
            namespace=os.getenv("NIDL_TEMPORAL_NAMESPACE", "default"),
            task_queue=os.getenv("NIDL_TEMPORAL_TASK_QUEUE", "nidl-workflows"),
            tls=os.getenv("NIDL_TEMPORAL_TLS", "false").lower() in {"1", "true", "yes"},
        )


def temporal_available() -> bool:
    try:
        import temporalio  # noqa: F401
    except ImportError:
        return False
    return True


def build_execution_input(
    workflow_id: str,
    definition: dict[str, Any],
    parameters: dict[str, Any],
    tenant: str,
    market: str,
    dry_run: bool = False,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Build the immutable payload recorded in Temporal history."""
    from .compiler import compile_workflow, validate_parameters
    from .models import Workflow
    from .primitives import REGISTRY

    model = Workflow.model_validate(definition)
    ordered = compile_workflow(model, set(REGISTRY))
    params = validate_parameters(model, parameters)
    run_id = request_id or str(uuid.uuid4())
    return {
        "run_id": run_id,
        "workflow_id": workflow_id,
        "workflow_name": model.metadata["name"],
        "workflow_version": model.metadata["version"],
        "tenant": tenant,
        "market": params.get("market", market),
        "parameters": params,
        "dry_run": dry_run,
        "steps": [step.model_dump(by_alias=True) for step in ordered],
        "outputs": model.outputs,
    }


try:
    from temporalio import activity, workflow
    from temporalio.common import RetryPolicy
    from temporalio.exceptions import ApplicationError
    from temporalio.worker import Worker
except ImportError:  # Local development can run without the optional Temporal extra.
    activity = workflow = RetryPolicy = ApplicationError = Worker = None


if activity is not None:

    @activity.defn
    async def begin_execution(payload: dict[str, Any]) -> dict[str, Any]:
        from .store import Store

        store = Store(os.getenv("NIDL_DB", "nidl.db"))
        row = store.create_run(
            payload["run_id"],
            payload["workflow_id"],
            payload["tenant"],
            result={"orchestrator": "temporal", "workflow_version": payload["workflow_version"]},
        )
        if row and row["status"] in {"SUCCEEDED", "CANCELED"}:
            return {"already_complete": True, "status": row["status"], "result": json.loads(row["result"])}
        store.update_run(payload["run_id"], "RUNNING", {"orchestrator": "temporal", "current_step": None}, payload["tenant"])
        return {"already_complete": False}


    @activity.defn
    async def execute_step(payload: dict[str, Any]) -> dict[str, Any]:
        from .compiler import resolve
        from .primitives import REGISTRY
        from .store import Store

        activity.heartbeat({"step": payload["step"]["id"], "phase": "starting"})
        step = payload["step"]
        condition = resolve(step.get("when", True), payload["parameters"], payload["outputs"])
        if condition is False:
            return {"status": "SKIPPED", "value": {"status": "SKIPPED"}}
        if payload["dry_run"] and step["uses"] in {"incident.upsert@1", "notify.enqueue@1"}:
            return {"status": "DRY_RUN", "value": {"id": "dry-run", "status": "DRY_RUN"}}
        store = Store(os.getenv("NIDL_DB", "nidl.db"))
        queries_path = Path(os.getenv("NIDL_QUERIES", "queries.json"))
        queries = json.loads(queries_path.read_text()) if queries_path.exists() else {}
        args = resolve(step.get("with", {}), payload["parameters"], payload["outputs"])
        context = {
            "store": store,
            "market": payload["market"],
            "tenant": payload["tenant"],
            "queries": queries,
            "dry_run": payload["dry_run"],
        }
        value = REGISTRY[step["uses"]](args, context)
        activity.heartbeat({"step": step["id"], "phase": "complete"})
        return {"status": "SUCCEEDED", "value": value}


    @activity.defn
    async def finish_execution(payload: dict[str, Any]) -> dict[str, Any]:
        from .store import Store

        store = Store(os.getenv("NIDL_DB", "nidl.db"))
        result = payload["result"]
        store.update_run(payload["run_id"], payload["status"], result, payload["tenant"])
        return result


    @workflow.defn
    class NIDLExecutionWorkflow:
        def __init__(self) -> None:
            self.state: dict[str, Any] = {"status": "PENDING", "current_step": None, "completed_steps": [], "cancel_requested": False}

        @workflow.run
        async def run(self, request: dict[str, Any]) -> dict[str, Any]:
            self.state["status"] = "STARTING"
            base_options = {
                "start_to_close_timeout": timedelta(seconds=30),
                "retry_policy": RetryPolicy(maximum_attempts=5),
            }
            begin = await workflow.execute_activity(begin_execution, request, **base_options)
            if begin["already_complete"]:
                self.state["status"] = begin["status"]
                return begin["result"]

            outputs: dict[str, Any] = {}
            diagnostics: list[dict[str, Any]] = []
            try:
                self.state["status"] = "RUNNING"
                for step in request["steps"]:
                    if self.state["cancel_requested"]:
                        result = {"steps": outputs, "diagnostics": diagnostics, "canceled": True}
                        await workflow.execute_activity(
                            finish_execution,
                            {"run_id": request["run_id"], "tenant": request["tenant"], "status": "CANCELED", "result": result},
                            **base_options,
                        )
                        self.state["status"] = "CANCELED"
                        return result
                    self.state["current_step"] = step["id"]
                    outcome = await workflow.execute_activity(
                        execute_step,
                        {**request, "step": step, "outputs": outputs},
                        start_to_close_timeout=timedelta(seconds=step.get("timeout_seconds", 60)),
                        heartbeat_timeout=timedelta(seconds=min(step.get("timeout_seconds", 60), 30)),
                        retry_policy=RetryPolicy(maximum_attempts=step.get("retry", 0) + 1),
                    )
                    outputs[step["id"]] = outcome["value"]
                    diagnostics.append({"step": step["id"], "status": outcome["status"]})
                    self.state["completed_steps"].append(step["id"])
                resolved_outputs = await workflow.execute_activity(
                    resolve_outputs,
                    {"definition": request["outputs"], "parameters": request["parameters"], "outputs": outputs},
                    **base_options,
                )
                result = {"steps": outputs, "outputs": resolved_outputs, "diagnostics": diagnostics, "orchestrator": "temporal"}
                await workflow.execute_activity(
                    finish_execution,
                    {"run_id": request["run_id"], "tenant": request["tenant"], "status": "SUCCEEDED", "result": result},
                    **base_options,
                )
                self.state["status"] = "SUCCEEDED"
                self.state["current_step"] = None
                return result
            except Exception as exc:
                failure = {"steps": outputs, "diagnostics": diagnostics, "error": str(exc), "orchestrator": "temporal"}
                await workflow.execute_activity(
                    finish_execution,
                    {"run_id": request["run_id"], "tenant": request["tenant"], "status": "FAILED", "result": failure},
                    **base_options,
                )
                self.state["status"] = "FAILED"
                raise ApplicationError("NIDL durable execution failed", non_retryable=True) from exc

        @workflow.signal
        def request_cancel(self) -> None:
            self.state["cancel_requested"] = True

        @workflow.query
        def execution_state(self) -> dict[str, Any]:
            return self.state


    @activity.defn
    async def resolve_outputs(payload: dict[str, Any]) -> dict[str, Any]:
        from .compiler import resolve

        return resolve(payload["definition"], payload["parameters"], payload["outputs"])

else:

    class NIDLExecutionWorkflow:  # pragma: no cover - placeholder for helpful imports
        pass

    begin_execution = execute_step = finish_execution = resolve_outputs = None


class TemporalService:
    def __init__(self, settings: TemporalSettings | None = None, client: Any | None = None):
        self.settings = settings or TemporalSettings.from_env()
        self._client = client

    async def client(self):
        if self._client is not None:
            return self._client
        if not temporal_available():
            raise TemporalNotInstalled("Install the Temporal integration with: pip install -e '.[temporal]'")
        from temporalio.client import Client

        self._client = await Client.connect(self.settings.address, namespace=self.settings.namespace, tls=self.settings.tls)
        return self._client

    async def start(self, request: dict[str, Any]) -> dict[str, Any]:
        client = await self.client()
        temporal_workflow_id = f"nidl-run-{request['run_id']}"
        workflow_ref = getattr(NIDLExecutionWorkflow, "run", "NIDLExecutionWorkflow")
        handle = await client.start_workflow(
            workflow_ref,
            request,
            id=temporal_workflow_id,
            task_queue=self.settings.task_queue,
        )
        return {"run_id": request["run_id"], "temporal_workflow_id": handle.id, "temporal_run_id": handle.result_run_id, "status": "PENDING"}

    async def state(self, run_id: str) -> dict[str, Any]:
        handle = (await self.client()).get_workflow_handle(f"nidl-run-{run_id}")
        return await handle.query(NIDLExecutionWorkflow.execution_state)

    async def cancel(self, run_id: str) -> dict[str, Any]:
        handle = (await self.client()).get_workflow_handle(f"nidl-run-{run_id}")
        await handle.signal(NIDLExecutionWorkflow.request_cancel)
        return {"run_id": run_id, "cancel_requested": True}


async def run_worker(settings: TemporalSettings | None = None) -> None:
    if not temporal_available():
        raise TemporalNotInstalled("Install the Temporal integration with: pip install -e '.[temporal]'")
    from temporalio.client import Client

    settings = settings or TemporalSettings.from_env()
    client = await Client.connect(settings.address, namespace=settings.namespace, tls=settings.tls)
    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[NIDLExecutionWorkflow],
        activities=[begin_execution, execute_step, finish_execution, resolve_outputs],
    )
    await worker.run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NIDL Temporal worker")
    parser.parse_args()
    asyncio.run(run_worker())


def configuration() -> dict[str, Any]:
    return {"available": temporal_available(), **asdict(TemporalSettings.from_env())}
