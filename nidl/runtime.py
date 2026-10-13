from __future__ import annotations

import time

from .compiler import compile_workflow, resolve, validate_parameters
from .primitives import REGISTRY


class StepExecutionError(RuntimeError):
    def __init__(self, step_id: str, cause: Exception):
        super().__init__(f"Step {step_id} failed: {cause}")
        self.step_id = step_id
        self.cause = cause


class Engine:
    def __init__(self, store, queries=None):
        self.store = store
        self.queries = queries or {}

    def run(self, workflow, parameters=None, market="demo", dry_run=False, tenant="default"):
        ordered = compile_workflow(workflow, set(REGISTRY))
        params = validate_parameters(workflow, parameters or {})
        outputs, diagnostics = {}, []
        context = {"store": self.store, "market": params.get("market", market), "tenant": tenant, "queries": self.queries, "dry_run": dry_run}
        for step in ordered:
            started = time.perf_counter()
            condition = resolve(step.when, params, outputs)
            if condition is False:
                outputs[step.id] = {"status": "SKIPPED"}
                diagnostics.append({"step": step.id, "status": "SKIPPED", "duration_ms": 0})
                continue
            args = resolve(step.with_, params, outputs)
            if dry_run and step.uses in ("incident.upsert@1", "notify.enqueue@1"):
                outputs[step.id] = {"id": "dry-run", "status": "DRY_RUN"}
            else:
                attempts = 0
                while True:
                    try:
                        outputs[step.id] = REGISTRY[step.uses](args, context)
                        break
                    except Exception as exc:
                        attempts += 1
                        if attempts > step.retry:
                            diagnostics.append({"step": step.id, "status": "FAILED", "error": str(exc), "duration_ms": round((time.perf_counter() - started) * 1000, 2)})
                            raise StepExecutionError(step.id, exc) from exc
            diagnostics.append({"step": step.id, "status": "SUCCEEDED", "duration_ms": round((time.perf_counter() - started) * 1000, 2)})
        return {"steps": outputs, "outputs": resolve(workflow.outputs, params, outputs), "diagnostics": diagnostics}
