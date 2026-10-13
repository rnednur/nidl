import json
from pathlib import Path
import pytest
from nidl.models import Workflow
from nidl.compiler import compile_workflow, CompilationError
from nidl.primitives import REGISTRY
from nidl.runtime import Engine
from nidl.store import Store

EXAMPLE=Path(__file__).parent.parent/'examples/harq.json'
def test_run(tmp_path):
    workflow=Workflow.model_validate_json(EXAMPLE.read_text())
    engine=Engine(Store(str(tmp_path/'test.db')))
    result=engine.run(workflow,{'market':'atlanta'})
    assert result['outputs']['impact']['estimated_users']==500
    assert result['steps']['detect']['count']==1
    assert engine.store.list('incidents')[0]['market']=='atlanta'
def test_dry_run_no_incident(tmp_path):
    workflow=Workflow.model_validate_json(EXAMPLE.read_text())
    store=Store(str(tmp_path/'test.db'))
    Engine(store).run(workflow,{'market':'demo'},dry_run=True)
    assert store.list('incidents')==[]
def test_cycle_rejected():
    doc=json.loads(EXAMPLE.read_text())
    doc['steps'][0]['needs']=['detect']
    with pytest.raises(CompilationError): compile_workflow(Workflow.model_validate(doc),set(REGISTRY))
def test_unknown_primitive():
    doc=json.loads(EXAMPLE.read_text());doc['steps'][0]['uses']='shell.exec@1'
    with pytest.raises(CompilationError): compile_workflow(Workflow.model_validate(doc),set(REGISTRY))
def test_idempotent_incident(tmp_path):
    w=Workflow.model_validate_json(EXAMPLE.read_text());store=Store(str(tmp_path/'test.db'))
    engine=Engine(store)
    first=engine.run(w,{'market':'demo'})['outputs']['incident']
    second=engine.run(w,{'market':'demo'})['outputs']['incident']
    assert first==second
    assert len(store.list('incidents'))==1
