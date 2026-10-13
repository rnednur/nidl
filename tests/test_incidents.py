from nidl.store import Store


def test_incident_lifecycle_and_timeline(tmp_path):
    store = Store(str(tmp_path / "state.db"))
    created = store.upsert_incident("episode", {"estimated_users": 10}, "atlanta")
    degraded = store.upsert_incident("episode", {"estimated_users": 30}, "atlanta", severity=4)
    recovering = store.upsert_incident("episode", {"estimated_users": 0}, "atlanta")
    resolved = store.upsert_incident("episode", {"estimated_users": 0}, "atlanta")
    assert created["status"] == "NEW"
    assert degraded["status"] == "DEGRADING"
    assert recovering["status"] == "RECOVERING"
    assert resolved["status"] == "RESOLVED"
    assert len(store.incident_timeline(created["id"])) == 4


def test_incident_identity_is_tenant_scoped(tmp_path):
    store = Store(str(tmp_path / "state.db"))
    one = store.upsert_incident("same", {}, "atlanta", "tenant-a")
    two = store.upsert_incident("same", {}, "atlanta", "tenant-b")
    assert one["id"] != two["id"]
    assert len(store.list("incidents", "tenant-a")) == 1


def test_acknowledgement_is_audited(tmp_path):
    store = Store(str(tmp_path / "state.db"))
    incident = store.upsert_incident("episode", {}, "atlanta")
    row = store.acknowledge(incident["id"], "operator", "Investigating", "default")
    assert row["acknowledged_by"] == "operator"
    assert store.incident_timeline(incident["id"])[-1]["event_type"] == "ACKNOWLEDGED"


def test_delivery_is_idempotent_per_fingerprint(tmp_path):
    store = Store(str(tmp_path / "state.db"))
    incident = store.upsert_incident("episode", {}, "atlanta")
    first = store.enqueue(incident["id"], "slack", "#noc", "sub-1", "fp-1")
    second = store.enqueue(incident["id"], "slack", "#noc", "sub-1", "fp-1")
    assert first == second
    assert len(store.list("deliveries")) == 1
