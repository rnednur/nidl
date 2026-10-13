from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

TABLES = {"workflows", "subscriptions", "subscription_versions", "presentations", "runs", "incidents", "deliveries", "incident_events", "audit_events", "wake_events", "evaluation_cache", "notification_states", "agent_profiles", "agent_activity", "proposals"}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: str = "nidl.db"):
        self.path = path
        with self.db() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS workflows(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body TEXT NOT NULL,
                    compiled TEXT, created TEXT NOT NULL, UNIQUE(tenant, id));
                CREATE TABLE IF NOT EXISTS subscriptions(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1, next_run TEXT, created TEXT NOT NULL, updated TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS subscription_versions(
                    id TEXT PRIMARY KEY, subscription_id TEXT NOT NULL, version INTEGER NOT NULL,
                    body TEXT NOT NULL, actor TEXT NOT NULL, created TEXT NOT NULL,
                    UNIQUE(subscription_id, version));
                CREATE TABLE IF NOT EXISTS presentations(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS runs(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, workflow_id TEXT, subscription_id TEXT,
                    status TEXT NOT NULL, result TEXT NOT NULL, created TEXT NOT NULL, updated TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS incidents(
                    id TEXT PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL, tenant TEXT NOT NULL, market TEXT NOT NULL,
                    status TEXT NOT NULL, severity INTEGER NOT NULL, evidence TEXT NOT NULL,
                    affected_users INTEGER NOT NULL DEFAULT 0, acknowledged_by TEXT, updated TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS incident_events(
                    id TEXT PRIMARY KEY, incident_id TEXT NOT NULL, event_type TEXT NOT NULL,
                    body TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS deliveries(
                    id TEXT PRIMARY KEY, incident_id TEXT NOT NULL, subscription_id TEXT NOT NULL DEFAULT '',
                    channel TEXT NOT NULL, destination TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL, reason TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3, next_attempt TEXT, created TEXT NOT NULL, updated TEXT NOT NULL,
                    UNIQUE(incident_id, subscription_id, channel, destination, fingerprint));
                CREATE TABLE IF NOT EXISTS audit_events(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
                    resource TEXT NOT NULL, detail TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS wake_events(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, source TEXT NOT NULL, payload TEXT NOT NULL,
                    evaluation_key TEXT, status TEXT NOT NULL, result TEXT, created TEXT NOT NULL, processed TEXT);
                CREATE TABLE IF NOT EXISTS evaluation_cache(
                    evaluation_key TEXT PRIMARY KEY, tenant TEXT NOT NULL, workflow_id TEXT NOT NULL,
                    result TEXT NOT NULL, run_id TEXT NOT NULL, watermark TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS notification_states(
                    incident_id TEXT NOT NULL, subscription_id TEXT NOT NULL, last_sent_at TEXT,
                    last_evidence_fingerprint TEXT, next_eligible_at TEXT, acknowledged_at TEXT,
                    escalation_level INTEGER NOT NULL DEFAULT 0, notification_count INTEGER NOT NULL DEFAULT 0,
                    window_started_at TEXT, last_affected_users INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(incident_id, subscription_id));
                CREATE TABLE IF NOT EXISTS agent_profiles(
                    principal_id TEXT NOT NULL, tenant TEXT NOT NULL, body TEXT NOT NULL, updated TEXT NOT NULL,
                    PRIMARY KEY(principal_id, tenant));
                CREATE TABLE IF NOT EXISTS agent_activity(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, principal_id TEXT NOT NULL,
                    activity_type TEXT NOT NULL, body TEXT NOT NULL, created TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS proposals(
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
                    body TEXT NOT NULL, status TEXT NOT NULL, expires TEXT NOT NULL, created TEXT NOT NULL);
                """
            )
            self._migrate_legacy(connection)

    @staticmethod
    def _migrate_legacy(connection):
        additions = {
            "workflows": {"tenant": "TEXT NOT NULL DEFAULT 'default'", "compiled": "TEXT"},
            "subscriptions": {"tenant": "TEXT NOT NULL DEFAULT 'default'", "version": "INTEGER NOT NULL DEFAULT 1", "next_run": "TEXT", "updated": "TEXT"},
            "runs": {"tenant": "TEXT NOT NULL DEFAULT 'default'", "subscription_id": "TEXT", "updated": "TEXT"},
            "incidents": {"tenant": "TEXT NOT NULL DEFAULT 'default'", "severity": "INTEGER NOT NULL DEFAULT 3", "affected_users": "INTEGER NOT NULL DEFAULT 0", "acknowledged_by": "TEXT", "created": "TEXT"},
            "deliveries": {"subscription_id": "TEXT NOT NULL DEFAULT ''", "fingerprint": "TEXT NOT NULL DEFAULT ''", "reason": "TEXT", "attempts": "INTEGER NOT NULL DEFAULT 0", "max_attempts": "INTEGER NOT NULL DEFAULT 3", "next_attempt": "TEXT", "updated": "TEXT"},
        }
        for table, columns in additions.items():
            existing = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
            for column, definition in columns.items():
                if column not in existing:
                    connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def db(self):
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        return connection

    def _table(self, table: str) -> str:
        if table not in TABLES:
            raise ValueError("Unsupported table")
        return table

    def save(self, table: str, body: dict[str, Any], key: str | None = None, tenant: str = "default", compiled: dict | None = None):
        table = self._table(table)
        key = key or str(uuid.uuid4())
        timestamp = now()
        with self.db() as connection:
            if table == "workflows":
                connection.execute("INSERT INTO workflows(id,tenant,body,compiled,created) VALUES(?,?,?,?,?)", (key, tenant, json.dumps(body), json.dumps(compiled), timestamp))
            elif table == "presentations":
                connection.execute("INSERT INTO presentations(id,tenant,body,created) VALUES(?,?,?,?)", (key, tenant, json.dumps(body), timestamp))
            elif table == "subscriptions":
                connection.execute("INSERT INTO subscriptions(id,tenant,body,version,next_run,created,updated) VALUES(?,?,?,?,?,?,?)", (key, tenant, json.dumps(body), 1, None, timestamp, timestamp))
                connection.execute("INSERT INTO subscription_versions(id,subscription_id,version,body,actor,created) VALUES(?,?,?,?,?,?)", (str(uuid.uuid4()), key, 1, json.dumps(body), body.get("owner", "system"), timestamp))
            else:
                raise ValueError(f"Use a specialized save method for {table}")
        return key

    def get(self, table: str, key: str, tenant: str | None = None):
        table = self._table(table)
        query = f"SELECT * FROM {table} WHERE id=?"
        values: list[Any] = [key]
        if tenant is not None and table in {"workflows", "subscriptions", "presentations", "runs", "incidents", "audit_events", "wake_events", "evaluation_cache", "agent_profiles", "agent_activity", "proposals"}:
            query += " AND tenant=?"
            values.append(tenant)
        with self.db() as connection:
            row = connection.execute(query, values).fetchone()
        return dict(row) if row else None

    def list(self, table: str, tenant: str | None = None, limit: int = 200):
        table = self._table(table)
        query = f"SELECT * FROM {table}"
        values: list[Any] = []
        if tenant is not None and table in {"workflows", "subscriptions", "presentations", "runs", "incidents", "audit_events", "wake_events", "evaluation_cache", "agent_profiles", "agent_activity", "proposals"}:
            query += " WHERE tenant=?"
            values.append(tenant)
        query += " ORDER BY rowid DESC LIMIT ?"
        values.append(min(limit, 500))
        with self.db() as connection:
            return [dict(row) for row in connection.execute(query, values)]

    def update_subscription(self, key: str, patch: dict[str, Any], tenant: str, actor: str = "system"):
        row = self.get("subscriptions", key, tenant)
        if not row:
            return None
        body = json.loads(row["body"])
        body.update({key: value for key, value in patch.items() if value is not None})
        next_version = int(row["version"]) + 1
        with self.db() as connection:
            connection.execute(
                "UPDATE subscriptions SET body=?, version=version+1, updated=? WHERE id=? AND tenant=?",
                (json.dumps(body), now(), key, tenant),
            )
            connection.execute("INSERT INTO subscription_versions(id,subscription_id,version,body,actor,created) VALUES(?,?,?,?,?,?)", (str(uuid.uuid4()), key, next_version, json.dumps(body), actor, now()))
        return self.get("subscriptions", key, tenant)

    def subscription_versions(self, key: str):
        with self.db() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM subscription_versions WHERE subscription_id=? ORDER BY version DESC", (key,))]

    def save_run(self, workflow_id: str, status: str, result: dict[str, Any], tenant: str = "default", subscription_id: str | None = None):
        key, timestamp = str(uuid.uuid4()), now()
        with self.db() as connection:
            connection.execute(
                "INSERT INTO runs(id,tenant,workflow_id,subscription_id,status,result,created,updated) VALUES(?,?,?,?,?,?,?,?)",
                (key, tenant, workflow_id, subscription_id, status, json.dumps(result, default=str), timestamp, timestamp),
            )
        return key

    def create_run(self, key: str, workflow_id: str, tenant: str = "default", subscription_id: str | None = None, result: dict[str, Any] | None = None):
        """Create a stable run record once, allowing safe API/request retries."""
        timestamp = now()
        with self.db() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO runs(id,tenant,workflow_id,subscription_id,status,result,created,updated) VALUES(?,?,?,?,?,?,?,?)",
                (key, tenant, workflow_id, subscription_id, "PENDING", json.dumps(result or {}, default=str), timestamp, timestamp),
            )
        return self.get("runs", key, tenant)

    def update_run(self, key: str, status: str, result: dict[str, Any], tenant: str = "default"):
        with self.db() as connection:
            connection.execute(
                "UPDATE runs SET status=?,result=?,updated=? WHERE id=? AND tenant=?",
                (status, json.dumps(result, default=str), now(), key, tenant),
            )
        return self.get("runs", key, tenant)

    def upsert_incident(self, key: str, evidence: dict[str, Any], market: str, tenant: str = "default", severity: int = 3):
        fingerprint = hashlib.sha256(f"{tenant}:{market}:{key}".encode()).hexdigest()
        affected = int(evidence.get("estimated_users", evidence.get("affected_users", 0)) or 0)
        timestamp = now()
        with self.db() as connection:
            existing = connection.execute("SELECT * FROM incidents WHERE fingerprint=?", (fingerprint,)).fetchone()
            incident_id = existing["id"] if existing else str(uuid.uuid4())
            if not existing:
                status, event_type, previous = "NEW", "CREATED", None
                connection.execute(
                    "INSERT INTO incidents(id,fingerprint,tenant,market,status,severity,evidence,affected_users,acknowledged_by,updated,created) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (incident_id, fingerprint, tenant, market, status, severity, json.dumps(evidence, default=str), affected, None, timestamp, timestamp),
                )
            else:
                previous = dict(existing)
                prior_affected = int(existing["affected_users"])
                if existing["status"] == "RESOLVED" and affected > 0:
                    status = "REOPENED"
                elif affected == 0 and prior_affected > 0:
                    status = "RECOVERING"
                elif affected > prior_affected or severity > existing["severity"]:
                    status = "DEGRADING"
                elif existing["status"] == "RECOVERING" and affected == 0:
                    status = "RESOLVED"
                else:
                    status = "ACTIVE_STABLE"
                event_type = "STATE_CHANGED" if status != existing["status"] else "EVIDENCE_UPDATED"
                connection.execute(
                    "UPDATE incidents SET status=?,severity=?,evidence=?,affected_users=?,updated=? WHERE id=?",
                    (status, severity, json.dumps(evidence, default=str), affected, timestamp, incident_id),
                )
            event = {"status": status, "severity": severity, "affected_users": affected, "previous_status": previous["status"] if previous else None}
            connection.execute("INSERT INTO incident_events(id,incident_id,event_type,body,created) VALUES(?,?,?,?,?)", (str(uuid.uuid4()), incident_id, event_type, json.dumps(event), timestamp))
        return {"id": incident_id, "status": status, "severity": severity, "affected_users": affected}

    def incident_timeline(self, incident_id: str):
        with self.db() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM incident_events WHERE incident_id=? ORDER BY created", (incident_id,))]

    def append_incident_event(self, incident_id: str, event_type: str, body: dict[str, Any]):
        with self.db() as connection:
            connection.execute("INSERT INTO incident_events(id,incident_id,event_type,body,created) VALUES(?,?,?,?,?)", (str(uuid.uuid4()), incident_id, event_type, json.dumps(body, default=str), now()))

    def acknowledge(self, incident_id: str, actor: str, note: str | None, tenant: str):
        with self.db() as connection:
            row = connection.execute("SELECT id FROM incidents WHERE id=? AND tenant=?", (incident_id, tenant)).fetchone()
            if not row:
                return None
            connection.execute("UPDATE incidents SET acknowledged_by=?,updated=? WHERE id=?", (actor, now(), incident_id))
            connection.execute("INSERT INTO incident_events(id,incident_id,event_type,body,created) VALUES(?,?,?,?,?)", (str(uuid.uuid4()), incident_id, "ACKNOWLEDGED", json.dumps({"actor": actor, "note": note}), now()))
        return self.get("incidents", incident_id, tenant)

    def enqueue(self, incident_id: str, channel: str, destination: str | None, subscription_id: str = "", fingerprint: str | None = None, reason: str | None = None):
        fingerprint = fingerprint or hashlib.sha256(f"{incident_id}:{channel}:{destination}".encode()).hexdigest()
        key, timestamp = str(uuid.uuid4()), now()
        with self.db() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO deliveries(id,incident_id,subscription_id,channel,destination,fingerprint,status,reason,attempts,max_attempts,next_attempt,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (key, incident_id, subscription_id, channel, destination or "", fingerprint, "PENDING", reason, 0, 3, timestamp, timestamp, timestamp),
            )
            row = connection.execute(
                "SELECT id,status,reason FROM deliveries WHERE incident_id=? AND subscription_id=? AND channel=? AND destination=? AND fingerprint=?",
                (incident_id, subscription_id, channel, destination or "", fingerprint),
            ).fetchone()
        return dict(row)

    def update_delivery(self, delivery_id: str, status: str, reason: str | None = None, retry_at: str | None = None):
        with self.db() as connection:
            connection.execute("UPDATE deliveries SET status=?,reason=?,attempts=attempts+1,next_attempt=?,updated=? WHERE id=?", (status, reason, retry_at, now(), delivery_id))
        return self.get("deliveries", delivery_id)

    def claim_wakeup(self, event_id: str, tenant: str, source: str, payload: dict[str, Any], evaluation_key: str | None = None):
        with self.db() as connection:
            existing = connection.execute("SELECT * FROM wake_events WHERE id=?", (event_id,)).fetchone()
            if existing:
                return {**dict(existing), "duplicate": True}
            connection.execute("INSERT INTO wake_events VALUES(?,?,?,?,?,?,?,?,?)", (event_id, tenant, source, json.dumps(payload, default=str), evaluation_key, "CLAIMED", None, now(), None))
        return {**self.get("wake_events", event_id), "duplicate": False}

    def complete_wakeup(self, event_id: str, status: str, result: dict[str, Any]):
        with self.db() as connection:
            connection.execute("UPDATE wake_events SET status=?,result=?,processed=? WHERE id=?", (status, json.dumps(result, default=str), now(), event_id))
        return self.get("wake_events", event_id)

    def cached_evaluation(self, evaluation_key: str):
        with self.db() as connection:
            row = connection.execute("SELECT * FROM evaluation_cache WHERE evaluation_key=?", (evaluation_key,)).fetchone()
        return dict(row) if row else None

    def save_evaluation(self, evaluation_key: str, tenant: str, workflow_id: str, result: dict[str, Any], run_id: str, watermark: str):
        with self.db() as connection:
            connection.execute("INSERT INTO evaluation_cache VALUES(?,?,?,?,?,?,?) ON CONFLICT(evaluation_key) DO UPDATE SET result=excluded.result,run_id=excluded.run_id,watermark=excluded.watermark,created=excluded.created", (evaluation_key, tenant, workflow_id, json.dumps(result, default=str), run_id, watermark, now()))

    def notification_state(self, incident_id: str, subscription_id: str):
        with self.db() as connection:
            row = connection.execute("SELECT * FROM notification_states WHERE incident_id=? AND subscription_id=?", (incident_id, subscription_id)).fetchone()
        return dict(row) if row else None

    def save_notification_state(self, incident_id: str, subscription_id: str, state: dict[str, Any]):
        fields = {"last_sent_at": None, "last_evidence_fingerprint": None, "next_eligible_at": None, "acknowledged_at": None, "escalation_level": 0, "notification_count": 0, "window_started_at": None, "last_affected_users": 0, **state}
        with self.db() as connection:
            connection.execute("INSERT INTO notification_states VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(incident_id,subscription_id) DO UPDATE SET last_sent_at=excluded.last_sent_at,last_evidence_fingerprint=excluded.last_evidence_fingerprint,next_eligible_at=excluded.next_eligible_at,acknowledged_at=excluded.acknowledged_at,escalation_level=excluded.escalation_level,notification_count=excluded.notification_count,window_started_at=excluded.window_started_at,last_affected_users=excluded.last_affected_users", (incident_id, subscription_id, fields["last_sent_at"], fields["last_evidence_fingerprint"], fields["next_eligible_at"], fields["acknowledged_at"], fields["escalation_level"], fields["notification_count"], fields["window_started_at"], fields["last_affected_users"]))
        return self.notification_state(incident_id, subscription_id)

    def save_proposal(self, tenant: str, actor: str, action: str, body: dict[str, Any], expires: str):
        key = str(uuid.uuid4())
        with self.db() as connection:
            connection.execute("INSERT INTO proposals VALUES(?,?,?,?,?,?,?,?)", (key, tenant, actor, action, json.dumps(body), "PENDING", expires, now()))
        return key

    def resolve_proposal(self, key: str, tenant: str, status: str):
        with self.db() as connection:
            row = connection.execute("SELECT * FROM proposals WHERE id=? AND tenant=?", (key, tenant)).fetchone()
            if not row:
                return None
            connection.execute("UPDATE proposals SET status=? WHERE id=?", (status, key))
        return dict(row)

    def get_profile(self, principal_id: str, tenant: str):
        with self.db() as connection:
            row = connection.execute("SELECT * FROM agent_profiles WHERE principal_id=? AND tenant=?", (principal_id, tenant)).fetchone()
        return dict(row) if row else None

    def save_profile(self, principal_id: str, tenant: str, body: dict[str, Any]):
        with self.db() as connection:
            connection.execute("INSERT INTO agent_profiles VALUES(?,?,?,?) ON CONFLICT(principal_id,tenant) DO UPDATE SET body=excluded.body,updated=excluded.updated", (principal_id, tenant, json.dumps(body), now()))
        return self.get_profile(principal_id, tenant)

    def record_activity(self, tenant: str, principal_id: str, activity_type: str, body: dict[str, Any]):
        with self.db() as connection:
            connection.execute("INSERT INTO agent_activity VALUES(?,?,?,?,?,?)", (str(uuid.uuid4()), tenant, principal_id, activity_type, json.dumps(body, default=str), now()))

    def audit(self, tenant: str, actor: str, action: str, resource: str, detail: dict[str, Any] | None = None):
        with self.db() as connection:
            connection.execute("INSERT INTO audit_events(id,tenant,actor,action,resource,detail,created) VALUES(?,?,?,?,?,?,?)", (str(uuid.uuid4()), tenant, actor, action, resource, json.dumps(detail or {}), now()))

    def summary(self, tenant: str):
        with self.db() as connection:
            def count(table: str, extra: str = ""):
                return connection.execute(f"SELECT COUNT(*) FROM {table} WHERE tenant=? {extra}", (tenant,)).fetchone()[0]
            return {
                "workflows": count("workflows"),
                "subscriptions": count("subscriptions"),
                "active_incidents": count("incidents", "AND status NOT IN ('RESOLVED')"),
                "runs_24h": count("runs", "AND created >= datetime('now','-1 day')"),
            }
