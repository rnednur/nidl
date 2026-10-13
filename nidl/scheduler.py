from __future__ import annotations

from datetime import datetime, timezone

from .wakeup import dispatch_scheduled


def cron_due(expression: str, at: datetime | None = None) -> bool:
    at = at or datetime.now(timezone.utc)
    fields = expression.split()
    if len(fields) != 5:
        return False

    def matches(field: str, value: int) -> bool:
        if field == "*":
            return True
        if field.startswith("*/"):
            return value % int(field[2:]) == 0
        return value in {int(item) for item in field.split(",") if item.isdigit()}

    return matches(fields[0], at.minute) and matches(fields[1], at.hour) and matches(fields[2], at.day) and matches(fields[3], at.month) and matches(fields[4], (at.weekday() + 1) % 7)


def run_due_subscriptions(store, engine, tenant: str, at: datetime | None = None):
    return dispatch_scheduled(store, engine, tenant, at)
