from __future__ import annotations

import json
import os
from dataclasses import dataclass

from fastapi import Header, HTTPException


@dataclass(frozen=True)
class Principal:
    subject: str
    tenant: str
    markets: frozenset[str]
    roles: frozenset[str]

    def permits_market(self, market: str) -> bool:
        return "*" in self.markets or market in self.markets

    def has_role(self, role: str) -> bool:
        return "admin" in self.roles or role in self.roles


def current_principal(authorization: str | None = Header(default=None)) -> Principal:
    if os.getenv("NIDL_AUTH_MODE", "disabled") == "disabled":
        return Principal("local-developer", "default", frozenset({"*"}), frozenset({"admin"}))
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Bearer API key required")
    token = authorization.removeprefix("Bearer ")
    records = json.loads(os.getenv("NIDL_API_KEYS", "{}"))
    record = records.get(token)
    if not record:
        raise HTTPException(401, "Invalid API key")
    return Principal(record["subject"], record["tenant"], frozenset(record.get("markets", [])), frozenset(record.get("roles", [])))


def require_role(principal: Principal, role: str):
    if not principal.has_role(role):
        raise HTTPException(403, f"Role {role} required")


def require_market(principal: Principal, market: str):
    if not principal.permits_market(market):
        raise HTTPException(403, "Market is outside the authorized scope")
