from __future__ import annotations

import json
import os
from pathlib import Path


def load_catalog() -> dict:
    path = os.getenv("NIDL_OKF_CATALOG")
    if not path:
        return {}
    catalog_path = Path(path)
    if not catalog_path.exists():
        raise ValueError("Configured OKF catalog does not exist")
    catalog = json.loads(catalog_path.read_text())
    if not isinstance(catalog, dict):
        raise ValueError("OKF catalog must be a JSON object keyed by versioned reference")
    return catalog


def resolve_recipe(reference: str | None):
    if not reference:
        return None
    catalog = load_catalog()
    if not catalog:
        raise ValueError("An OKF reference requires NIDL_OKF_CATALOG")
    recipe = catalog.get(reference)
    if not recipe:
        raise ValueError(f"Unknown OKF recipe: {reference}")
    return {"ref": reference, "digest": recipe.get("digest"), "permissions": recipe.get("permissions", []), "metadata": recipe.get("metadata", {})}
