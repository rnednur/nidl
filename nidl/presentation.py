from __future__ import annotations

import html
from typing import Any


def field_value(data: dict[str, Any], path: str):
    value: Any = data
    for part in path.split("."):
        value = value[int(part)] if isinstance(value, list) else value.get(part)
        if value is None:
            break
    return value


def normalize_presentation(definition, result: dict[str, Any]):
    rendered = []
    for component in definition.components:
        item = {"type": component["type"]}
        if component["type"] == "metric_cards":
            item["items"] = [{**metric, "value": field_value(result, metric["field"])} for metric in component.get("items", [])]
        elif "source" in component:
            item["data"] = field_value(result, component["source"]) or []
            item["config"] = {key: value for key, value in component.items() if key not in {"type", "source"}}
        elif "field" in component:
            item["value"] = field_value(result, component["field"])
        else:
            item["text"] = html.escape(str(component.get("text", "")))
        rendered.append(item)
    return {"components": rendered}
