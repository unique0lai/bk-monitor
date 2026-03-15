"""
metric_json 兼容转换工具。

把 base 侧的新结构转换成旧接口需要的返回结构，后续字段兼容可以继续在这里扩展。
"""

from __future__ import annotations

from typing import Any


def _dump_payload(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return value


def _normalize_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def convert_metric_json_to_legacy(metric_json: list[Any]) -> list[dict[str, Any]]:
    """
    将新的 metric_json 结构转换成旧接口结构。

    当前兼容规则：
    1. `rules` 统一转成 `rule_list`
    2. `dimensions` 默认补空数组
    3. `is_diff_metric` 默认补 `False`
    4. `is_manual` 在缺失时根据 `rule_list` 推断
    5. `tag_list` 优先使用旧字段，没有时兼容读取 `tags`
    """

    legacy_metric_json: list[dict[str, Any]] = []

    for metric_group in metric_json:
        metric_group_payload = _dump_payload(metric_group)
        rule_list = _normalize_list(metric_group_payload.get("rule_list") or metric_group_payload.get("rules"))

        legacy_fields: list[dict[str, Any]] = []
        for field in metric_group_payload.get("fields", []):
            field_payload = _dump_payload(field)
            tag_list = field_payload.get("tag_list")
            if tag_list is None:
                tag_list = field_payload.get("tags", [])

            legacy_fields.append(
                {
                    "description": field_payload.get("description", ""),
                    "type": field_payload.get("type"),
                    "monitor_type": field_payload.get("monitor_type"),
                    "unit": field_payload.get("unit", "none"),
                    "name": field_payload.get("name"),
                    "is_diff_metric": field_payload.get("is_diff_metric", False),
                    "is_active": field_payload.get("is_active", True),
                    "source_name": field_payload.get("source_name", ""),
                    "dimensions": _normalize_list(field_payload.get("dimensions")),
                    "is_manual": field_payload.get("is_manual", not bool(rule_list)),
                    "tag_list": _normalize_list(tag_list),
                }
            )

        legacy_metric_json.append(
            {
                "table_name": metric_group_payload.get("table_name"),
                "table_desc": metric_group_payload.get("table_desc", ""),
                "fields": legacy_fields,
                "rule_list": rule_list,
            }
        )

    return legacy_metric_json
