"""质量监控结果分类的共享实现（统一写法的唯一入口）。

湖内质量监控在「产生检查结果」时，与治理报告在「读取质检结果」时，对
通过 / 不通过两类结果必须共用同一套分类规则：

- 检查结果只有两类：通过、不通过；
- 治理报告按质量监控事件数据中的两类来源事件定类别，来源事件类型沿用
  历史口径，已有事件仍按原字段（event_type / passed）读取：
  * quality_passed_to_govern  -> 通过
  * quality_failed_to_lineage -> 不通过

任何模块都不要再自行散落字面量判断，统一调用本模块的函数与常量。
"""
from __future__ import annotations

from typing import Any, Callable, Iterable

# 质量监控模块名（事件来源模块的统一写法）
QUALITY_MODULE = "quality_monitor"

# 两类来源事件：沿用既有事件类型，保证历史事件可按原字段读取
SOURCE_EVENT_PASSED = "quality_passed_to_govern"
SOURCE_EVENT_FAILED = "quality_failed_to_lineage"

# 检查结果的统一类别写法
CATEGORY_PASSED = "通过"
CATEGORY_FAILED = "不通过"
CATEGORIES: tuple[str, str] = (CATEGORY_PASSED, CATEGORY_FAILED)

# 类别 <-> 来源事件类型 的唯一映射表
EVENT_TYPE_BY_CATEGORY: dict[str, str] = {
    CATEGORY_PASSED: SOURCE_EVENT_PASSED,
    CATEGORY_FAILED: SOURCE_EVENT_FAILED,
}
CATEGORY_BY_EVENT_TYPE: dict[str, str] = {
    event_type: category for category, event_type in EVENT_TYPE_BY_CATEGORY.items()
}


def is_quality_result_event(event: dict[str, Any]) -> bool:
    """判断一条跨模块事件是否为质量监控检查结果事件（两类来源之一）。"""
    return (
        event.get("source_module") == QUALITY_MODULE
        and event.get("event_type") in CATEGORY_BY_EVENT_TYPE
    )


def category_from_passed(passed: bool) -> str:
    """检查产生侧：由原有 passed 字段得到统一类别。"""
    return CATEGORY_PASSED if passed else CATEGORY_FAILED


def event_type_from_passed(passed: bool) -> str:
    """检查产生侧：由原有 passed 字段得到对应的来源事件类型。"""
    return EVENT_TYPE_BY_CATEGORY[category_from_passed(passed)]


def classify_event(event: dict[str, Any]) -> str | None:
    """报表读取侧：按来源事件类型定类别；非质检结果事件返回 None。"""
    if not is_quality_result_event(event):
        return None
    return CATEGORY_BY_EVENT_TYPE[event["event_type"]]


def classify_receipt(receipt: dict[str, Any]) -> str:
    """报表读取侧：对检查回执按原有 passed 字段定类别。"""
    return category_from_passed(bool(receipt.get("passed")))


def split_by_category(
    rows: Iterable[dict[str, Any]],
    *,
    classifier: Callable[[dict[str, Any]], str | None],
) -> dict[str, list[dict[str, Any]]]:
    """按统一类别拆分一批记录（事件或回执均可）。"""
    buckets: dict[str, list[dict[str, Any]]] = {
        CATEGORY_PASSED: [],
        CATEGORY_FAILED: [],
    }
    for row in rows:
        category = classifier(row)
        if category in buckets:
            buckets[category].append(row)
    return buckets


def summarize(
    rows: Iterable[dict[str, Any]],
    *,
    classifier: Callable[[dict[str, Any]], str | None],
) -> dict[str, Any]:
    """对一批事件或回执做分类统计，键名与治理报告历史口径保持一致。

    返回 {"quality_checks", "passes", "failures", "by_category"}，
    其中前三个键与旧报告 report_data["quality_monitoring"] 完全同名同义。
    """
    buckets = split_by_category(rows, classifier=classifier)
    passes = len(buckets[CATEGORY_PASSED])
    failures = len(buckets[CATEGORY_FAILED])
    return {
        "quality_checks": passes + failures,
        "passes": passes,
        "failures": failures,
        "by_category": {
            CATEGORY_PASSED: passes,
            CATEGORY_FAILED: failures,
        },
    }
