"""湖内质量监控领域服务：配置数据湖内数据的完整性、时效性和准确性监控规则。

上游接收入湖管道完成事件，对入湖数据执行质量检查。
下游联动血缘图谱记录质量问题，再汇总到治理报告中。

检查结果的通过/不通过分类、以及两类来源事件类型的解释，统一收口到
``app.quality_classification``，本服务只负责产生检查与回执，
不自行散落分类字面量。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.quality_classification import (
    EVENT_TYPE_BY_CATEGORY,
    QUALITY_MODULE,
    category_from_passed,
)
from app.store import store


class QualityMonitorService(BaseModuleService):
    MODULE = "quality_monitor"
    ENTITY = "监控规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "监控指标", "告警阈值", "目标湖表", "检查频率"]
    REQUIRED_FIELDS = ["规则名称", "监控指标", "告警阈值"]
    STATUS_ORDER = ["待启用", "已启用", "已停用"]
    ACTION_RULES = {"启用监控": "已启用", "停用监控": "已停用"}
    NEGATIVE_ACTIONS = ["停用监控"]

    def enable_monitor(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启用监控规则：验证目标湖表存在。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "待启用":
            return None, f"监控规则状态为{entry.get('status')}，不可启用"

        table_name = entry.get("目标湖表", "")

        # 验证目标湖表已注册
        lake_tables = store.find_by_field("lake_table", "表名", table_name)
        if not lake_tables:
            return None, f"目标湖表 {table_name} 不存在"

        entry["status"] = "已启用"
        entry["pending"] = False
        entry["abnormal"] = False

        store.log_event("quality_rule_enabled", "quality_monitor", "lake_table", {
            "rule_id": entry_id,
            "table_name": table_name,
            "metric": entry.get("监控指标", ""),
        })

        return entry, f"监控规则 {entry.get('规则名称')} 已启用，指标={entry.get('监控指标')}"

    def check_quality(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行质量检查：对目标湖表数据执行指标检查，命中阈值则触发告警。

        检查产生侧统一走共享分类：
        1) 先按历史口径写两类来源事件之一（事件类型与 payload 字段不变）；
        2) 再登记同口径的检查回执（保留原有字段），治理质量统计明细与
           治理报告均从该回执/事件复算。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "监控规则不存在"
        if entry.get("status") != "已启用":
            return None, f"监控规则状态为{entry.get('status')}，不可执行检查"

        table_name = entry.get("目标湖表", "")
        metric = entry.get("监控指标", "")
        threshold = entry.get("告警阈值", "")

        # 模拟检查：统计目标湖表的数据行数
        lake_table_rows = store.find_by_field("lake_table", "表名", table_name)
        row_count = len(lake_table_rows)

        # 阈值比较
        try:
            threshold_value = int(threshold) if threshold.isdigit() else 0
        except (ValueError, AttributeError):
            threshold_value = 0

        passed = row_count >= threshold_value
        # 统一分类：类别与来源事件类型都来自共享实现
        category = category_from_passed(passed)
        source_event_type = EVENT_TYPE_BY_CATEGORY[category]
        checked_at = datetime.datetime.now().isoformat()

        # 质量不通过时联动血缘，通过时联动治理报告（沿用历史事件口径）
        if not passed:
            event = store.log_event(source_event_type, QUALITY_MODULE, "lineage_graph", {
                "rule_id": entry_id,
                "table_name": table_name,
                "metric": metric,
                "threshold": threshold,
                "actual": row_count,
            })

            # 更新血缘链路为待验证
            lineages = store.find_by_field("lineage_graph", "中间转换", table_name)
            if not lineages:
                lineages = store.find_by_field("lineage_graph", "下游消费", table_name)
            for lin in lineages:
                lin["status"] = "待验证"
                lin["abnormal"] = True
        else:
            event = store.log_event(source_event_type, QUALITY_MODULE, "govern_report", {
                "rule_id": entry_id,
                "table_name": table_name,
                "metric": metric,
                "actual": row_count,
            })

        # 检查回执：原有字段保持不变，追加台账/分类定位字段
        receipt = {
            "rule_id": entry_id,
            "rule_name": entry.get("规则名称"),
            "table_name": table_name,
            "metric": metric,
            "threshold": threshold,
            "actual_value": row_count,
            "passed": passed,
            "checked_at": checked_at,
            # 以下为收拢后的统一分类与追踪字段，不改动原有字段
            "category": category,
            "source_event_type": source_event_type,
            "event_id": event["id"],
        }
        store.append_quality_receipt(receipt)

        return entry, (
            f"质量检查完成：{metric}={row_count}，阈值={threshold}，{category}"
        )

    def list_check_receipts(self) -> list[dict[str, Any]]:
        """查询湖内质量监控提供的全部检查回执（治理质量统计明细来源）。"""
        return store.quality_receipts()

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启用监控":
            return self.enable_monitor(entry_id)
        return super().run_action(entry_id, action)
