"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
支持跨模块查询、快照记录和流水日志，用于支撑跨模块数据流。
"""
from __future__ import annotations

import copy
import datetime
from typing import Any

from app.seed import SEED_ROWS
from app.quality_classification import (
    SOURCE_EVENT_FAILED,
    SOURCE_EVENT_PASSED,
)

# 质量监控两类来源事件（统一写法，分类解释收口在 quality_classification）
_QUALITY_RESULT_EVENT_TYPES = (SOURCE_EVENT_PASSED, SOURCE_EVENT_FAILED)


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块事件日志：记录所有跨模块操作
        self._events: list[dict[str, Any]] = []
        # 模块快照备份：用于检查点和恢复
        self._snapshots: dict[str, list[dict[str, Any]]] = {}
        # 湖内质量监控检查回执台账与读取批次（供治理质量统计明细复算）
        self._quality_receipts: list[dict[str, Any]] = []
        self._quality_batches: dict[str, dict[str, Any]] = {}
        self._quality_batch_seq = 0

    # ---- 基础读写 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by_field(self, module: str, field: str, value: str) -> list[dict[str, Any]]:
        """按指定字段值查找记录，支持跨模块关联查询。"""
        return [
            row for row in self.rows(module)
            if str(row.get(field, "")).casefold() == value.casefold()
        ]

    def upsert(self, module: str, entry: dict[str, Any]) -> dict[str, Any]:
        """插入或更新记录；存在相同 id 时覆盖，否则追加。"""
        rows = self.rows(module)
        entry_id = entry.get("id")
        for i, row in enumerate(rows):
            if row.get("id") == entry_id:
                rows[i] = entry
                return entry
        rows.append(entry)
        return entry

    # ---- 跨模块事件 ----

    def log_event(
        self, event_type: str, source_module: str, target_module: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """记录跨模块操作事件，供血缘和审计追溯。"""
        event = {
            "id": len(self._events) + 1,
            "event_type": event_type,
            "source_module": source_module,
            "target_module": target_module,
            "timestamp": datetime.datetime.now().isoformat(),
            "payload": payload,
        }
        self._events.append(event)
        return event

    def events_for_module(self, module: str) -> list[dict[str, Any]]:
        """查询与指定模块相关的所有事件。"""
        return [
            e for e in self._events
            if e["source_module"] == module or e["target_module"] == module
        ]

    def events_between(self, source: str, target: str) -> list[dict[str, Any]]:
        """查询两个模块之间的单向事件。"""
        return [
            e for e in self._events
            if e["source_module"] == source and e["target_module"] == target
        ]

    # ---- 质量检查回执与读取批次 ----

    def append_quality_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """登记一条质量检查回执，返回带稳定回执 id 的副本。

        回执的业务字段（rule_id/passed/actual_value 等）沿用检查产生侧的
        原有口径，只追加台账定位与交付追踪字段，已有字段保持可按原名读取。
        """
        stored = dict(receipt)
        stored["receipt_id"] = len(self._quality_receipts) + 1
        stored.setdefault("delivered", False)
        stored.setdefault("delivered_batch_id", None)
        self._quality_receipts.append(stored)
        return stored

    def quality_receipts(self) -> list[dict[str, Any]]:
        """全部检查回执（写入途中与已交付的都在此处）。"""
        return list(self._quality_receipts)

    def receipts_for_quality_batch(self, batch_id: str) -> list[dict[str, Any]]:
        """按批次水位读取同一批检查回执。

        水位为批次打开时已存在的最大质检结果事件 id；读取中断后重试，
        用同一批次 id 仍取到完全相同的一批回执，迟到检查不会混入。
        """
        batch = self._quality_batches.get(batch_id)
        if batch is None:
            return []
        watermark = batch["event_id_watermark"]
        return [
            dict(receipt)
            for receipt in self._quality_receipts
            if receipt.get("event_id") is not None
            and receipt["event_id"] <= watermark
        ]

    def open_quality_batch(self) -> dict[str, Any]:
        """打开一次治理报告读取批次，以当前质检结果事件 id 作为水位。"""
        self._quality_batch_seq += 1
        watermark = max(
            (
                int(e["id"])
                for e in self._events
                if e.get("source_module") == "quality_monitor"
                and e.get("event_type") in _QUALITY_RESULT_EVENT_TYPES
            ),
            default=0,
        )
        batch_id = f"qb-{self._quality_batch_seq:04d}"
        batch = {
            "batch_id": batch_id,
            "opened_at": datetime.datetime.now().isoformat(),
            "event_id_watermark": watermark,
            "delivered": False,
            "delivered_report_id": None,
            "delivered_at": None,
        }
        self._quality_batches[batch_id] = batch
        return dict(batch)

    def get_quality_batch(self, batch_id: str) -> dict[str, Any] | None:
        batch = self._quality_batches.get(batch_id)
        return dict(batch) if batch is not None else None

    def quality_batches(self) -> list[dict[str, Any]]:
        return [dict(batch) for batch in self._quality_batches.values()]

    def mark_quality_batch_delivered(self, batch_id: str, report_id: int) -> None:
        """批次随治理报告生成而交付，并把该批回执标记为已交付。

        标记动作可重试：重复交付同一批次结果保持一致，不会把写入途中的
        新回执计入历史批次。
        """
        batch = self._quality_batches.get(batch_id)
        if batch is None:
            raise KeyError(f"质量读取批次不存在：{batch_id}")
        watermark = batch["event_id_watermark"]
        for receipt in self._quality_receipts:
            if (
                receipt.get("event_id") is not None
                and receipt["event_id"] <= watermark
            ):
                receipt["delivered"] = True
                receipt["delivered_batch_id"] = batch_id
        batch["delivered"] = True
        batch["delivered_report_id"] = report_id
        batch["delivered_at"] = datetime.datetime.now().isoformat()

    # ---- 快照与检查点 ----

    def create_snapshot(self, module: str) -> str:
        """为指定模块创建深拷贝快照，返回快照标识。"""
        timestamp = datetime.datetime.now().isoformat()
        key = f"{module}@{timestamp}"
        self._snapshots[key] = copy.deepcopy(self.rows(module))
        return key

    def get_snapshot(self, key: str) -> list[dict[str, Any]] | None:
        """按标识获取快照副本。"""
        raw = self._snapshots.get(key)
        return copy.deepcopy(raw) if raw is not None else None

    def snapshot_keys(self) -> list[str]:
        return sorted(self._snapshots)

    # ---- 概览 ----

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    def cross_module_stats(self) -> dict[str, object]:
        """跨模块统计：事件数、快照数、关联模块对。"""
        pairs: set[tuple[str, str]] = set()
        for e in self._events:
            pairs.add((e["source_module"], e["target_module"]))
        return {
            "event_count": len(self._events),
            "snapshot_count": len(self._snapshots),
            "linked_pairs": len(pairs),
            "linked_modules": sorted(set(
                m for p in pairs for m in p
            )),
        }


store = Store()
