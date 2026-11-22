"""治理报告领域服务：定期生成湖内治理报告和自动优化建议。

治理报告是跨模块数据流的最终汇聚点：汇总冷热分层迁移、小文件合并、
保留策略清理、质量监控结果和血缘链路状态，生成综合治理报告。

质量部分的口径：
- 湖内质量监控提供原有检查回执，作为治理质量统计明细；
- 报告生成时按「同一读取批次」的明细，用共享分类
  （app.quality_classification）计算通过数与失败数；
- 批次在读取开始时按质检结果事件 id 固定水位，读取中断后用同一批次
  重试仍从同一批有明确来源的事件重新生成，迟到的检查不会混入；
- 报告的输入输出字段保持历史口径不变。
"""
from __future__ import annotations

import datetime
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.quality_classification import (
    classify_event,
    classify_receipt,
    is_quality_result_event,
    summarize,
)
from app.store import store


class GovernReportService(BaseModuleService):
    MODULE = "govern_report"
    ENTITY = "治理报告"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["报告周期", "治理指标", "优化建议", "执行状态", "审批状态"]
    REQUIRED_FIELDS = ["报告周期", "治理指标", "优化建议"]
    STATUS_ORDER = ["待生成", "已生成", "已审批", "已执行"]
    ACTION_RULES = {"生成报告": "已生成", "执行优化": "已执行"}
    NEGATIVE_ACTIONS = []

    # ---- 质量读取批次（共享分类的报表读取侧） ----

    def open_quality_batch(self) -> dict[str, Any]:
        """打开一次质量结果读取批次，返回带水位的批次信息。"""
        return store.open_quality_batch()

    def read_quality_batch(self, batch_id: str) -> dict[str, Any]:
        """读取同一批次的治理质量统计明细并按统一类别复算。

        明细为该批次水位内的检查回执；统计同时用事件侧（原始 event_type
        字段）独立复算一遍，两条路径结果必须一致，保证可复算、可对账。
        """
        batch = store.get_quality_batch(batch_id)
        if batch is None:
            raise KeyError(f"质量读取批次不存在：{batch_id}")
        watermark = batch["event_id_watermark"]

        # 明细侧：湖内质量监控提供的检查回执
        receipts = store.receipts_for_quality_batch(batch_id)
        receipt_stats = summarize(receipts, classifier=classify_receipt)

        # 事件侧：同一批有明确来源的质检事件，按原 event_type 字段复算
        batch_events = [
            e for e in store._events
            if is_quality_result_event(e) and int(e["id"]) <= watermark
        ]
        event_stats = summarize(batch_events, classifier=classify_event)

        if (
            receipt_stats["quality_checks"] != event_stats["quality_checks"]
            or receipt_stats["passes"] != event_stats["passes"]
            or receipt_stats["failures"] != event_stats["failures"]
        ):
            raise RuntimeError(
                "质量明细与来源事件复算不一致："
                f"receipts={receipt_stats['by_category']}，"
                f"events={event_stats['by_category']}"
            )

        return {
            "batch": batch,
            "receipts": receipts,
            "events": batch_events,
            "stats": receipt_stats,
            "event_stats": event_stats,
        }

    def recompute_quality_batch(self, batch_id: str) -> dict[str, Any]:
        """对历史批次重新复算（不改变交付状态），供操作员核对。"""
        snapshot = self.read_quality_batch(batch_id)
        return {
            "batch_id": batch_id,
            "delivered": snapshot["batch"]["delivered"],
            "stats": snapshot["stats"],
            "receipt_ids": [r["receipt_id"] for r in snapshot["receipts"]],
            "event_ids": [e["id"] for e in snapshot["events"]],
        }

    def generate_report(
        self,
        entry_id: int,
        *,
        batch_id: str | None = None,
        on_quality_read: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """生成治理报告：跨模块聚合数据生成综合治理报告。
        这是整个系统的最终数据产物消费入口。

        :param batch_id: 读取中断重试时传入同一批次 id；为空则打开新批次。
        :param on_quality_read: 质量批次水位固定之后、分类统计之前的回调，
            用于模拟「报告读取质检结果期间同一条规则的第二次检查迟到写入」，
            验证迟到检查不会进入本批次。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "治理报告不存在"
        if entry.get("status") != "待生成":
            return None, f"治理报告状态为{entry.get('status')}，不可生成"

        # 1) 汇总冷热分层迁移
        tier_events = store.events_for_module("storage_tier")
        tier_migrations = [e for e in tier_events if e["event_type"] == "tier_migration_executed"]
        total_migrated = sum(
            e["payload"].get("migrated_partition_count", 0) for e in tier_migrations
        )

        # 2) 汇总小文件合并
        compaction_events = store.events_for_module("file_compaction")
        completed_compactions = [e for e in compaction_events if e["event_type"] == "compaction_completed"]

        # 3) 汇总保留策略清理
        retention_events = store.events_for_module("retention_mgr")
        completed_cleanups = [e for e in retention_events if e["event_type"] == "retention_cleanup_executed"]

        # 4) 汇总质量监控结果
        # 4.1 固定读取批次：重试沿用同一批次，水位不变
        if batch_id is None:
            batch = self.open_quality_batch()
            batch_id = batch["batch_id"]
        else:
            batch = store.get_quality_batch(batch_id)
            if batch is None:
                return None, f"质量读取批次 {batch_id} 不存在，无法重试生成"

        # 4.2 水位固定后允许迟到写入发生（回调），但不会被本批次读取
        if on_quality_read is not None:
            on_quality_read()

        # 4.3 按同一批次明细做统一分类统计（回执与来源事件双路复算）
        quality_snapshot = self.read_quality_batch(batch_id)
        quality_stats = quality_snapshot["stats"]

        # 5) 汇总血缘链路状态
        lineage_rows = store.rows("lineage_graph")
        broken_lineages = [l for l in lineage_rows if l.get("status") == "链路断裂"]
        uncertain_lineages = [l for l in lineage_rows if l.get("status") == "待验证"]

        # 6) 汇总回填任务
        backfill_events = store.events_for_module("backfill_job")
        completed_backfills = [e for e in backfill_events if e["event_type"] == "backfill_completed_to_govern"]

        # 组装报告（字段与历史口径完全一致）
        report_data = {
            "report_period": entry.get("报告周期", ""),
            "generated_at": datetime.datetime.now().isoformat(),
            "storage_governance": {
                "tier_migrations": len(tier_migrations),
                "total_partitions_migrated": total_migrated,
                "completed_compactions": len(completed_compactions),
                "completed_cleanups": len(completed_cleanups),
            },
            "quality_monitoring": {
                "quality_checks": quality_stats["quality_checks"],
                "failures": quality_stats["failures"],
                "passes": quality_stats["passes"],
            },
            "lineage_health": {
                "total_lineages": len(lineage_rows),
                "broken": len(broken_lineages),
                "uncertain": len(uncertain_lineages),
                "healthy": len(lineage_rows) - len(broken_lineages) - len(uncertain_lineages),
            },
            "backfill_summary": {
                "completed_backfills": len(completed_backfills),
            },
        }

        # 计算优化建议
        suggestions = []
        if len(broken_lineages) > 0:
            suggestions.append(f"修复 {len(broken_lineages)} 条断裂血缘链路")
        if quality_stats["failures"] > 0:
            suggestions.append(f"处理 {quality_stats['failures']} 个质量不达标指标")
        if total_migrated > 0:
            suggestions.append(f"冷热迁移有效，已迁移 {total_migrated} 个分区")
        if len(completed_compactions) > 0:
            suggestions.append(f"小文件合并完成 {len(completed_compactions)} 个任务")

        entry["status"] = "已生成"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["优化建议"] = "；".join(suggestions) if suggestions else "当前治理状态良好"
        entry["执行状态"] = "报告已生成"

        # 批次随报告交付：本批回执标记为已交给报表，写入途中的新回执仍可核对
        store.mark_quality_batch_delivered(batch_id, entry_id)

        store.log_event("govern_report_generated", "govern_report", "govern_report", {
            "report_id": entry_id,
            "report_data": report_data,
        })

        return entry, (
            f"治理报告已生成：迁移分区={total_migrated}，质量检查={quality_stats['quality_checks']}，"
            f"血缘链路={len(lineage_rows)}，回填任务={len(completed_backfills)}"
        )

    def execute_optimization(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行治理优化：根据报告建议触发实际的治理操作。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "治理报告不存在"
        if entry.get("status") not in ("已生成", "已审批"):
            return None, f"治理报告状态为{entry.get('status')}，不可执行优化"

        # 触发血缘链路修复
        broken_lineages = [
            l for l in store.rows("lineage_graph")
            if l.get("status") == "链路断裂"
        ]
        for lin in broken_lineages:
            lin["pending"] = True

        # 触发质量监控重新检查
        quality_rules = store.rows("quality_monitor")
        for qr in quality_rules:
            if qr.get("status") == "已启用":
                qr["pending"] = True

        entry["status"] = "已执行"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["执行状态"] = "优化已执行"

        store.log_event("govern_optimization_executed", "govern_report", "govern_report", {
            "report_id": entry_id,
            "broken_lineages_triggered": len(broken_lineages),
            "quality_rules_retriggered": len(quality_rules),
        })

        return entry, f"治理优化已执行：触发 {len(broken_lineages)} 条血缘修复、{len(quality_rules)} 个质量规则"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "生成报告":
            return self.generate_report(entry_id)
        if action == "执行优化":
            return self.execute_optimization(entry_id)
        return super().run_action(entry_id, action)
