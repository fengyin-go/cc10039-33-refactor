"""跨模块编排端点：暴露跨模块数据流链路、事件日志和快照信息。

提供 /api/cross/events, /api/cross/snapshots, /api/cross/pipeline-status 等端点，
让前端和治理报告可以消费跨模块链路数据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import service_for, SERVICES
from app.services.govern_report import GovernReportService
from app.quality_classification import (
    CATEGORIES,
    CATEGORY_FAILED,
    CATEGORY_PASSED,
    classify_receipt,
)
from app.store import store
from app.services.partition_policy import PartitionPolicyService
from app.services.lake_table import LakeTableService
from app.services.storage_tier import StorageTierService
from app.services.file_compaction import FileCompactionService
from app.services.retention_mgr import RetentionMgrService
from app.services.backfill_job import BackfillJobService

router = APIRouter(prefix="/api/cross", tags=["跨模块编排"])


@router.get("/events")
def list_cross_events(
    source: str | None = Query(default=None),
    target: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    """查询跨模块事件日志，支持按 source/target 筛选。"""
    if source and target:
        events = store.events_between(source, target)
    elif source or target:
        events = store.events_for_module(source or target or "")
    else:
        events = list(store._events)
    return {
        "total": len(events),
        "items": events[-limit:],
    }


@router.get("/snapshots")
def list_snapshots() -> dict[str, Any]:
    """查询所有模块快照记录。"""
    return {
        "total": len(store._snapshots),
        "keys": store.snapshot_keys(),
    }


@router.get("/overview")
def cross_module_overview() -> dict[str, Any]:
    """跨模块统计概览。"""
    return store.cross_module_stats()


@router.get("/pipeline-status")
def pipeline_status() -> dict[str, Any]:
    """汇总数据湖治理全链路状态。
    从分区策略 → 湖表注册 → 冷热分层 → 小文件合并 → 保留策略 → 治理报告。
    """
    # 分区策略概览
    policies = store.rows("partition_policy")
    active_policies = [p for p in policies if p.get("status") == "已生效"]

    # 湖表注册概览
    lake_tables = store.rows("lake_table")
    registered_tables = [lt for lt in lake_tables if lt.get("status") == "已注册"]

    # 冷热分层概览
    tiers = store.rows("storage_tier")
    active_tiers = [t for t in tiers if t.get("status") == "已生效"]

    # 小文件合并概览
    compactions = store.rows("file_compaction")
    pending_compactions = [c for c in compactions if c.get("status") == "待调度"]
    running_compactions = [c for c in compactions if c.get("status") == "执行中"]
    completed_compactions = [c for c in compactions if c.get("status") == "已完成"]

    # 保留策略概览
    retentions = store.rows("retention_mgr")
    active_retentions = [r for r in retentions if r.get("status") == "已生效"]

    # 回填任务概览
    backfills = store.rows("backfill_job")
    active_backfills = [b for b in backfills if b.get("status") in ("待调度", "回填中")]

    # 血缘链路概览
    lineages = store.rows("lineage_graph")
    broken_lineages = [l for l in lineages if l.get("status") == "链路断裂"]

    # 治理报告概览
    reports = store.rows("govern_report")

    # 跨模块事件
    cross_stats = store.cross_module_stats()

    return {
        "pipeline": {
            "partition_policies": {"total": len(policies), "active": len(active_policies)},
            "lake_tables": {"total": len(lake_tables), "registered": len(registered_tables)},
            "storage_tiers": {"total": len(tiers), "active": len(active_tiers)},
            "file_compactions": {
                "total": len(compactions),
                "pending": len(pending_compactions),
                "running": len(running_compactions),
                "completed": len(completed_compactions),
            },
            "retention_mgrs": {"total": len(retentions), "active": len(active_retentions)},
            "backfill_jobs": {"total": len(backfills), "active": len(active_backfills)},
            "lineage_graphs": {"total": len(lineages), "broken": len(broken_lineages)},
            "govern_reports": {"total": len(reports)},
        },
        "cross_module": cross_stats,
        "health": {
            "has_broken_lineages": len(broken_lineages) > 0,
            "has_active_backfills": len(active_backfills) > 0,
            "all_lake_tables_registered": len(registered_tables) == len(lake_tables),
        },
    }


def _receipt_buckets() -> dict[str, list[dict[str, Any]]]:
    """按统一类别与交付状态整理检查回执，供操作员核对。"""
    receipts = store.quality_receipts()
    buckets: dict[str, list[dict[str, Any]]] = {
        CATEGORY_PASSED: [],
        CATEGORY_FAILED: [],
    }
    for receipt in receipts:
        buckets[classify_receipt(receipt)].append(receipt)
    return buckets


@router.get("/quality-checks")
def list_quality_checks() -> dict[str, Any]:
    """质量检查回执核对视图：哪些事件已交给报表、哪些还在写入途中。

    - delivered=True：已随某个治理报告批次交付；
    - delivered=False：检查回执已写入，但尚未被任何报告批次读取（写入途中）。
    类别统一来自共享分类实现，通过/不通过两类与来源事件一一对应。
    """
    buckets = _receipt_buckets()
    delivered = [r for r in store.quality_receipts() if r.get("delivered")]
    in_flight = [r for r in store.quality_receipts() if not r.get("delivered")]
    return {
        "total": len(store.quality_receipts()),
        "categories": list(CATEGORIES),
        "by_category": {
            category: {
                "total": len(rows),
                "delivered": sum(1 for r in rows if r.get("delivered")),
                "in_flight": sum(1 for r in rows if not r.get("delivered")),
            }
            for category, rows in buckets.items()
        },
        "delivered": delivered,
        "in_flight": in_flight,
    }


@router.get("/quality-batches")
def list_quality_batches() -> dict[str, Any]:
    """列出治理报告的质量读取批次及其固定水位、交付状态。"""
    batches = store.quality_batches()
    return {
        "total": len(batches),
        "items": batches,
    }


@router.get("/quality-batches/{batch_id}/recompute")
def recompute_quality_batch(batch_id: str) -> dict[str, Any]:
    """对指定批次按同一批有明确来源的事件重新复算分类统计与明细。"""
    if store.get_quality_batch(batch_id) is None:
        raise HTTPException(status_code=404, detail=f"质量读取批次 {batch_id} 不存在")
    service = GovernReportService()
    snapshot = service.read_quality_batch(batch_id)
    return {
        "batch": snapshot["batch"],
        "stats": snapshot["stats"],
        "event_stats": snapshot["event_stats"],
        "receipts": snapshot["receipts"],
        "events": snapshot["events"],
    }