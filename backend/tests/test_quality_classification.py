"""质量监控分类收拢与治理报告批次读取的验收测试。

覆盖验收点：
1. 通过/不通过两类与两类来源事件共用同一分类实现；
2. 湖内质量监控提供原有检查回执给治理质量统计明细，报告按同一批次明细
   计算通过数/失败数，报告输入输出字段不变；
3. 同一条规则的第二次检查迟到，在报告读取质检结果期间写入，不进本批次；
4. 操作员可核对哪些事件已交给报表、哪些还在写入途中；
5. 分类后的统计与明细可复算，已有事件用原字段仍可读取；
6. 读取中断后重试，从同一批有明确来源的事件重新生成；
7. 历史通过/不通过记录各取了哪些行可逐行核对，旧报告字段按原口径验算。

store 是全局单例，测试用带标记的自建数据并以差值（delta）方式断言，
避免依赖种子数据与其它测试的执行顺序。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.routers import cross_module
from app.services.govern_report import GovernReportService
from app.quality_classification import (
    CATEGORIES,
    CATEGORY_BY_EVENT_TYPE,
    CATEGORY_FAILED,
    CATEGORY_PASSED,
    EVENT_TYPE_BY_CATEGORY,
    SOURCE_EVENT_FAILED,
    SOURCE_EVENT_PASSED,
    classify_event,
    classify_receipt,
    category_from_passed,
    event_type_from_passed,
    is_quality_result_event,
    summarize,
)
from app.services.quality_monitor import QualityMonitorService
from app.store import store

MARK = "qc_collapse"


def _make_lake_table(name: str) -> dict:
    rows = store.rows("lake_table")
    entry = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "status": "已注册",
        "pending": False,
        "abnormal": False,
        "表名": name,
        "所属业务域": MARK,
        "存储格式": "parquet",
        "分区键": "dt",
        "当前分片数": 1,
    }
    rows.append(entry)
    return entry


def _make_enabled_rule(rule_name: str, table_name: str, threshold: str) -> dict:
    """创建并启用一条质检规则（跳过状态机，直接置为已启用）。"""
    rows = store.rows("quality_monitor")
    rule = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "status": "已启用",
        "pending": False,
        "abnormal": False,
        "规则名称": rule_name,
        "监控指标": f"{MARK}_metric",
        "告警阈值": threshold,
        "目标湖表": table_name,
        "检查频率": "1d",
    }
    rows.append(rule)
    return rule


def _make_report_entry(period: str) -> dict:
    rows = store.rows("govern_report")
    entry = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "status": "待生成",
        "pending": True,
        "abnormal": False,
        "报告周期": period,
        "治理指标": MARK,
        "优化建议": MARK,
        "执行状态": MARK,
        "审批状态": MARK,
    }
    rows.append(entry)
    return entry


def _receipts_for(rules: list[dict]) -> list[dict]:
    rule_ids = {r["id"] for r in rules}
    return [r for r in store.quality_receipts() if r["rule_id"] in rule_ids]


def _events_for(rules: list[dict]) -> list[dict]:
    rule_ids = {r["id"] for r in rules}
    return [
        e for e in store._events
        if is_quality_result_event(e)
        and e["payload"].get("rule_id") in rule_ids
    ]


class TestQualityClassificationShared:
    """共享分类实现的单元约束：两侧必须用同一套写法。"""

    def test_two_categories_and_two_source_events(self):
        assert CATEGORIES == (CATEGORY_PASSED, CATEGORY_FAILED)
        assert set(EVENT_TYPE_BY_CATEGORY) == set(CATEGORIES)
        assert EVENT_TYPE_BY_CATEGORY[CATEGORY_PASSED] == SOURCE_EVENT_PASSED
        assert EVENT_TYPE_BY_CATEGORY[CATEGORY_FAILED] == SOURCE_EVENT_FAILED
        # 双向映射互为逆映射
        for category, event_type in EVENT_TYPE_BY_CATEGORY.items():
            assert CATEGORY_BY_EVENT_TYPE[event_type] == category

    def test_producer_and_reader_share_one_implementation(self):
        # 产生侧：passed -> 类别/事件类型
        assert category_from_passed(True) == CATEGORY_PASSED
        assert category_from_passed(False) == CATEGORY_FAILED
        assert event_type_from_passed(True) == SOURCE_EVENT_PASSED
        assert event_type_from_passed(False) == SOURCE_EVENT_FAILED

        # 读取侧：事件/回执 -> 类别，与产生侧结论一致
        pass_event = {
            "source_module": "quality_monitor",
            "event_type": SOURCE_EVENT_PASSED,
        }
        fail_event = {
            "source_module": "quality_monitor",
            "event_type": SOURCE_EVENT_FAILED,
        }
        assert classify_event(pass_event) == CATEGORY_PASSED
        assert classify_event(fail_event) == CATEGORY_FAILED
        assert classify_receipt({"passed": True}) == CATEGORY_PASSED
        assert classify_receipt({"passed": False}) == CATEGORY_FAILED

        # 非质检结果事件不参与分类
        assert classify_event({"source_module": "ingest_pipeline",
                               "event_type": SOURCE_EVENT_PASSED}) is None
        assert is_quality_result_event(pass_event) is True
        assert is_quality_result_event(fail_event) is True

    def test_summarize_uses_historical_key_names(self):
        rows = [
            {"passed": True},
            {"passed": True},
            {"passed": False},
        ]
        stats = summarize(rows, classifier=classify_receipt)
        assert stats["quality_checks"] == 3
        assert stats["passes"] == 2
        assert stats["failures"] == 1
        assert stats["by_category"] == {CATEGORY_PASSED: 2, CATEGORY_FAILED: 1}


class TestQualityReceiptAndReportBatch:
    """端到端验收：回执 -> 批次明细 -> 报告计数 -> 复算/重试/核对。"""

    def setup_method(self):
        self.qual_svc = QualityMonitorService()
        self.gov_svc = GovernReportService()
        # 准备历史通过记录与不通过记录各一条（两个规则，各检查一次）
        self.pass_table = _make_lake_table(f"{MARK}_pass_table")
        self.fail_table = _make_lake_table(f"{MARK}_fail_table")
        # 通过规则：阈值 0，湖表存在 -> 行数 1 >= 0 -> 通过
        self.pass_rule = _make_enabled_rule(
            f"{MARK}_pass_rule", self.pass_table["表名"], "0")
        # 不通过规则：阈值 9，湖表只有 1 行 -> 1 < 9 -> 不通过
        self.fail_rule = _make_enabled_rule(
            f"{MARK}_fail_rule", self.fail_table["表名"], "9")

        self.rules = [self.pass_rule, self.fail_rule]
        self.receipts_before = len(_receipts_for(self.rules))

        _, pass_msg = self.qual_svc.check_quality(self.pass_rule["id"])
        _, fail_msg = self.qual_svc.check_quality(self.fail_rule["id"])
        assert "通过" in pass_msg and "不通过" not in pass_msg
        assert "不通过" in fail_msg

    def _history_receipts(self) -> list[dict]:
        return _receipts_for(self.rules)

    def _history_events(self) -> list[dict]:
        return _events_for(self.rules)

    def test_checks_produce_receipts_and_legacy_events(self):
        """历史通过/不通过记录：回执保留原字段，来源事件保持原口径。"""
        receipts = self._history_receipts()
        assert len(receipts) - self.receipts_before == 2

        pass_receipts = [r for r in receipts if classify_receipt(r) == CATEGORY_PASSED]
        fail_receipts = [r for r in receipts if classify_receipt(r) == CATEGORY_FAILED]
        assert len(pass_receipts) == 1
        assert len(fail_receipts) == 1
        pass_receipt, fail_receipt = pass_receipts[0], fail_receipts[0]

        # 回执原有字段仍可按原名读取
        for receipt in (pass_receipt, fail_receipt):
            for field in ("rule_id", "rule_name", "table_name", "metric",
                          "threshold", "actual_value", "passed", "checked_at"):
                assert field in receipt
        assert pass_receipt["passed"] is True
        assert fail_receipt["passed"] is False
        # 收拢后的统一分类字段与来源事件字段
        assert pass_receipt["category"] == CATEGORY_PASSED
        assert fail_receipt["category"] == CATEGORY_FAILED
        assert pass_receipt["source_event_type"] == SOURCE_EVENT_PASSED
        assert fail_receipt["source_event_type"] == SOURCE_EVENT_FAILED
        # 初始回执尚未交给任何报表
        assert pass_receipt["delivered"] is False
        assert fail_receipt["delivered"] is False

        # 已有事件用原字段仍可读取，类型/payload 与历史口径一致
        events = self._history_events()
        assert len(events) == 2
        pass_events = [e for e in events if classify_event(e) == CATEGORY_PASSED]
        fail_events = [e for e in events if classify_event(e) == CATEGORY_FAILED]
        assert len(pass_events) == 1 and len(fail_events) == 1
        pe, fe = pass_events[0], fail_events[0]
        assert pe["event_type"] == SOURCE_EVENT_PASSED
        assert pe["target_module"] == "govern_report"
        assert pe["payload"]["rule_id"] == self.pass_rule["id"]
        assert pe["payload"]["actual"] == pass_receipt["actual_value"]
        assert fe["event_type"] == SOURCE_EVENT_FAILED
        assert fe["target_module"] == "lineage_graph"
        assert fe["payload"]["threshold"] == fail_receipt["threshold"]
        # 回执与来源事件通过 event_id 一一对应
        assert pass_receipt["event_id"] == pe["id"]
        assert fail_receipt["event_id"] == fe["id"]

    def test_report_counts_use_same_batch_details_with_legacy_output(self):
        """报告按同一批次明细算通过/失败数，旧报告字段按原口径验算。"""
        report = _make_report_entry(f"{MARK}_period_1")
        entry, msg = self.gov_svc.generate_report(report["id"])
        assert entry is not None, msg
        assert entry["status"] == "已生成"

        # 报告条目字段仍是原字段
        assert entry["执行状态"] == "报告已生成"
        assert "优化建议" in entry

        # 从 govern_report_generated 事件里取回 report_data（输入输出不变）
        generated = [
            e for e in store._events
            if e["event_type"] == "govern_report_generated"
            and e["payload"].get("report_id") == report["id"]
        ]
        assert len(generated) == 1
        quality_block = generated[0]["payload"]["report_data"]["quality_monitoring"]
        # 旧口径：quality_checks/passes/failures 三个键名不变
        assert set(quality_block) == {"quality_checks", "failures", "passes"}

        # 逐行核对两类计数各取了哪些行（全局单例，按本测试规则过滤）
        receipts = self._history_receipts()
        events = self._history_events()
        rule_ids = {r["id"] for r in self.rules}
        passed_rows = [r for r in receipts if classify_receipt(r) == CATEGORY_PASSED]
        failed_rows = [r for r in receipts if classify_receipt(r) == CATEGORY_FAILED]
        assert len(passed_rows) == 1
        assert len(failed_rows) == 1
        assert passed_rows[0]["rule_id"] == self.pass_rule["id"]
        assert failed_rows[0]["rule_id"] == self.fail_rule["id"]

        # 报告全量计数 = 本批全量明细，且本测试的两条都在批次内
        delivered_batch_id = next(
            b["batch_id"] for b in store.quality_batches()
            if b["delivered_report_id"] == report["id"]
        )
        batch_receipts = store.receipts_for_quality_batch(delivered_batch_id)
        assert quality_block["quality_checks"] == len(batch_receipts)
        assert quality_block["passes"] == sum(
            1 for r in batch_receipts if classify_receipt(r) == CATEGORY_PASSED)
        assert quality_block["failures"] == sum(
            1 for r in batch_receipts if classify_receipt(r) == CATEGORY_FAILED)
        my_ids_in_batch = {r["receipt_id"] for r in batch_receipts
                           if r["rule_id"] in rule_ids}
        assert my_ids_in_batch == {r["receipt_id"] for r in receipts}

        # 用旧字段（原始事件 event_type）独立验算，结果必须一致
        legacy_passes = sum(1 for e in events if e["event_type"] == SOURCE_EVENT_PASSED)
        legacy_failures = sum(1 for e in events if e["event_type"] == SOURCE_EVENT_FAILED)
        assert legacy_passes == 1
        assert legacy_failures == 1
        batch_events = [
            e for e in store._events
            if is_quality_result_event(e)
            and e["id"] <= store.get_quality_batch(delivered_batch_id)["event_id_watermark"]
        ]
        assert sum(1 for e in batch_events
                   if e["event_type"] == SOURCE_EVENT_PASSED) == quality_block["passes"]
        assert sum(1 for e in batch_events
                   if e["event_type"] == SOURCE_EVENT_FAILED) == quality_block["failures"]

        # 本批回执已交给报表
        assert all(
            store.quality_receipts()[r["receipt_id"] - 1]["delivered"]
            for r in receipts
        )

    def test_late_second_check_excluded_then_included_by_next_report(self):
        """同一条规则的第二次检查迟到：读取期间写入不进本批，下一批再纳入。"""
        report1 = _make_report_entry(f"{MARK}_period_late_1")
        rule_ids = {r["id"] for r in self.rules}

        # 打开批次后、报告读取期间，再提交同一条规则的第二次检查（迟到）
        batch = self.gov_svc.open_quality_batch()

        def late_check() -> None:
            self.qual_svc.check_quality(self.fail_rule["id"])

        entry, _ = self.gov_svc.generate_report(
            report1["id"], batch_id=batch["batch_id"], on_quality_read=late_check)
        assert entry is not None

        receipts = self._history_receipts()
        events = self._history_events()
        # 迟到的第二次检查已经写入回执与事件
        assert len(receipts) == 3
        assert len(events) == 3

        snapshot1 = self.gov_svc.read_quality_batch(batch["batch_id"])
        # 本批次只读取水位内的回执，迟到检查被排除
        batch_mine = [r for r in snapshot1["receipts"] if r["rule_id"] in rule_ids]
        assert len(batch_mine) == 2
        assert sum(1 for r in batch_mine if classify_receipt(r) == CATEGORY_PASSED) == 1
        assert sum(1 for r in batch_mine if classify_receipt(r) == CATEGORY_FAILED) == 1
        batch_receipt_ids = {r["receipt_id"] for r in snapshot1["receipts"]}
        late_receipt = max(receipts, key=lambda r: r["receipt_id"])
        assert late_receipt["receipt_id"] not in batch_receipt_ids

        # 操作员核对：迟到回执仍在写入途中，前两条已交付
        overview = cross_module.list_quality_checks()
        delivered_ids = {r["receipt_id"] for r in overview["delivered"]}
        inflight_ids = {r["receipt_id"] for r in overview["in_flight"]}
        assert late_receipt["receipt_id"] in inflight_ids
        assert {r["receipt_id"] for r in batch_mine} <= delivered_ids
        assert late_receipt["receipt_id"] not in delivered_ids
        # 分类视图仍区分通过/不通过
        assert set(overview["categories"]) == set(CATEGORIES)
        my_inflight = [r for r in overview["in_flight"] if r["rule_id"] in rule_ids]
        assert all(classify_receipt(r) == CATEGORY_FAILED for r in my_inflight)

        # 下一份报告打开新批次，迟到检查被纳入：本测试规则共 3 条（1 通过 2 失败）
        report2 = _make_report_entry(f"{MARK}_period_late_2")
        entry2, _ = self.gov_svc.generate_report(report2["id"])
        assert entry2 is not None
        snapshot2_batch = store.quality_batches()[-1]["batch_id"]
        snapshot2 = self.gov_svc.read_quality_batch(snapshot2_batch)
        batch2_mine = [r for r in snapshot2["receipts"] if r["rule_id"] in rule_ids]
        assert len(batch2_mine) == 3
        assert sum(1 for r in batch2_mine if classify_receipt(r) == CATEGORY_PASSED) == 1
        assert sum(1 for r in batch2_mine if classify_receipt(r) == CATEGORY_FAILED) == 2
        # 迟到回执随新批次交付，本测试规则不再有写入途中的回执
        assert store.quality_receipts()[late_receipt["receipt_id"] - 1]["delivered"] is True
        assert all(
            r["rule_id"] not in rule_ids
            for r in cross_module.list_quality_checks()["in_flight"]
        )

    def test_retry_rebuilds_from_same_sourced_batch(self):
        """读取中断重试：同一批次 id 重新生成，结果与首次一致且交付幂等。"""
        report = _make_report_entry(f"{MARK}_period_retry")
        rule_ids = {r["id"] for r in self.rules}
        batch = self.gov_svc.open_quality_batch()

        # 模拟中断：批次已打开、水位已固定，但报告尚未生成（无交付）
        snapshot = self.gov_svc.read_quality_batch(batch["batch_id"])
        assert snapshot["batch"]["delivered"] is False
        initial_mine = [r for r in snapshot["receipts"] if r["rule_id"] in rule_ids]
        assert len(initial_mine) == 2

        # 中断期间又有新检查写入，重试同一批次时不得混入
        self.qual_svc.check_quality(self.pass_rule["id"])
        retried_entry, _ = self.gov_svc.generate_report(
            report["id"], batch_id=batch["batch_id"])
        assert retried_entry is not None

        retried = self.gov_svc.read_quality_batch(batch["batch_id"])
        retried_mine = [r for r in retried["receipts"] if r["rule_id"] in rule_ids]
        # 仍是水位内的两条历史回执，中断期间的新回执被排除
        assert len(retried_mine) == 2
        assert sum(1 for r in retried_mine if classify_receipt(r) == CATEGORY_PASSED) == 1
        assert sum(1 for r in retried_mine if classify_receipt(r) == CATEGORY_FAILED) == 1
        assert retried["batch"]["delivered"] is True
        assert retried["batch"]["delivered_report_id"] == report["id"]

        # 交付动作可重复执行，不改变批次口径
        store.mark_quality_batch_delivered(batch["batch_id"], report["id"])
        again = self.gov_svc.read_quality_batch(batch["batch_id"])
        assert again["stats"] == retried["stats"]

        # 端点级复算：回执侧与原始事件侧双路一致
        recompute = cross_module.recompute_quality_batch(batch["batch_id"])
        assert recompute["stats"] == recompute["event_stats"]
        # 明细行与统计可相互复算
        assert (
            sum(1 for r in recompute["receipts"]
                if classify_receipt(r) == CATEGORY_PASSED)
            == recompute["stats"]["passes"]
        )
        assert (
            sum(1 for r in recompute["receipts"]
                if classify_receipt(r) == CATEGORY_FAILED)
            == recompute["stats"]["failures"]
        )
        # 复算明细与本批来源事件一一对应（同 event_id 集合）
        assert {r["event_id"] for r in recompute["receipts"]} == {
            e["id"] for e in recompute["events"]
        }

    def test_unknown_batch_rejected(self):
        with pytest.raises(KeyError):
            self.gov_svc.read_quality_batch("qb-not-exist")
        entry, msg = self.gov_svc.generate_report(
            _make_report_entry(f"{MARK}_period_bad")["id"],
            batch_id="qb-not-exist",
        )
        assert entry is None
        assert "不存在" in msg
