"""缓存退役证据批次：来源下线/恢复的证据归档、同轮冲突、互斥不变量与契约回写。"""
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store
from app.services import cache_retire_batch as retire_batch
from app.services.source_registry import SourceRegistryService
from app.services.result_cache import ResultCacheService
from app.services.data_contract import DataContractService

client = TestClient(app)


@pytest.fixture()
def services():
    return {
        "source": SourceRegistryService(),
        "cache": ResultCacheService(),
        "contract": DataContractService(),
    }


def _make_source(svc: SourceRegistryService, no: str) -> dict:
    entry, msg = svc.register_source({
        "源编号": no, "源类型": "mysql", "连接地址": f"db://test/{no}"})
    assert entry is not None, msg
    return entry


def _make_cache(svc: ResultCacheService, no_suffix: str, summary: str) -> dict:
    entry, missing = svc.create_entry({
        "条目编号": f"RESU-T{no_suffix}", "查询摘要": summary, "结果哈希": f"h{no_suffix}",
        "缓存命中": "0", "过期时间": "2027-12-31 00:00:00", "存储大小": "64",
        "创建时间": "2026-11-21 00:00:00", "最后命中": "-"})
    assert entry is not None, missing
    assert entry["status"] == "有效"
    return entry


def _make_contract(svc: DataContractService, no_suffix: str, dataset: str) -> dict:
    entry, missing = svc.create_entry({
        "契约编号": f"DATA-T{no_suffix}", "数据集": dataset, "Schema约束": "id,name",
        "SLA定义": "每小时更新", "更新频率": "3600", "负责人": "tester",
        "消费者": "dashboard", "生效时间": "2026-11-21"})
    assert entry is not None, missing
    svc.run_action(entry["id"], "签署契约")
    assert entry["status"] == "已承诺"
    return entry


class TestRetireScope:
    def test_retire_only_archives_entries_referencing_source(self, services):
        """一次来源退役只归档实际引用它的条目，查询号相近的其他结果保持有效。"""
        src_a = _make_source(services["source"], "SOUR-RT-A")
        src_b = _make_source(services["source"], "SOUR-RT-B")
        e1 = _make_cache(services["cache"], "A1", "federated query on SOUR-RT-A join dims")
        e2 = _make_cache(services["cache"], "A2", "federated query on SOUR-RT-B join dims")
        e3 = _make_cache(services["cache"], "A3", "federated query on SOUR-RT-A9 join dims")
        e4 = _make_cache(services["cache"], "A4", "standalone aggregation without source")

        result = services["cache"].retire_source(src_a["id"])
        assert result["ok"], result["message"]
        batch = store.find("cache_retire_batch", result["batch_id"])
        assert batch["status"] == "已归档"
        assert batch["涉及条目数"] == "1"
        assert batch["已确认条目"] == e1["条目编号"]
        assert batch["待完成条目"] == "-"

        # 实际引用的条目过期，其余保持有效
        assert store.find("result_cache", e1["id"])["status"] == "已过期"
        for e in (e2, e3, e4):
            assert store.find("result_cache", e["id"])["status"] == "有效"

        # 证据组记录实际过期状态
        confirmed = batch["evidence"]["confirmed"]
        assert [i["cache_id"] for i in confirmed] == [e1["id"]]
        assert confirmed[0]["实际状态"] == "已过期"

        # 其他来源不参与切换
        assert store.find("source_registry", src_b["id"])["status"] == "已注册"
        assert retire_batch.batches_for_source(src_b["id"]) == []

    def test_unfinished_entries_traceable_when_interrupted(self, services):
        """已确认证据与实际过期状态成组存储；单边变更中断后可追到未完成条目。"""
        src = _make_source(services["source"], "SOUR-RT-C")
        e1 = _make_cache(services["cache"], "C1", "scan of SOUR-RT-C hourly")
        e2 = _make_cache(services["cache"], "C2", "scan of SOUR-RT-C daily")

        opened = services["cache"].retire_source(src["id"], finalize=False)
        assert opened["ok"]
        batch = store.find("cache_retire_batch", opened["batch_id"])
        assert batch["status"] == "汇总中"
        assert store.find("source_registry", src["id"])["status"] == "已停用"

        # 中断现场：条目尚未过期，待完成组可追溯
        assert store.find("result_cache", e1["id"])["status"] == "有效"
        pending_ids = {i["cache_id"] for i in services["cache"].unfinished_retire_entries()}
        assert {e1["id"], e2["id"]} <= pending_ids
        assert e1["条目编号"] in batch["待完成条目"]

        finalized = services["cache"].finalize_retire_batch(opened["batch_id"])
        assert finalized["ok"]
        assert set(finalized["confirmed_entries"]) == {e1["id"], e2["id"]}
        assert store.find("result_cache", e1["id"])["status"] == "已过期"
        assert store.find("result_cache", e2["id"])["status"] == "已过期"
        assert all(i["batch_id"] != opened["batch_id"]
                   for i in services["cache"].unfinished_retire_entries())

        # 重复归档幂等拒绝
        again = services["cache"].finalize_retire_batch(opened["batch_id"])
        assert not again["ok"]


class TestRecoverConflict:
    def test_same_round_recover_conflicts_with_cleanup(self, services):
        """来源下线的同一轮收到恢复确认：清理与恢复对同一份缓存记录冲突。"""
        src = _make_source(services["source"], "SOUR-RT-D")
        e1 = _make_cache(services["cache"], "D1", "federated query on SOUR-RT-D")

        opened = services["cache"].retire_source(src["id"], finalize=False)
        conflict = services["cache"].recover_source(src["id"])
        assert not conflict["ok"] and conflict["conflict"]
        assert conflict["contested_cache_ids"] == [e1["id"]]

        batch = store.find("cache_retire_batch", opened["batch_id"])
        assert batch["abnormal"] is True
        assert str(e1["id"]) in batch["conflict"] or str(e1["id"]) in str(batch["conflict"])

        # 冲突现场：清理未应用、恢复也未应用
        assert store.find("result_cache", e1["id"])["status"] == "有效"
        assert store.find("source_registry", src["id"])["status"] == "已停用"

        # 归档后新一轮恢复生效
        services["cache"].finalize_retire_batch(opened["batch_id"])
        assert store.find("result_cache", e1["id"])["status"] == "已过期"
        recovered = services["cache"].recover_source(src["id"])
        assert recovered["ok"], recovered["message"]
        assert recovered["recovered_cache_ids"] == [e1["id"]]
        assert store.find("result_cache", e1["id"])["status"] == "有效"
        assert store.find("source_registry", src["id"])["status"] == "已注册"

        # 重复恢复被拒绝
        again = services["cache"].recover_source(src["id"])
        assert not again["ok"]


class TestMutualExclusion:
    def test_valid_read_and_retirement_are_mutually_exclusive(self, services):
        """有效读取与退役完成不能同时对外成立；恢复后读取重新成立。"""
        src = _make_source(services["source"], "SOUR-RT-E")
        summary = "federated query on SOUR-RT-E join dims"
        e1 = _make_cache(services["cache"], "E1", summary)

        hit = services["cache"].get_cached_result(summary)
        assert hit is not None and hit["cache_id"] == e1["id"]

        services["cache"].retire_source(src["id"])
        assert services["cache"].get_cached_result(summary) is None

        # 外部把条目翻回有效 → 不变量报警，读取侧强制纠正
        store.find("result_cache", e1["id"])["status"] = "有效"
        invariant = services["cache"].verify_retirement_invariant()
        assert not invariant["ok"]
        assert any(v["cache_id"] == e1["id"] for v in invariant["violations"])
        assert services["cache"].get_cached_result(summary) is None
        assert store.find("result_cache", e1["id"])["status"] == "已过期"
        assert services["cache"].verify_retirement_invariant()["ok"]

        services["cache"].recover_source(src["id"])
        assert services["cache"].get_cached_result(summary) is not None


class TestContractEvidence:
    def test_contract_verifies_coverage_and_writes_checklist(self, services):
        """契约凭证据批次校验来源覆盖，把违约/恢复回写到检查清单；已消费批次仍能核对。"""
        src = _make_source(services["source"], "SOUR-RT-F")
        e1 = _make_cache(services["cache"], "F1", "federated query on SOUR-RT-F")
        contract = _make_contract(services["contract"], "F", "dataset_SOUR-RT-F")

        retired = services["cache"].retire_source(src["id"])
        batch_id = retired["batch_id"]

        verdict = services["contract"].verify_with_retire_evidence(contract["id"])
        assert verdict["ok"]
        assert verdict["status"] == "已违约"
        assert verdict["coverage_violations"] == []
        assert verdict["checklist"][0]["结果"] == "已违约"
        assert verdict["checklist"][0]["来源编号"] == "SOUR-RT-F"
        assert store.find("data_contract", contract["id"])["检查清单"] == verdict["checklist"]

        # 批次被消费：状态推进、契约回写，但证据仍可核对
        batch = store.find("cache_retire_batch", batch_id)
        assert batch["status"] == "已消费"
        assert "DATA-T" in batch["契约回写"]
        assert contract["id"] in batch["consumed_by"]
        checked = services["cache"].verify_batch(batch_id)
        assert checked["ok"] and checked["consistent"]
        assert checked["checks"][0]["cache_id"] == e1["id"]

        # 恢复后再次校验：恢复状态回写，契约回到已承诺
        services["cache"].recover_source(src["id"])
        verdict2 = services["contract"].verify_with_retire_evidence(contract["id"])
        assert verdict2["status"] == "已承诺"
        assert verdict2["checklist"][0]["结果"] == "已恢复"
        assert store.find("data_contract", contract["id"])["status"] == "已承诺"

        # 已消费的退役批次依旧可核对，并能看到条目已被后续恢复
        checked2 = services["cache"].verify_batch(batch_id)
        assert checked2["ok"] and checked2["consistent"]
        assert checked2["checks"][0]["detail"] == "已被后续恢复批次解除退役"

    def test_coverage_violation_when_expiration_has_no_evidence(self, services):
        """批次之外的静默过期被覆盖校验揪出，契约判违约。"""
        src = _make_source(services["source"], "SOUR-RT-G")
        e1 = _make_cache(services["cache"], "G1", "federated query on SOUR-RT-G")
        e2 = _make_cache(services["cache"], "G2", "aggregate scan of SOUR-RT-G")
        contract = _make_contract(services["contract"], "G", "dataset_SOUR-RT-G")

        # 单边变更：先把 e2 直接过期（不经过证据批次），再下线来源
        store.find("result_cache", e2["id"])["status"] = "已过期"
        services["cache"].retire_source(src["id"])

        verdict = services["contract"].verify_with_retire_evidence(contract["id"])
        assert verdict["status"] == "已违约"
        assert any(e2["条目编号"] in v for v in verdict["coverage_violations"])
        assert not any(e1["条目编号"] in v for v in verdict["coverage_violations"])

    def test_contract_without_evidence_is_rejected(self, services):
        src = _make_source(services["source"], "SOUR-RT-H")
        contract = _make_contract(services["contract"], "H", "dataset_SOUR-RT-H")
        verdict = services["contract"].verify_with_retire_evidence(contract["id"])
        assert not verdict["ok"]


class TestPipelineEndpoint:
    def test_module_registered_and_listed(self):
        resp = client.get("/api/cache_retire_batch")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["total"] >= 1
        resp = client.get("/api/overview")
        assert any(m["name"] == "cache_retire_batch" for m in resp.json()["modules"])

    def test_retire_recover_evidence_pipeline(self):
        resp = client.post("/api/pipelines/retire-recover-evidence",
                           params={"source_no": "SOUR-PIPE-1"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"]

        # 同轮恢复冲突，归档只覆盖实际引用的两条
        assert data["conflict"]["conflict"] is True
        assert set(data["finalized"]["confirmed_entries"]) == set(data["caches"]["target"])
        assert data["read_after_retire"] is None

        # 契约校验数据：先违约回写，恢复后再回写
        assert data["verdict_retire"]["status"] == "已违约"
        assert data["verdict_recover"]["status"] == "已承诺"
        assert data["recovered"]["ok"]
        assert data["read_after_recover"] is True

        # 对照来源与相近查询号不参与切换
        assert data["control_status"] == "已注册"
        assert data["control_cache_status"] == "有效"
        assert data["near_cache_status"] == "有效"
        assert data["invariant"]["ok"]
