"""缓存退役证据批次领域服务：汇总来源下线记录与结果缓存明细，归档缓存证据解释生命周期变更。

跨模块数据流：
- 来源下线 → 生成证据批次（汇总中），实际引用该来源的有效缓存条目进入待完成组
- 归档批次 → 待完成条目逐条过期并转入已确认组，记录实际过期状态；中断时待完成组可追溯
- 同一轮恢复确认 → 与进行中的清理冲突，批次标记异常并保留争议条目清单
- 契约校验 → 消费已归档批次核对来源覆盖，把违约/恢复回写到双方检查清单
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

RETIRE_ACTION = "来源下线"
RECOVER_ACTION = "来源恢复"
FINAL_STATUSES = ("已归档", "已消费")
MODULE = "cache_retire_batch"


class CacheRetireBatchService(BaseModuleService):
    MODULE = MODULE
    ENTITY = "证据批次"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["批次编号", "来源编号", "触发动作", "涉及条目数", "已确认条目", "待完成条目", "契约回写", "归档时间"]
    REQUIRED_FIELDS = ["批次编号", "来源编号"]
    STATUS_ORDER = ["汇总中", "已归档", "已消费", "已作废"]
    ACTION_RULES = {"归档批次": "已归档", "核销批次": "已消费", "作废批次": "已作废"}
    NEGATIVE_ACTIONS = ["作废批次"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """归档批次必须走真实的条目过期流程，不能只翻状态。"""
        if action == "归档批次":
            result = finalize_retire_batch(entry_id)
            return store.find(MODULE, entry_id), result["message"]
        return super().run_action(entry_id, action)


# ---------- 引用匹配：标识符级，查询号相近的条目不误伤 ----------

def _contains_token(text: str, token: str) -> bool:
    """token 必须作为独立标识出现：SOUR-0001 命中「on SOUR-0001 join」，不命中 SOUR-00011。"""
    if not text or not token:
        return False
    pattern = rf"(?<![0-9A-Za-z_-]){re.escape(token)}(?![0-9A-Za-z_-])"
    return re.search(pattern, text) is not None


def entry_references_source(entry: dict[str, Any], source_id: int) -> bool:
    """判断缓存条目是否实际引用了指定来源（直接引用源编号，或经由OLAP立方）。"""
    summary = str(entry.get("查询摘要", ""))
    for s in store.rows("source_registry"):
        if int(s.get("id", 0)) == source_id and _contains_token(summary, str(s.get("源编号", ""))):
            return True
    for c in store.rows("olap_cube"):
        if _contains_token(str(c.get("数据来源", "")), str(source_id)) and _contains_token(summary, f"cube_{c.get('id')}"):
            return True
    return False


# ---------- 批次生命周期 ----------

def create_retire_batch(source_id: int, finalize: bool = True) -> dict[str, Any]:
    """来源下线：汇总实际引用该来源的有效缓存条目，生成退役证据批次。

    finalize=False 时批次停留在「汇总中」，模拟单边变更中断：
    待完成条目保留在证据组里，可经 finalize_retire_batch 补完。
    """
    source = store.find("source_registry", source_id)
    if source is None:
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}
    open_batch = find_open_retire_batch(source_id)
    if open_batch is not None:
        return {"ok": False, "batch_id": open_batch["id"],
                "message": f"来源{source.get('源编号')}存在未归档的退役批次{open_batch.get('批次编号')}，请先归档或作废"}

    # 来源下线记录：与缓存明细一起进批次
    source["status"] = "已停用"
    source["接入状态"] = "已下线"
    source["pending"] = True

    candidates = [
        e for e in store.rows("result_cache")
        if e.get("status") == "有效" and entry_references_source(e, source_id)
    ]
    batch = _new_batch(source, RETIRE_ACTION)
    batch["涉及条目数"] = str(len(candidates))
    batch["evidence"]["pending"] = [
        {"cache_id": e["id"], "条目编号": e.get("条目编号"), "查询摘要": e.get("查询摘要", ""), "原因": "待归档"}
        for e in candidates
    ]
    batch["待完成条目"] = _join_nos(batch["evidence"]["pending"])
    _write_batch_audit("retire_open", batch, len(candidates))

    if finalize:
        return finalize_retire_batch(batch["id"])
    return {
        "ok": True,
        "batch_id": batch["id"],
        "批次编号": batch["批次编号"],
        "status": batch["status"],
        "pending_entries": [i["cache_id"] for i in batch["evidence"]["pending"]],
        "message": f"退役批次{batch['批次编号']}已汇总，{len(candidates)}条缓存待归档",
    }


def finalize_retire_batch(batch_id: int) -> dict[str, Any]:
    """归档退役批次：待完成条目逐条过期并转入已确认组，记录每条实际过期状态。"""
    batch = store.find(MODULE, batch_id)
    if batch is None:
        return {"ok": False, "message": f"证据批次 {batch_id} 不存在或已归档"}
    if batch.get("触发动作") != RETIRE_ACTION:
        return {"ok": False, "message": f"批次{batch.get('批次编号')}是恢复批次，无需归档流程"}
    if batch.get("status") != "汇总中":
        return {"ok": False, "batch_id": batch_id, "status": batch.get("status"),
                "message": f"批次{batch.get('批次编号')}已{batch.get('status')}，不重复归档"}

    confirmed = list(batch["evidence"]["confirmed"])
    still_pending: list[dict[str, Any]] = []
    for item in batch["evidence"]["pending"]:
        entry = store.find("result_cache", int(item["cache_id"]))
        if entry is None:
            item["原因"] = "条目缺失"
            still_pending.append(item)
            continue
        if entry.get("status") == "有效":
            entry["status"] = "已过期"
            entry["pending"] = False
        confirmed.append({
            "cache_id": entry["id"],
            "条目编号": entry.get("条目编号"),
            "查询摘要": entry.get("查询摘要", ""),
            "实际状态": entry.get("status"),
            "确认时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
    batch["evidence"]["confirmed"] = confirmed
    batch["evidence"]["pending"] = still_pending
    batch["已确认条目"] = _join_nos(confirmed)
    batch["待完成条目"] = _join_nos(still_pending)
    batch["归档时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
    batch["status"] = "已归档"
    batch["pending"] = False
    _write_batch_audit("retire_finalize", batch, len(confirmed))
    return {
        "ok": True,
        "batch_id": batch_id,
        "批次编号": batch["批次编号"],
        "status": batch["status"],
        "confirmed_entries": [i["cache_id"] for i in confirmed],
        "unfinished_entries": [i["cache_id"] for i in still_pending],
        "message": f"批次{batch['批次编号']}已归档，确认{len(confirmed)}条缓存过期",
    }


def recover_source(source_id: int) -> dict[str, Any]:
    """来源恢复确认：同一轮存在未归档清理则冲突；否则按最近退役批次恢复缓存条目。"""
    source = store.find("source_registry", source_id)
    if source is None:
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    open_batch = find_open_retire_batch(source_id)
    if open_batch is not None:
        contested = [int(i["cache_id"]) for i in
                     open_batch["evidence"]["confirmed"] + open_batch["evidence"]["pending"]]
        open_batch["abnormal"] = True
        open_batch["conflict"] = f"恢复确认与进行中的清理冲突，争议条目{contested}"
        _write_batch_audit("recover_conflict", open_batch, len(contested))
        return {
            "ok": False,
            "conflict": True,
            "batch_id": open_batch["id"],
            "contested_cache_ids": contested,
            "message": f"退役批次{open_batch.get('批次编号')}尚未归档，恢复确认与清理对同一份缓存记录冲突",
        }

    retire = _latest_final_batch(source_id, RETIRE_ACTION)
    if retire is None:
        return {"ok": False, "message": f"来源{source.get('源编号')}没有已归档的退役批次，无需恢复"}
    recovery = _latest_final_batch(source_id, RECOVER_ACTION)
    if recovery is not None and int(recovery["id"]) > int(retire["id"]):
        return {"ok": False, "message": f"批次{retire.get('批次编号')}已被批次{recovery.get('批次编号')}恢复，不重复执行"}

    batch = _new_batch(source, RECOVER_ACTION)
    restored: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for item in retire["evidence"]["confirmed"]:
        entry = store.find("result_cache", int(item["cache_id"]))
        if entry is None:
            blocked.append({**item, "原因": "条目缺失"})
            continue
        if entry.get("status") == "已过期":
            entry["status"] = "有效"
            entry["pending"] = False
            restored.append({
                "cache_id": entry["id"],
                "条目编号": entry.get("条目编号"),
                "查询摘要": entry.get("查询摘要", ""),
                "实际状态": "有效",
                "确认时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
        else:
            blocked.append({"cache_id": entry["id"], "条目编号": entry.get("条目编号"),
                            "查询摘要": entry.get("查询摘要", ""),
                            "原因": f"状态{entry.get('status')}不可恢复"})
    batch["evidence"] = {"confirmed": restored, "pending": blocked}
    batch["涉及条目数"] = str(len(restored) + len(blocked))
    batch["已确认条目"] = _join_nos(restored)
    batch["待完成条目"] = _join_nos(blocked)
    batch["归档时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
    batch["status"] = "已归档"
    batch["pending"] = False

    source["status"] = "已注册"
    source["接入状态"] = "已接入"
    source["pending"] = True
    source["abnormal"] = False

    _write_batch_audit("recover_apply", batch, len(restored))
    return {
        "ok": True,
        "batch_id": batch["id"],
        "批次编号": batch["批次编号"],
        "recovered_cache_ids": [i["cache_id"] for i in restored],
        "blocked": blocked,
        "message": f"来源{source.get('源编号')}已恢复，{len(restored)}条缓存重新有效",
    }


# ---------- 核对与不变量 ----------

def retired_cache_ids() -> set[int]:
    """最新已归档/已消费批次确认为下线的缓存条目集合；恢复批次可解除。"""
    latest: dict[int, tuple[int, str]] = {}
    for b in store.rows(MODULE):
        if b.get("status") not in FINAL_STATUSES:
            continue
        for item in b.get("evidence", {}).get("confirmed", []):
            cid = int(item["cache_id"])
            if cid not in latest or int(b["id"]) > latest[cid][0]:
                latest[cid] = (int(b["id"]), str(b.get("触发动作", "")))
    return {cid for cid, (_, action) in latest.items() if action == RETIRE_ACTION}


def verify_batch(batch_id: int) -> dict[str, Any]:
    """核对证据批次：已确认条目逐条对账；已消费批次仍可核对，不受影响。"""
    batch = store.find(MODULE, batch_id)
    if batch is None:
        return {"ok": False, "message": f"证据批次 {batch_id} 不存在或已归档"}

    retired = retired_cache_ids()
    checks: list[dict[str, Any]] = []
    consistent = True
    for item in batch.get("evidence", {}).get("confirmed", []):
        cid = int(item["cache_id"])
        entry = store.find("result_cache", cid)
        if entry is None:
            checks.append({"cache_id": cid, "ok": False, "detail": "条目缺失"})
            consistent = False
            continue
        ok = entry.get("条目编号") == item.get("条目编号")
        detail = f"当前状态{entry.get('status')}"
        if batch.get("触发动作") == RETIRE_ACTION and batch.get("status") in FINAL_STATUSES:
            if cid in retired:
                if entry.get("status") == "有效":
                    ok = False
                    detail = "退役已完成但条目仍有效，违反互斥不变量"
                else:
                    detail = "已退役，与证据一致"
            else:
                detail = "已被后续恢复批次解除退役"
        elif batch.get("触发动作") == RECOVER_ACTION:
            if cid in retired:
                detail = "已被更新的退役批次再次下线"
            elif entry.get("status") != "有效":
                ok = False
                detail = "恢复批次确认有效但条目非有效"
            else:
                detail = "恢复有效，与证据一致"
        if not ok:
            consistent = False
        checks.append({"cache_id": cid, "条目编号": item.get("条目编号"), "ok": ok, "detail": detail})

    pending = [
        {"cache_id": int(i["cache_id"]), "条目编号": i.get("条目编号"), "原因": i.get("原因", "")}
        for i in batch.get("evidence", {}).get("pending", [])
    ]
    return {
        "ok": True,
        "consistent": consistent,
        "batch_id": batch_id,
        "批次编号": batch.get("批次编号"),
        "status": batch.get("status"),
        "checks": checks,
        "pending_entries": pending,
        "contract_checklist": batch.get("contract_checklist", []),
        "consumed_by": batch.get("consumed_by", []),
    }


def verify_retirement_invariant() -> dict[str, Any]:
    """全局互斥不变量：退役完成的条目不得对外有效读取，反之亦然。"""
    retired = retired_cache_ids()
    violations = []
    for e in store.rows("result_cache"):
        if int(e.get("id", 0)) in retired and e.get("status") == "有效":
            violations.append({"cache_id": e["id"], "条目编号": e.get("条目编号"),
                               "detail": "退役已完成但条目仍有效"})
    return {"ok": not violations, "violations": violations, "retired_count": len(retired)}


def unfinished_entries() -> list[dict[str, Any]]:
    """汇总所有未归档批次的待完成条目：单边变更中断后可追到未完成的条目。"""
    result = []
    for b in store.rows(MODULE):
        if b.get("status") == "汇总中":
            for item in b.get("evidence", {}).get("pending", []):
                result.append({"batch_id": b["id"], "批次编号": b.get("批次编号"), **item})
    return result


# ---------- 契约消费 ----------

def batches_for_source(source_id: int) -> list[dict[str, Any]]:
    rows = [b for b in store.rows(MODULE) if b.get("source_id") == source_id]
    return sorted(rows, key=lambda r: int(r.get("id", 0)))


def write_contract_verdict(batch_id: int, contract: dict[str, Any], verdict: str) -> None:
    """契约消费批次：回写检查清单并把批次推进为已消费（证据保留，仍可核对）。"""
    batch = store.find(MODULE, batch_id)
    if batch is None:
        return
    checklist = batch.setdefault("contract_checklist", [])
    checklist.append({
        "contract_id": contract["id"],
        "契约编号": contract.get("契约编号"),
        "结果": verdict,
        "核对时间": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    consumed = batch.setdefault("consumed_by", [])
    if contract["id"] not in consumed:
        consumed.append(contract["id"])
    batch["契约回写"] = "、".join(f"{c['契约编号']}:{c['结果']}" for c in checklist[-3:])
    if batch.get("status") == "已归档":
        batch["status"] = "已消费"
        batch["pending"] = False


# ---------- 内部辅助 ----------

def find_open_retire_batch(source_id: int) -> dict[str, Any] | None:
    found = None
    for b in store.rows(MODULE):
        if (b.get("source_id") == source_id and b.get("触发动作") == RETIRE_ACTION
                and b.get("status") == "汇总中"):
            found = b
    return found


def _latest_final_batch(source_id: int, action: str) -> dict[str, Any] | None:
    found = None
    for b in store.rows(MODULE):
        if b.get("source_id") == source_id and b.get("触发动作") == action and b.get("status") in FINAL_STATUSES:
            if found is None or int(b["id"]) > int(found["id"]):
                found = b
    return found


def _new_batch(source: dict[str, Any], action: str) -> dict[str, Any]:
    rows = store.rows(MODULE)
    batch = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "批次编号": f"RETI-{uuid.uuid4().hex[:8]}",
        "来源编号": source.get("源编号", ""),
        "触发动作": action,
        "涉及条目数": "0",
        "已确认条目": "-",
        "待完成条目": "-",
        "契约回写": "-",
        "归档时间": "-",
        "status": "汇总中",
        "pending": True,
        "abnormal": False,
        "source_id": source.get("id"),
        "round_id": uuid.uuid4().hex[:8],
        "evidence": {"confirmed": [], "pending": []},
        "contract_checklist": [],
        "consumed_by": [],
        "conflict": "",
    }
    rows.append(batch)
    return batch


def _join_nos(items: list[dict[str, Any]]) -> str:
    nos = [str(i.get("条目编号", "")) for i in items if i.get("条目编号")]
    return ", ".join(nos) if nos else "-"


def _write_batch_audit(action: str, batch: dict[str, Any], count: int) -> None:
    rows = store.rows("query_audit")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"cache_retire_batch.{action}(batch_id={batch.get('id')})",
        "执行耗时": "60ms",
        "扫描行数": str(count),
        "返回行数": str(count),
        "数据来源": "cache_retire_batch",
        "操作用户": "cache_manager",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    })
