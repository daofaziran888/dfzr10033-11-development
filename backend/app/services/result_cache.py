"""结果缓存领域服务：查询结果缓存的生命周期管理、命中统计、失效检测与跨模块消费。"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services import cache_retire_batch as retire_batch
from app.services.base import BaseModuleService
from app.store import store


class ResultCacheService(BaseModuleService):
    MODULE = "result_cache"
    ENTITY = "缓存条目"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["条目编号", "查询摘要", "结果哈希", "缓存命中", "过期时间", "存储大小", "创建时间", "最后命中"]
    REQUIRED_FIELDS = ["条目编号", "查询摘要"]
    STATUS_ORDER = ["有效", "已过期", "已淘汰"]
    ACTION_RULES = {"主动刷新": "有效", "淘汰过期": "已淘汰"}
    NEGATIVE_ACTIONS = ["淘汰过期"]

    def invalidate_by_source(self, source_id: int) -> dict[str, Any]:
        """数据源变更时淘汰关联的 result_cache，通知下游消费者。"""
        entries = store.rows(self.MODULE)
        invalidated = []

        for entry in entries:
            if entry.get("status") != "有效":
                continue
            # 检查是否关联到该数据源
            summary = entry.get("查询摘要", "")
            related = False

            # 检查 source_registry 关联
            sources = store.rows("source_registry")
            for s in sources:
                if s.get("id") == source_id and str(s.get("源编号", "")) in summary:
                    related = True
                    break

            # 检查 olap_cube 关联
            if not related:
                cubes = store.rows("olap_cube")
                for c in cubes:
                    if str(source_id) in str(c.get("数据来源", "")) and f"cube_{c.get('id')}" in summary:
                        related = True
                        break

            if related:
                entry["status"] = "已过期"
                entry["pending"] = False
                invalidated.append({
                    "cache_id": entry["id"],
                    "条目编号": entry.get("条目编号"),
                    "摘要": summary,
                })

        # 更新数据契约状态（如果有消费者受影响）
        _update_contract_on_invalidation(source_id, len(invalidated))

        # 写入审计
        _write_cache_audit("invalidate", source_id, len(invalidated))

        return {
            "ok": True,
            "source_id": source_id,
            "invalidated_count": len(invalidated),
            "entries": invalidated[:10],
        }

    def retire_source(self, source_id: int, finalize: bool = True) -> dict[str, Any]:
        """来源退役：生成缓存退役证据批次，只归档实际引用该来源的有效条目。

        finalize=False 时批次停留在「汇总中」，模拟单边变更中断，
        待完成条目留在证据组里，可经 finalize_retire_batch 补完。
        """
        return retire_batch.create_retire_batch(source_id, finalize=finalize)

    def finalize_retire_batch(self, batch_id: int) -> dict[str, Any]:
        """归档退役批次：待完成条目逐条过期并转入已确认组。"""
        return retire_batch.finalize_retire_batch(batch_id)

    def recover_source(self, source_id: int) -> dict[str, Any]:
        """来源恢复确认：同一轮未归档清理与之冲突；否则按最近退役批次恢复。"""
        return retire_batch.recover_source(source_id)

    def verify_batch(self, batch_id: int) -> dict[str, Any]:
        """核对证据批次：已确认条目与实际状态对账；已消费批次仍可核对。"""
        return retire_batch.verify_batch(batch_id)

    def verify_retirement_invariant(self) -> dict[str, Any]:
        """互斥不变量：退役完成的条目不得对外有效读取。"""
        return retire_batch.verify_retirement_invariant()

    def unfinished_retire_entries(self) -> list[dict[str, Any]]:
        """单边变更中断后仍未完成归档的缓存条目。"""
        return retire_batch.unfinished_entries()

    def get_cached_result(self, query_fingerprint: str) -> dict[str, Any] | None:
        """按查询摘要查找缓存结果，更新命中计数。被 query_federation、data_exporter 调用。"""
        entries = store.rows(self.MODULE)
        for entry in entries:
            if entry.get("查询摘要") == query_fingerprint and entry.get("status") == "有效":
                # 检查是否过期
                expire_str = entry.get("过期时间", "")
                try:
                    expire_ts = time.mktime(time.strptime(expire_str, "%Y-%m-%d %H:%M:%S"))
                    if time.time() > expire_ts:
                        entry["status"] = "已过期"
                        entry["pending"] = False
                        continue
                except (ValueError, OverflowError):
                    pass

                # 互斥不变量：退役批次已归档的条目不得再对外提供有效读取
                if int(entry.get("id", 0)) in retire_batch.retired_cache_ids():
                    entry["status"] = "已过期"
                    entry["pending"] = False
                    continue

                # 更新命中统计
                entry["缓存命中"] = str(int(entry.get("缓存命中", "0")) + 1)
                entry["最后命中"] = time.strftime("%Y-%m-%d %H:%M:%S")

                # 返回缓存数据
                return {
                    "cache_id": entry["id"],
                    "条目编号": entry.get("条目编号"),
                    "data": entry.get("result_data", entry.get("cached_data", [])),
                    "hash": entry.get("结果哈希"),
                    "created": entry.get("创建时间"),
                    "hits": entry.get("缓存命中"),
                }

        return None

    def prewarm_cache(self, query_patterns: list[str]) -> dict[str, Any]:
        """缓存预热：读取 query_audit 高频查询，预计算结果并写入。"""
        results = []
        for pattern in query_patterns:
            # 模拟预热：生成结果并缓存
            fake_result = [{"pattern": pattern, "value": i} for i in range(1, 6)]
            entries = store.rows(self.MODULE)
            cache_id = max((int(r.get("id", 0)) for r in entries), default=0) + 1
            cache = {
                "id": cache_id,
                "条目编号": f"RESU-PW{uuid.uuid4().hex[:8]}",
                "查询摘要": f"prewarm_{pattern}",
                "结果哈希": hashlib.md5(str(fake_result).encode()).hexdigest()[:12],
                "缓存命中": "0",
                "过期时间": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 86400)
                ),
                "存储大小": str(len(str(fake_result))),
                "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "有效",
                "pending": True,
                "abnormal": False,
                "result_data": fake_result,
            }
            entries.append(cache)
            results.append({"pattern": pattern, "cache_id": cache_id})

        return {"ok": True, "prewarmed": len(results), "entries": results}

    def cache_hit_rate_report(self) -> dict[str, Any]:
        """生成缓存命中率报告，写入审计日志。"""
        entries = store.rows(self.MODULE)
        total = len(entries)
        valid = sum(1 for e in entries if e.get("status") == "有效")
        expired = sum(1 for e in entries if e.get("status") == "已过期")
        evicted = sum(1 for e in entries if e.get("status") == "已淘汰")

        total_hits = sum(int(e.get("缓存命中", "0")) for e in entries if e.get("缓存命中", "0").isdigit())

        report = {
            "total_entries": total,
            "valid": valid,
            "expired": expired,
            "evicted": evicted,
            "hit_rate": round(valid / max(total, 1), 2),
            "total_hits": total_hits,
        }

        _write_cache_audit("report", 0, total)

        return {"ok": True, "report": report}


def _update_contract_on_invalidation(source_id: int, invalidated_count: int) -> None:
    contracts = store.rows("data_contract")
    for c in contracts:
        if c.get("status") == "已承诺" and str(source_id) in str(c.get("数据集", "")):
            if invalidated_count > 5:
                c["status"] = "已违约"


def _write_cache_audit(action: str, entity_id: int, count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"result_cache.{action}(entity_id={entity_id})",
        "执行耗时": "50ms",
        "扫描行数": str(count),
        "返回行数": str(count),
        "数据来源": "result_cache",
        "操作用户": "cache_manager",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)