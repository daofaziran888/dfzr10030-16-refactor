"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

重构后仓库额外承担两件事：
1. 提供可重入事务锁（transaction），让节点读取、关系落库、关系导出共用同一提交边界；
2. 维护字段投影版本序列与内部表（节点关系记录、历史关系留档、血缘核对记录），
   内部表不进入 module_names/overview 的业务模块统计。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS

# 内部表：只服务于转换节点字段投影与血缘核对，不属于业务模块清单。
TABLE_NODE_RELATIONS = "transform_node_relations"
TABLE_RELATION_HISTORY = "transform_relation_history"
TABLE_LINEAGE_CHECKS = "lineage_checks"
INTERNAL_TABLES = (
    TABLE_NODE_RELATIONS,
    TABLE_RELATION_HISTORY,
    TABLE_LINEAGE_CHECKS,
)


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._internal: dict[str, list[dict[str, Any]]] = {
            name: [] for name in INTERNAL_TABLES
        }
        # 可重入锁：同一次读取内部嵌套调用 service/store 不会自锁。
        self._lock = threading.RLock()
        # 字段投影版本绑定数据版本：只有转换节点发生写入（登记/状态流转）时才 +1，
        # 两次写入之间的任意次列表、拓扑详情、血缘读取都回查同一投影版本。
        self._data_version = 1

    @contextmanager
    def transaction(self) -> Iterator["Store"]:
        """事务边界：进入即冻结写入时机，退出时统一提交（内存库即释放锁）。"""
        self._lock.acquire()
        try:
            yield self
        finally:
            self._lock.release()

    def bump_data_version(self) -> int:
        """写入事务提交前推进数据版本（即下一版字段投影版本）。"""
        with self._lock:
            self._data_version += 1
            return self._data_version

    def current_projection_version(self) -> int:
        return self._data_version

    def module_names(self) -> list[str]:
        # 内部表不计入业务模块。
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        if module in self._tables:
            return self._tables[module]
        return self._internal.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

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


store = Store()
