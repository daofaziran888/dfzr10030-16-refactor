"""数据血缘追踪 DAG 图服务。

血缘入口不再自行拼装转换节点的可用标记与输出目标：
``get_graph`` 通过同一版本的字段投影读取转换节点，并回读按关系版本
落库的节点关系核对记录，保证血缘页面与转换列表、拓扑详情口径一致。
血缘关系自身的登记与状态流转行为保持不变。
"""
from __future__ import annotations

from typing import Any

from app.services import node_projection as projection
from app.store import TABLE_LINEAGE_CHECKS, store

MODULE = "lineage"
REQUIRED_FIELDS = ["关系编号", "上游节点", "下游节点"]
STATUS_ORDER = ["草稿", "已记录", "已验证", "已归档"]
ACTION_RULES = {"记录关系": "已记录", "验证关系": "已验证", "归档关系": "已归档"}
NEGATIVE_ACTIONS: list[str] = []


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class LineageService:
    def get_graph(self, mode: str = "frozen") -> dict[str, Any]:
        """血缘读取：触发同一字段投影，并回查转换节点关系核对记录。"""
        read = projection.build_projection(mode=mode)

        # 血缘关系自身的 DAG 节点（保持既有行为）。
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            node = {
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("关系编号", "")),
                "status": str(row.get("status", STATUS_ORDER[0])),
            }
            for key, value in row.items():
                if key in ("id", "status", "pending", "abnormal") or key.startswith("_"):
                    continue
                node[key] = value
            nodes.append(node)
        for i in range(len(nodes) - 1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"], "label": "依赖"})

        return {
            "projection_version": read.projection_version,
            "projection_mode": read.mode,
            "nodes": nodes,
            "edges": edges,
            # 与转换列表/拓扑详情同一投影版本的核对结果。
            "transform_nodes": read.nodes,
            "checks": read.checks,
            "anomalies": read.anomalies,
            "relations": read.relations,
        }

    def get_checks(self, node_code: str | None = None) -> dict[str, Any]:
        """血缘服务读取依赖异常清单与节点关系核对记录。"""
        read = projection.build_projection(node_code=node_code)
        return {
            "projection_version": read.projection_version,
            "checks": read.checks,
            "anomalies": read.anomalies,
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        with store.transaction():
            rows = store.rows(MODULE)
            entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
            for key, value in values.items():
                if key not in entry and key not in REQUIRED_FIELDS:
                    entry[key] = value
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            rows.append(entry)
            return dict(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据血缘追踪可执行范围"
        with store.transaction():
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"血缘关系 {entry_id} 不存在或已归档"
            target = ACTION_RULES[action]
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            snapshot = dict(entry)
        return snapshot, f"血缘关系已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
