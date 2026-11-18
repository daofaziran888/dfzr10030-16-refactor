"""转换节点字段投影：列表、拓扑详情、血缘读取共用的版本化读取路径。

统一读取数据流（全部在 store.transaction() 同一事务内完成，保证落库一致性）：

1. 冻结读取集合：读取转换节点字段、输入来源、输出目标与上下游关联记录
   （依赖节点字段），形成本次读取的节点快照；
2. 整理读取集合：统一拼装节点名称、可用标记、输入来源、输出目标，
   字段投影与数据版本绑定（节点版本 + 字段投影版本）；
3. 生成转换节点关系字段记录及依赖异常清单；
4. 已停用节点按读取时的节点版本留档进历史关系清单（同一节点版本只留档一次）；
5. 血缘核对记录按关系版本幂等写入，血缘服务直接回查同一投影版本。

并发语义：读取期间节点又发生停用/启用更新时，支持两种等价策略——
- frozen（默认）：以读取开始时冻结的节点版本生成历史关系清单；
- latest：以提交时刻的最新节点版本重建当前读取结果。
两种策略下每条关系记录都在同一事务内由单行快照生成，
同一节点关系记录不可能混入两个可用标记版本。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.store import (
    TABLE_LINEAGE_CHECKS,
    TABLE_NODE_RELATIONS,
    TABLE_RELATION_HISTORY,
    store,
)

TRANSFORM_MODULE = "transform"

STATUS_ENABLED = "已启用"
STATUS_DISABLED = "已停用"

# 关系记录每条都必须保留的字段（验收回查口径）。
RELATION_FIELDS = ["节点编号", "可用标记", "输入来源", "输出目标", "关系版本"]

DEP_SPLIT_RE = re.compile(r"[,，、;/；\s]+")

# 清单类型：可执行清单 / 历史关系清单 / 待校验集合（不进前两份清单）。
LIST_EXECUTABLE = "executable"
LIST_HISTORY = "history"
LIST_PENDING = "pending"


@dataclass
class ProjectionRead:
    """一次字段投影读取的完整产物。"""

    projection_version: int
    mode: str
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)
    relations: list[dict[str, Any]] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)
    anomalies: list[dict[str, Any]] = field(default_factory=list)
    checks: list[dict[str, Any]] = field(default_factory=list)
    check_archive: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "projection_version": self.projection_version,
            "projection_mode": self.mode,
            "nodes": self.nodes,
            "edges": self.edges,
            "relations": self.relations,
            "history_relations": self.history,
            "anomalies": self.anomalies,
            "checks": self.checks,
            "check_archive": self.check_archive,
        }


def node_version(row: dict[str, Any]) -> int:
    """节点数据版本：初始为 1，每次状态流转 +1。"""
    try:
        return max(1, int(row.get("_node_version", 1)))
    except (TypeError, ValueError):
        return 1


def is_available(row: dict[str, Any]) -> bool:
    """统一可用标记：只有「已启用」节点可继续被引用。

    列表、拓扑详情、血缘读取都使用这同一个标记，
    修复过去一个页面显示已停用、另一个页面仍可引用的不一致。
    """
    return str(row.get("status", "")) == STATUS_ENABLED


def split_dependencies(value: Any) -> list[str]:
    """解析上下游关联记录：依赖节点字段支持逗号/顿号/分号/空白分隔。"""
    if value is None:
        return []
    return [part for part in DEP_SPLIT_RE.split(str(value).strip()) if part]


def relation_version_of(node_code: str, version: int) -> str:
    """关系版本与节点版本绑定：同一节点版本在任意次读取中保持稳定。"""
    return f"{node_code}#v{version}"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _snapshot_rows() -> list[dict[str, Any]]:
    # 浅拷贝即可：行内值都是不可变的字符串/数字/布尔。
    return [dict(row) for row in store.rows(TRANSFORM_MODULE)]


def _project_node(row: dict[str, Any], projection_version: int) -> dict[str, Any]:
    code = str(row.get("节点编号", ""))
    version = node_version(row)
    available = is_available(row)
    projected: dict[str, Any] = {
        "id": int(row.get("id", 0)),
        # 三个入口统一使用节点编号作为节点名称，不再各自拼装。
        "name": code,
        "status": str(row.get("status", "")),
    }
    for key, value in row.items():
        if key in ("id", "status", "pending", "abnormal") or key.startswith("_"):
            continue
        projected[key] = value
    projected["可用标记"] = available
    projected["节点版本"] = version
    projected["关系版本"] = relation_version_of(code, version)
    projected["投影版本"] = projection_version
    return projected


def _build_edges(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # 保持既有 DAG 连线行为：按读取顺序串联相邻节点。
    edges: list[dict[str, Any]] = []
    for index in range(len(nodes) - 1):
        edges.append({"from": nodes[index]["id"], "to": nodes[index + 1]["id"], "label": "依赖"})
    return edges


def _build_relation(row: dict[str, Any], projection_version: int) -> dict[str, Any]:
    code = str(row.get("节点编号", ""))
    version = node_version(row)
    available = is_available(row)
    if available:
        list_type = LIST_EXECUTABLE
    elif str(row.get("status", "")) == STATUS_DISABLED:
        list_type = LIST_HISTORY
    else:
        list_type = LIST_PENDING
    return {
        "节点编号": code,
        "节点版本": version,
        "可用标记": available,
        "输入来源": row.get("输入来源", ""),
        "输出目标": row.get("输出目标", ""),
        "依赖节点": split_dependencies(row.get("依赖节点")),
        "关系版本": relation_version_of(code, version),
        "投影版本": projection_version,
        "清单类型": list_type,
    }


def _build_anomalies(
    rows: list[dict[str, Any]],
    code_set: set[str],
    available_map: dict[str, bool],
    projection_version: int,
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    for row in rows:
        code = str(row.get("节点编号", ""))
        rel_version = relation_version_of(code, node_version(row))
        for dep in split_dependencies(row.get("依赖节点")):
            if dep not in code_set:
                kind = "依赖缺失"
                detail = f"依赖节点 {dep} 不存在于转换节点读取集合"
            elif not available_map.get(dep, False):
                kind = "依赖已停用"
                detail = f"依赖节点 {dep} 不可用（可用标记=False），不可继续引用"
            else:
                continue
            anomalies.append({
                "节点编号": code,
                "依赖节点": dep,
                "异常类型": kind,
                "异常说明": detail,
                "关系版本": rel_version,
                "投影版本": projection_version,
            })
    return anomalies


def _upsert(table_name: str, match: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """按 match 键幂等落库：存在则更新，不存在则追加。"""
    table = store.rows(table_name)
    for existing in table:
        if all(existing.get(key) == value for key, value in match.items()):
            existing.update(payload)
            return existing
    row_id = max((int(item.get("id", 0)) for item in table), default=0) + 1
    record = {"id": row_id, **match, **payload}
    table.append(record)
    return record


def _persist(read: ProjectionRead) -> None:
    """关系记录、历史留档、血缘核对记录在同一事务内落库。"""
    # 不变量：同一节点不能同时进入可执行清单与历史关系清单。
    executable_keys = {r["节点编号"] for r in read.relations if r["清单类型"] == LIST_EXECUTABLE}
    history_keys = {r["节点编号"] for r in read.relations if r["清单类型"] == LIST_HISTORY}
    assert not (executable_keys & history_keys), "同一节点混入可执行清单与历史关系清单"

    anomaly_map: dict[str, list[dict[str, Any]]] = {}
    for anomaly in read.anomalies:
        anomaly_map.setdefault(anomaly["节点编号"], []).append(anomaly)

    for relation in read.relations:
        code = relation["节点编号"]
        rel_version = relation["关系版本"]
        common = {
            "节点编号": code,
            "可用标记": relation["可用标记"],
            "输入来源": relation["输入来源"],
            "输出目标": relation["输出目标"],
            "关系版本": rel_version,
            "节点版本": relation["节点版本"],
        }
        # 1) 转换节点关系字段记录（按关系版本留痕，标记当前版本）。
        _upsert(
            TABLE_NODE_RELATIONS,
            {"节点编号": code, "关系版本": rel_version},
            {
                **common,
                "依赖节点": "、".join(relation["依赖节点"]),
                "清单类型": relation["清单类型"],
                "投影版本": read.projection_version,
                "是否当前": True,
                "更新时间": _now(),
            },
        )
        # 同一节点的旧版本关系记录与核对记录不再是当前版本。
        for stale in store.rows(TABLE_NODE_RELATIONS):
            if stale.get("节点编号") == code and stale.get("关系版本") != rel_version:
                stale["是否当前"] = False
        for stale in store.rows(TABLE_LINEAGE_CHECKS):
            if stale.get("节点编号") == code and stale.get("关系版本") != rel_version:
                stale["是否当前"] = False

        node_anomalies = anomaly_map.get(code, [])
        check_result = "依赖异常" if node_anomalies else "正常"
        check_summary = "；".join(item["异常说明"] for item in node_anomalies)
        # 2) 血缘服务按关系版本写入节点关系核对记录（幂等，历史版本留档可追溯）。
        _upsert(
            TABLE_LINEAGE_CHECKS,
            {"节点编号": code, "关系版本": rel_version},
            {
                **common,
                "关系编号": f"CHK-{code}-v{relation['节点版本']}",
                "核对结果": check_result,
                "异常摘要": check_summary,
                "异常数量": len(node_anomalies),
                "投影版本": read.projection_version,
                "核对状态": "已记录",
                "是否当前": True,
                "更新时间": _now(),
            },
        )

        # 3) 已停用节点：历史关系按读取时冻结的节点版本留档，只留档一次。
        if relation["清单类型"] == LIST_HISTORY:
            _upsert(
                TABLE_RELATION_HISTORY,
                {"节点编号": code, "节点版本": relation["节点版本"]},
                {
                    **common,
                    "依赖节点": "、".join(relation["依赖节点"]),
                    "清单类型": LIST_HISTORY,
                    "留档投影版本": read.projection_version,
                    "留档时间": _now(),
                },
            )

    # 回读落库结果，保证导出/血缘入口与本次读取完全一致。
    read.history = [dict(item) for item in store.rows(TABLE_RELATION_HISTORY)]
    all_checks = [dict(item) for item in store.rows(TABLE_LINEAGE_CHECKS)]
    # 血缘入口只回查每个节点当前关系版本的核对记录；历史版本保留在归档里。
    read.checks = [item for item in all_checks if item.get("是否当前", True)]
    read.check_archive = all_checks


def build_projection(mode: str = "frozen", node_code: str | None = None) -> ProjectionRead:
    """构建一次字段投影读取。

    mode:
      - frozen：以读取开始时冻结的节点版本生成全部产物（默认）；
      - latest：以提交时刻的最新节点版本重建当前读取结果。
    node_code: 仅回查单个节点时传入；依赖解析仍基于完整读取集合。
    """
    if mode not in ("frozen", "latest"):
        raise ValueError(f"不支持的投影读取模式：{mode}")

    with store.transaction():
        # 字段投影版本绑定数据版本：没有节点写入时，所有入口读到的版本号一致。
        projection_version = store.current_projection_version()
        rows = _snapshot_rows()
        if mode == "latest":
            # 提交时刻重新读取最新节点版本重建当前结果。
            rows = _snapshot_rows()

        code_set = {str(row.get("节点编号", "")) for row in rows}
        available_map = {code: False for code in code_set}
        for row in rows:
            available_map[str(row.get("节点编号", ""))] = is_available(row)

        projected_nodes = [_project_node(row, projection_version) for row in rows]
        edges = _build_edges(projected_nodes)
        relations = [_build_relation(row, projection_version) for row in rows]
        anomalies = _build_anomalies(rows, code_set, available_map, projection_version)

        read = ProjectionRead(
            projection_version=projection_version,
            mode=mode,
            nodes=projected_nodes,
            edges=edges,
            relations=relations,
            anomalies=anomalies,
        )
        _persist(read)

        if node_code is not None:
            read.nodes = [node for node in read.nodes if node.get("节点编号") == node_code]
            read.relations = [item for item in read.relations if item.get("节点编号") == node_code]
            read.anomalies = [item for item in read.anomalies if item.get("节点编号") == node_code]
            read.checks = [
                item for item in read.checks if item.get("节点编号") == node_code
            ]
            read.check_archive = [
                item for item in read.check_archive if item.get("节点编号") == node_code
            ]
            read.history = [
                item for item in read.history if item.get("节点编号") == node_code
            ]
        return read


def current_relations() -> list[dict[str, Any]]:
    """读取当前关系记录（只取每个节点的最新关系版本）。"""
    return [
        dict(row)
        for row in store.rows(TABLE_NODE_RELATIONS)
        if row.get("是否当前", True)
    ]


def history_relations() -> list[dict[str, Any]]:
    """读取历史关系留档。"""
    return [dict(row) for row in store.rows(TABLE_RELATION_HISTORY)]


def lineage_checks() -> list[dict[str, Any]]:
    """读取血缘核对记录（仅每个节点的当前关系版本）。"""
    return [
        dict(row)
        for row in store.rows(TABLE_LINEAGE_CHECKS)
        if row.get("是否当前", True)
    ]


def dependency_anomalies() -> list[dict[str, Any]]:
    """读取当前关系版本的依赖异常清单。"""
    latest_version = store.current_projection_version()
    checks = store.rows(TABLE_LINEAGE_CHECKS)
    return [
        {
            "节点编号": row.get("节点编号"),
            "关系版本": row.get("关系版本"),
            "异常摘要": row.get("异常摘要", ""),
            "异常数量": row.get("异常数量", 0),
            "投影版本": row.get("投影版本", latest_version),
        }
        for row in checks
        if row.get("是否当前", True) and row.get("异常数量", 0) > 0
    ]
