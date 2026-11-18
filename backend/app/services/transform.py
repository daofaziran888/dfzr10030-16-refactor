"""采集转换管道 DAG 图服务。

重构后不再自行拼装节点名称、可用标记与输入输出，而是统一走
``app.services.node_projection`` 的版本化字段投影读取路径，
保证列表（GET /api/transform）、拓扑详情（GET /api/transform/{id}）
与血缘读取使用同一节点可用标记、输出目标与字段投影版本。
"""
from __future__ import annotations

from typing import Any

from app.services import node_projection as projection
from app.store import store

MODULE = "transform"
REQUIRED_FIELDS = ["节点编号", "输入来源", "输出目标"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用节点": "已启用", "停用节点": "已停用"}
NEGATIVE_ACTIONS = ["停用节点"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class TransformService:
    # ---- 统一读取入口 -------------------------------------------------

    def get_graph(self, mode: str = "frozen") -> dict[str, Any]:
        """列表/拓扑：一次冻结投影，节点与边共享同一字段投影版本。"""
        read = projection.build_projection(mode=mode)
        result = read.as_dict()
        # 兼容既有前端契约：顶层仍为 nodes / edges。
        result["nodes"] = read.nodes
        result["edges"] = read.edges
        return result

    def get_projection(self, mode: str = "frozen", node_code: str | None = None) -> dict[str, Any]:
        """完整投影产物：关系记录、历史清单、异常清单、血缘核对记录。"""
        return projection.build_projection(mode=mode, node_code=node_code).as_dict()

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """拓扑详情：同样走字段投影，返回与列表/血缘一致的可用标记与版本。"""
        with store.transaction():
            raw = store.find(MODULE, entry_id)
            if raw is None:
                return None
            node_code = str(raw.get("节点编号", ""))
        read = projection.build_projection(node_code=node_code)
        if read.nodes:
            return read.nodes[0]
        return None

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        read = projection.build_projection()
        return read.nodes, read.edges

    # ---- 写入入口（状态流转在事务内提交并推进节点版本） --------------

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        with store.transaction():
            rows = store.rows(MODULE)
            entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
            # 登记时允许附带其余展示字段（转换表达式、依赖节点等）。
            for key, value in values.items():
                if key not in entry and key not in REQUIRED_FIELDS:
                    entry[key] = value
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            entry["_node_version"] = 1
            entry.setdefault("节点状态", STATUS_ORDER[0])
            rows.append(entry)
            # 节点写入提交即推进数据版本，后续读取绑定新的字段投影版本。
            store.bump_data_version()
            return dict(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集转换管道可执行范围"
        with store.transaction():
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"转换节点 {entry_id} 不存在或已归档"
            target = ACTION_RULES[action]
            # 停用/启用属于可用标记更新，推进节点数据版本；
            # 同一事务内完成状态、节点版本、数据版本切换，读路径不会读到半成品。
            entry["status"] = target
            entry["_node_version"] = projection.node_version(entry) + 1
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            entry["节点状态"] = target
            store.bump_data_version()
            snapshot = dict(entry)
        # 提交后用最新版本重建当前读取结果，回查即可看到新投影版本。
        return self.get_entry(entry_id) or snapshot, f"转换节点已{action}"
