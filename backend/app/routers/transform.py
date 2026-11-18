"""采集转换管道 DAG 拓扑接口：返回节点、连线图结构。

列表、拓扑详情、血缘读取共用 node_projection 的版本化字段投影，
本路由只负责参数解析与出参包装，业务判断全部在 services 层。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.transform import TransformService

router = APIRouter(prefix="/api/transform", tags=["采集转换管道"])
service = TransformService()
LIST_FIELDS = ["节点编号", "输入来源", "输出目标", "转换表达式", "执行顺序", "依赖节点", "失败策略", "节点状态"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]
PROJECTION_MODES = ("frozen", "latest")


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


def _normalize_mode(mode: str) -> str:
    mode = (mode or "frozen").lower()
    if mode not in PROJECTION_MODES:
        raise HTTPException(status_code=400, detail=f"投影读取模式仅支持：{'、'.join(PROJECTION_MODES)}")
    return mode


@router.get("")
def get_graph(mode: str = "frozen") -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据（同一字段投影版本）。"""
    return service.get_graph(mode=_normalize_mode(mode))


@router.get("/projection")
def get_projection(mode: str = "frozen", node: str | None = None) -> dict[str, Any]:
    """完整字段投影：节点、关系字段记录、历史关系清单、依赖异常清单、核对记录。"""
    return service.get_projection(mode=_normalize_mode(mode), node_code=node)


@router.get("/export")
def export_entries() -> StreamingResponse:
    # 导出与关系记录读取共用同一投影事务，保证落库什么就导出什么。
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status",
                     "可用标记", "关系版本", "投影版本"])
    for node in nodes:
        writer.writerow([
            "node", node.get("id", ""), csv_cell(node.get("name", "")),
            "", "", "", node.get("status", ""),
            node.get("可用标记", ""), csv_cell(node.get("关系版本", "")),
            node.get("投影版本", ""),
        ])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from", ""), edge.get("to", ""),
                         edge.get("label", ""), "", "", "", ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=transform.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"转换节点 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="转换节点已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
