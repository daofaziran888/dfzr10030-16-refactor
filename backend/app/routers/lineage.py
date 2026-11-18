"""数据血缘追踪 DAG 拓扑接口：返回节点、连线图结构。

血缘读取复用转换节点字段投影的同一版本，并暴露按关系版本落库的
节点关系核对记录与依赖异常清单。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.lineage import LineageService

router = APIRouter(prefix="/api/lineage", tags=["数据血缘追踪"])
service = LineageService()
LIST_FIELDS = ["关系编号", "上游节点", "下游节点", "来源字段", "目标字段", "转换说明", "采集批次", "关系版本"]
STATUSES = ["草稿", "已记录", "已验证", "已归档"]
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
    """返回血缘节点、边及同一投影版本的转换节点核对数据。"""
    return service.get_graph(mode=_normalize_mode(mode))


@router.get("/checks")
def get_checks(node: str | None = None) -> dict[str, Any]:
    """血缘服务读取依赖异常清单，按关系版本回查节点关系核对记录。"""
    return service.get_checks(node_code=node)


@router.get("/export")
def export_entries() -> StreamingResponse:
    graph = service.get_graph()
    nodes, edges = graph["nodes"], graph["edges"]
    checks = graph.get("checks", [])
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id", ""), csv_cell(node.get("name", "")), "", "", "", node.get("status", "")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from", ""), edge.get("to", ""), edge.get("label", ""), ""])
    # 关系核对记录与 DAG 导出于同一读取事务，落库与导出口径一致。
    writer.writerow([])
    writer.writerow(["check", "节点编号", "可用标记", "输入来源", "输出目标", "关系版本", "核对结果", "投影版本"])
    for item in checks:
        writer.writerow([
            "check", csv_cell(item.get("节点编号", "")), item.get("可用标记", ""),
            csv_cell(item.get("输入来源", "")), csv_cell(item.get("输出目标", "")),
            csv_cell(item.get("关系版本", "")), item.get("核对结果", ""),
            item.get("投影版本", ""),
        ])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=lineage.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"血缘关系 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="血缘关系已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
