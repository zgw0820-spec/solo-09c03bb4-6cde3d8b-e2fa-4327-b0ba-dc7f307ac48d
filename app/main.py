"""FastAPI 入口：巡检终端离线领号与联网补录 API。"""

from __future__ import annotations

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from . import __version__
from .config import settings
from .db import (
    AllocationConflict,
    BackfillRejected,
    SegmentNotFound,
    allocate,
    backfill,
    ensure_business_schema,
    get_segment,
    list_orders,
    seed_orders,
)
from .models import (
    AllocateRequest,
    AllocateResponse,
    BackfillRequest,
    BackfillResponse,
    BatchError,
    SegmentView,
    is_valid_order_id,
)

app = FastAPI(
    title="巡检离线领号与补录 API",
    version=__version__,
    description=(
        "巡检终端进入无网区前先领取连续 OFF 工单号段，恢复联网后按号段"
        "乱序补录负责人。领号按（终端标识, 请求号）幂等重试，并发领号"
        "号段不重叠，重启不重号，自动避开旧库已用编号；补录整批校验，"
        "段外编号/同号异负责人报出单号且整批不入库，不同号段补录互不阻塞。"
    ),
)


@app.on_event("startup")
def _startup() -> None:
    ensure_business_schema()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "version": __version__}


@app.get("/orders", summary="查看当前工单及负责人（含种子与补录工单）")
def get_orders() -> dict:
    return {"orders": [{"order_id": o, "assignee": a} for o, a in list_orders()]}


@app.post("/orders/seed", summary="幂等写入普通编号测试工单（负责人置为 init）")
def post_seed(order_ids: list[str]) -> dict:
    clean = sorted({o.strip() for o in order_ids if o.strip()})
    if not clean:
        raise HTTPException(status_code=400, detail="order_ids 不能为空")
    protected = [o for o in clean if is_valid_order_id(o)]
    if protected:
        # OFF-n 是领号专用编号空间，种子接口只允许新增普通编号
        raise HTTPException(
            status_code=400,
            detail=(
                f"{protected} 为领号专用 {settings.order_prefix}<正整数> 编号，"
                "请通过 /allocate 领取、/backfill 补录；本接口只新增普通编号"
            ),
        )
    seed_orders(clean)
    return {"seeded": clean}


@app.post(
    "/allocate",
    response_model=AllocateResponse,
    summary="领号：领取一段连续 OFF 工单号（同请求号幂等重试）",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "新领号段": {
                            "summary": "终端 PDA-001 领取 5 个连续编号",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "quantity": 5,
                            },
                        },
                        "同请求号重试（返回原号段）": {
                            "summary": "原请求号原数量重试，retried=true",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "quantity": 5,
                            },
                        },
                        "改数量重试（预期 409）": {
                            "summary": "同请求号改数量，服务端拒绝",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "quantity": 6,
                            },
                        },
                    }
                }
            }
        }
    },
)
def post_allocate(
    req: AllocateRequest = Body(...),
) -> AllocateResponse:
    try:
        seg = allocate(req.terminal_id, req.request_no, req.quantity)
    except AllocationConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "quantity_mismatch",
                "terminal_id": exc.terminal_id,
                "request_no": exc.request_no,
                "requested_quantity": exc.requested,
                "existing_quantity": exc.existing,
                "message": str(exc),
            },
        ) from exc
    return AllocateResponse(
        terminal_id=seg.terminal_id,
        request_no=seg.request_no,
        quantity=seg.quantity,
        start_order_id=seg.start_order_id,
        end_order_id=seg.end_order_id,
        order_ids=seg.order_ids,
        retried=seg.retried,
    )


@app.get(
    "/segments/{terminal_id}/{request_no}",
    response_model=SegmentView,
    summary="查询某次领号得到的号段",
)
def get_segment_view(terminal_id: str, request_no: str) -> SegmentView:
    seg = get_segment(terminal_id, request_no)
    if seg is None:
        raise HTTPException(
            status_code=404,
            detail=f"终端 {terminal_id} 的请求号 {request_no} 未领取过号段",
        )
    return SegmentView(
        terminal_id=seg.terminal_id,
        request_no=seg.request_no,
        quantity=seg.quantity,
        start_order_id=seg.start_order_id,
        end_order_id=seg.end_order_id,
        created_at=seg.created_at,
    )


@app.post(
    "/backfill",
    response_model=BackfillResponse,
    responses={409: {"model": BatchError}, 404: {"description": "号段不存在"}},
    summary="补录：按领号号段乱序提交工单负责人（整批事务）",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "乱序补录": {
                            "summary": "号段内工单乱序提交，允许乱序",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "items": [
                                    {"order_id": "OFF-3", "assignee": "alice"},
                                    {"order_id": "OFF-1", "assignee": "bob"},
                                    {"order_id": "OFF-2", "assignee": "alice"},
                                ],
                            },
                        },
                        "同号同负责人重试（幂等成功）": {
                            "summary": "重复提交已存在的同号同负责人，inserted=0",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "items": [
                                    {"order_id": "OFF-1", "assignee": "bob"},
                                ],
                            },
                        },
                        "同号异负责人（预期 409 整批拒绝）": {
                            "summary": "OFF-1 已属 bob，改 carol 触发冲突",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "items": [
                                    {"order_id": "OFF-1", "assignee": "carol"},
                                    {"order_id": "OFF-4", "assignee": "alice"},
                                ],
                            },
                        },
                        "段外编号（预期 409 整批拒绝）": {
                            "summary": "OFF-999 不在号段内",
                            "value": {
                                "terminal_id": "PDA-001",
                                "request_no": "REQ-20261125-001",
                                "items": [
                                    {"order_id": "OFF-999", "assignee": "alice"},
                                ],
                            },
                        },
                    }
                }
            }
        }
    },
)
def post_backfill(
    req: BackfillRequest = Body(...),
) -> BackfillResponse:
    items = [(it.order_id, it.assignee) for it in req.items]
    try:
        inserted, already_present, order_ids = backfill(
            req.terminal_id, req.request_no, items
        )
    except SegmentNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BackfillRejected as exc:
        raise HTTPException(
            status_code=409,
            detail=BatchError(
                error="backfill_batch_rejected",
                out_of_range_orders=exc.out_of_range_orders,
                conflict_orders=exc.conflict_orders,
                detail=str(exc),
            ).model_dump(),
        ) from exc
    return BackfillResponse(
        terminal_id=req.terminal_id,
        request_no=req.request_no,
        received=len(items),
        inserted=inserted,
        already_present=already_present,
        order_ids=order_ids,
        message=(
            f"补录完成：新写入 {inserted} 单，同号同负责人幂等跳过 "
            f"{already_present} 单，整批已提交"
        ),
    )
