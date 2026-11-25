"""请求/响应数据模型与入参校验。

业务规则
========
领号（进无网区前）：
- 终端标识 + 请求号唯一确定一次领号；数量 1~50；
- 同终端用原请求号、原数量重试 -> 幂等返回原号段；改数量 -> 409 拒绝。

补录（恢复联网后）：
- 携带终端标识与领号请求号，定位当时领到的号段；
- 工单号可乱序提交，但必须全部落在号段内；
- 同号同负责人重复提交视为成功（幂等）；
- 同号异负责人、段外编号 -> 报出单号，整批不入库。
"""

from __future__ import annotations

import re
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from .config import settings

# 工单号格式：前缀 + 正整数（不允许前导 0、负数、非数字）
ORDER_ID_RE = re.compile(
    r"^" + re.escape(settings.order_prefix) + r"[1-9][0-9]*$"
)


def is_valid_order_id(order_id: str) -> bool:
    return bool(ORDER_ID_RE.fullmatch(order_id or ""))


def make_order_id(no: int) -> str:
    return f"{settings.order_prefix}{no}"


class AllocateRequest(BaseModel):
    terminal_id: str = Field(
        ..., min_length=1, max_length=64, description="终端标识（如设备 SN）"
    )
    request_no: str = Field(
        ..., min_length=1, max_length=64, description="领号请求号，终端内唯一"
    )
    quantity: int = Field(
        ...,
        ge=settings.min_quantity,
        le=settings.max_quantity,
        description=f"领取数量（{settings.min_quantity}~{settings.max_quantity}）",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "terminal_id": "PDA-001",
                    "request_no": "REQ-20261125-001",
                    "quantity": 5,
                }
            ]
        }
    }


class AllocateResponse(BaseModel):
    terminal_id: str
    request_no: str
    quantity: int
    start_order_id: str = Field(..., description="号段起始工单号，含")
    end_order_id: str = Field(..., description="号段结束工单号，含")
    order_ids: List[str] = Field(..., description="号段内全部连续工单号")
    retried: bool = Field(
        ..., description="false=本次新领；true=同请求号重试，返回原号段"
    )


class BackfillItem(BaseModel):
    order_id: str = Field(
        ..., min_length=1, max_length=64, description="工单号，如 OFF-123"
    )
    assignee: str = Field(..., min_length=1, max_length=64, description="负责人")

    @field_validator("order_id")
    @classmethod
    def _check_format(cls, v: str) -> str:
        if not is_valid_order_id(v):
            raise ValueError(
                f"工单号 {v!r} 格式非法，应为 {settings.order_prefix}<正整数>"
            )
        return v


class BackfillRequest(BaseModel):
    terminal_id: str = Field(..., min_length=1, max_length=64, description="终端标识")
    request_no: str = Field(
        ..., min_length=1, max_length=64, description="领号时使用的请求号"
    )
    items: List[BackfillItem] = Field(
        ...,
        min_length=1,
        max_length=settings.max_quantity,
        description="本号段工单的补录条目，允许乱序",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "terminal_id": "PDA-001",
                    "request_no": "REQ-20261125-001",
                    "items": [
                        {"order_id": "OFF-3", "assignee": "alice"},
                        {"order_id": "OFF-1", "assignee": "bob"},
                        {"order_id": "OFF-2", "assignee": "alice"},
                    ],
                }
            ]
        }
    }


class BackfillResponse(BaseModel):
    terminal_id: str
    request_no: str
    received: int = Field(..., description="本次请求条目数（去重前）")
    inserted: int = Field(..., description="新写入工单数")
    already_present: int = Field(
        ..., description="同号同负责人、视为成功而跳过的工单数"
    )
    order_ids: List[str] = Field(..., description="本次涉及的全部工单号（升序）")
    message: str


class SegmentView(BaseModel):
    terminal_id: str
    request_no: str
    quantity: int
    start_order_id: str
    end_order_id: str
    created_at: str


class OrderView(BaseModel):
    order_id: str
    assignee: str


class BatchError(BaseModel):
    """整批拒绝时的错误体：报出全部问题单号，且保证整批不入库。"""

    error: str
    out_of_range_orders: List[str] = Field(default_factory=list)
    conflict_orders: List[str] = Field(default_factory=list)
    malformed_orders: Optional[List[str]] = None
    detail: str
