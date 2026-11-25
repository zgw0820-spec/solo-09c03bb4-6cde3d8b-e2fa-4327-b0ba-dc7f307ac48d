"""运行配置，全部来自环境变量，便于容器化。"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # PostgreSQL 连接串（psycopg 关键字参数形式或 dsn 均可）
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/patrol",
    )
    # 工单号前缀：完整工单号 = 前缀 + 正整数，如 OFF-123
    order_prefix: str = os.getenv("ORDER_PREFIX", "OFF-")
    # 单次领号数量上下限
    min_quantity: int = int(os.getenv("MIN_QUANTITY", "1"))
    max_quantity: int = int(os.getenv("MAX_QUANTITY", "50"))
    # 补录事务等锁上限（毫秒）：仅作用于同段并发补录，不同段互不阻塞
    lock_timeout_ms: int = int(os.getenv("LOCK_TIMEOUT_MS", "10000"))


settings = Settings()
