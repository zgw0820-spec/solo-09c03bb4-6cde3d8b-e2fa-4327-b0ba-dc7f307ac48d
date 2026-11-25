"""PostgreSQL 访问层：号段分配与补录入库。

并发与持久化保证
================
1. 号段分配
   - ``allocation_meta`` 单行计数器，分配事务内 ``SELECT ... FOR UPDATE``
     取号并推进，号段在数据库层面串行生成，**并发领号绝不重叠**；
   - 号段落在独立表 ``order_segments``，(terminal_id, request_no) 唯一约束，
     同终端同请求号重试直接回读原号段，改数量则抛 :class:`AllocationConflict`；
   - 计数器与号段均持久化，**服务/数据库重启后不会重发已领号码**；
   - 分配前扫描 ``work_orders`` 中已存在的 ``OFF-n``（旧库迁移数据、
     历史数据），计数器水位低于已用编号时整体跳过，**旧库已用编号必避开**。

2. 补录
   - 每笔补录是一个事务，先 ``SELECT ... FOR UPDATE`` 锁定本号段行：
     同段并发补录串行化，段间争用不同的行，**一段事务卡住不阻塞其他段**；
   - 段外编号、同号异负责人全部在写入前检出，任一存在即回滚，
     **整批不入库**；同号同负责人直接跳过，视为成功（幂等）。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg

from .config import settings
from .models import make_order_id

# 已领号段的编号前缀（与 models.ORDER_ID_RE 同源，SQL 正则转义用）
_PREFIX_PATTERN = "^" + re.escape(settings.order_prefix) + r"[0-9]+$"
_PREFIX_LIKE = f"{settings.order_prefix}%"
# SUBSTRING(x FROM int) 取该位置起的子串；必须是整数而非字符串模式
_PREFIX_CUT = len(settings.order_prefix) + 1


# --------------------------------------------------------------------------- #
# 业务异常
# --------------------------------------------------------------------------- #
class AllocationConflict(Exception):
    """同终端同请求号已领号，但本次重试数量与原数量不一致。"""

    def __init__(self, terminal_id: str, request_no: str,
                 requested: int, existing: int) -> None:
        self.terminal_id = terminal_id
        self.request_no = request_no
        self.requested = requested
        self.existing = existing
        super().__init__(
            f"终端 {terminal_id} 的请求号 {request_no} 已领取数量 {existing}，"
            f"不能以数量 {requested} 重复领取"
        )


class SegmentNotFound(Exception):
    """补录携带的终端标识/请求号找不到对应号段。"""

    def __init__(self, terminal_id: str, request_no: str) -> None:
        self.terminal_id = terminal_id
        self.request_no = request_no
        super().__init__(
            f"终端 {terminal_id} 的请求号 {request_no} 未领取过号段，"
            "请先调用领号接口"
        )


@dataclass
class BackfillRejected(Exception):
    """补录批次校验失败：报出全部问题单号，整批不入库。"""

    out_of_range_orders: List[str]
    conflict_orders: List[str]

    def __post_init__(self) -> None:
        parts = []
        if self.out_of_range_orders:
            parts.append(f"段外编号 {self.out_of_range_orders}")
        if self.conflict_orders:
            parts.append(f"同号异负责人 {self.conflict_orders}")
        super().__init__("；".join(parts))


# --------------------------------------------------------------------------- #
# 连接与建表
# --------------------------------------------------------------------------- #
def connect(autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(settings.database_url, autocommit=autocommit)


def ensure_business_schema() -> None:
    """建表并初始化分配计数器（幂等，可重复执行、可在重启后执行）。"""
    with connect(autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.work_orders (
                order_id   text PRIMARY KEY,
                assignee   text NOT NULL,
                source     text NOT NULL DEFAULT 'seed',
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.order_segments (
                terminal_id text NOT NULL,
                request_no  text NOT NULL,
                quantity    integer NOT NULL
                            CHECK (quantity BETWEEN {int(settings.min_quantity)}
                                                  AND {int(settings.max_quantity)}),
                start_no    bigint NOT NULL,
                end_no      bigint NOT NULL,
                created_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (terminal_id, request_no),
                CHECK (end_no - start_no + 1 = quantity)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.allocation_meta (
                id      smallint PRIMARY KEY DEFAULT 1,
                next_no bigint NOT NULL,
                CHECK (id = 1)
            )
            """
        )
        # 计数器初值 = 旧库/已领号段中最大 OFF 编号 + 1，保证重启不重号、
        # 旧库已用编号被避开。仅在 meta 尚无行时写入。
        conn.execute(
            """
            INSERT INTO public.allocation_meta(id, next_no)
            SELECT 1, GREATEST(
                (
                    SELECT COALESCE(MAX(SUBSTRING(order_id FROM %s)::bigint), 0) + 1
                    FROM public.work_orders
                    WHERE order_id LIKE %s
                      AND order_id ~ %s
                ),
                (SELECT COALESCE(MAX(end_no), 0) + 1 FROM public.order_segments),
                1
            )
            WHERE NOT EXISTS (SELECT 1 FROM public.allocation_meta)
            """,
            (
                _PREFIX_CUT,
                _PREFIX_LIKE,
                _PREFIX_PATTERN,
            ),
        )


# --------------------------------------------------------------------------- #
# 工单查询与种子数据
# --------------------------------------------------------------------------- #
def list_orders() -> List[Tuple[str, str]]:
    ensure_business_schema()
    with connect(autocommit=True) as conn:
        return conn.execute(
            "SELECT order_id, assignee FROM public.work_orders ORDER BY order_id"
        ).fetchall()


def seed_orders(order_ids: List[str]) -> None:
    """幂等写入普通编号测试工单（source='seed'），不触碰已存在行。

    领号产生的 ``OFF-n`` 编号空间受保护，种子接口拒绝写入该前缀编号，
    即原 /orders/seed 只新增普通编号。
    """
    if not order_ids:
        return
    ensure_business_schema()
    with connect(autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.work_orders(order_id, assignee, source)
            VALUES (%s, 'init', 'seed')
            ON CONFLICT (order_id) DO NOTHING
            """,
            [(oid,) for oid in sorted(set(order_ids))],
        )


# --------------------------------------------------------------------------- #
# 领号
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Segment:
    terminal_id: str
    request_no: str
    quantity: int
    start_no: int
    end_no: int
    retried: bool
    created_at: str = ""

    @property
    def order_ids(self) -> List[str]:
        return [make_order_id(n) for n in range(self.start_no, self.end_no + 1)]

    @property
    def start_order_id(self) -> str:
        return make_order_id(self.start_no)

    @property
    def end_order_id(self) -> str:
        return make_order_id(self.end_no)


def allocate(terminal_id: str, request_no: str, quantity: int) -> Segment:
    """领取一段连续 OFF 编号；同请求号重试返回原号段，改数量抛冲突。

    并发同 (terminal_id, request_no) 请求可能同时通过首查、在 INSERT 上
    撞唯一约束：输家事务整体回滚（计数器推进一并撤销，不浪费号码），
    重试时回读赢家已提交的原号段。
    """
    ensure_business_schema()
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            # 普通事务，with 退出时 COMMIT，异常时 ROLLBACK 并关闭连接
            with connect() as conn:
                conn.execute(
                    f"SET LOCAL lock_timeout = {int(settings.lock_timeout_ms)}"
                )

                row = conn.execute(
                    """
                    SELECT quantity, start_no, end_no
                    FROM public.order_segments
                    WHERE terminal_id = %s AND request_no = %s
                    """,
                    (terminal_id, request_no),
                ).fetchone()
                if row is not None:
                    existing_qty, start_no, end_no = row
                    if existing_qty != quantity:
                        raise AllocationConflict(
                            terminal_id, request_no, quantity, existing_qty
                        )
                    return Segment(terminal_id, request_no, existing_qty,
                                   start_no, end_no, retried=True)
                # 锁单行计数器：并发领号在此串行，号段不可能重叠
                next_no = conn.execute(
                    "SELECT next_no FROM public.allocation_meta "
                    "WHERE id = 1 FOR UPDATE"
                ).fetchone()[0]

                # 双保险：扫描旧库/历史已用 OFF 编号与已领号段，水位不足则跳过
                used_max = conn.execute(
                    """
                    SELECT COALESCE(MAX(n), 0) FROM (
                        SELECT SUBSTRING(order_id FROM %s)::bigint AS n
                        FROM public.work_orders
                        WHERE order_id LIKE %s AND order_id ~ %s
                        UNION ALL
                        SELECT end_no FROM public.order_segments
                    ) used
                    """,
                    (_PREFIX_CUT, _PREFIX_LIKE, _PREFIX_PATTERN),
                ).fetchone()[0]
                start_no = max(next_no, used_max + 1)
                end_no = start_no + quantity - 1

                conn.execute(
                    """
                    INSERT INTO public.order_segments
                        (terminal_id, request_no, quantity, start_no, end_no)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (terminal_id, request_no, quantity, start_no, end_no),
                )
                conn.execute(
                    "UPDATE public.allocation_meta SET next_no = %s WHERE id = 1",
                    (end_no + 1,),
                )
                created = conn.execute(
                    """
                    SELECT created_at FROM public.order_segments
                    WHERE terminal_id = %s AND request_no = %s
                    """,
                    (terminal_id, request_no),
                ).fetchone()[0]
                return Segment(terminal_id, request_no, quantity,
                               start_no, end_no, retried=False,
                               created_at=created.isoformat())
        except psycopg.errors.UniqueViolation as exc:
            # 并发同请求号：赢家已提交号段，回滚后重试回读
            last_exc = exc
            continue
    raise last_exc  # type: ignore[misc]


def get_segment(terminal_id: str,
                request_no: str,
                for_update: bool = False) -> Optional[Segment]:
    clause = " FOR UPDATE" if for_update else ""
    with connect() as conn:
        if for_update:
            conn.execute(
                f"SET LOCAL lock_timeout = {int(settings.lock_timeout_ms)}"
            )
        row = conn.execute(
            f"""
            SELECT quantity, start_no, end_no, created_at
            FROM public.order_segments
            WHERE terminal_id = %s AND request_no = %s{clause}
            """,
            (terminal_id, request_no),
        ).fetchone()
    if row is None:
        return None
    return Segment(terminal_id, request_no, row[0], row[1], row[2],
                   retried=False, created_at=row[3].isoformat())


# --------------------------------------------------------------------------- #
# 补录
# --------------------------------------------------------------------------- #
def backfill(
    terminal_id: str,
    request_no: str,
    items: Sequence[Tuple[str, str]],
) -> Tuple[int, int, List[str]]:
    """按号段补录工单。

    返回 ``(新写入数, 同号同负责人跳过数, 涉及工单号升序列表)``。
    校验失败抛 :class:`BackfillRejected`，事务回滚、整批不入库；
    号段不存在抛 :class:`SegmentNotFound`。
    """
    ensure_business_schema()
    with connect() as conn:  # 一笔事务，任一校验失败整体回滚
        conn.execute(f"SET LOCAL lock_timeout = {int(settings.lock_timeout_ms)}")

        seg_row = conn.execute(
            """
            SELECT quantity, start_no, end_no
            FROM public.order_segments
            WHERE terminal_id = %s AND request_no = %s
            FOR UPDATE
            """,
            (terminal_id, request_no),
        ).fetchone()
        if seg_row is None:
            raise SegmentNotFound(terminal_id, request_no)
        quantity, start_no, end_no = seg_row

        # 批内去重：同号同负责人合并；同号异负责人记为冲突
        deduped: Dict[str, str] = {}
        conflict_orders: List[str] = []
        for order_id, assignee in items:
            old = deduped.get(order_id)
            if old is None:
                deduped[order_id] = assignee
            elif old != assignee:
                conflict_orders.append(order_id)

        out_of_range = sorted(
            oid for oid in deduped
            if not (start_no <= int(oid[len(settings.order_prefix):]) <= end_no)
        )
        conflict_orders = sorted(set(conflict_orders))

        # 库内同号异负责人冲突（种子数据或早前补录已占用该编号）；
        # 同时记录此前已存在的同号同负责人工单数，用于幂等统计。
        already_present = 0
        if deduped:
            existing = conn.execute(
                "SELECT order_id, assignee FROM public.work_orders "
                "WHERE order_id = ANY(%s)",
                (list(deduped.keys()),),
            ).fetchall()
            for oid, assignee in existing:
                if assignee != deduped[oid]:
                    conflict_orders.append(oid)
                else:
                    already_present += 1
            conflict_orders = sorted(set(conflict_orders))

        if out_of_range or conflict_orders:
            raise BackfillRejected(
                out_of_range_orders=out_of_range,
                conflict_orders=sorted(conflict_orders),
            )

        # 全部合法：upsert。冲突已在上面排除，ON CONFLICT 只可能命中
        # 同号同负责人的幂等重试，DO NOTHING 即可。
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO public.work_orders(order_id, assignee, source)
                VALUES (%s, %s, 'backfill')
                ON CONFLICT (order_id) DO NOTHING
                """,
                [(oid, assignee) for oid, assignee in deduped.items()],
            )
        inserted = len(deduped) - already_present
        return inserted, already_present, sorted(deduped.keys())
