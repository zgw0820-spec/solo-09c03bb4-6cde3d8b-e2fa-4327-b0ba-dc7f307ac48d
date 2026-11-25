"""端到端自测：离线领号 + 联网补录全链路。

覆盖：
 1. 领号格式/连续性/数量边界
 2. 同请求号幂等重试、改数量拒绝
 3. 并发领号号段不重叠、同请求号并发只生成一段
 4. 重启（重复建表初始化）后不重发已领号码
 5. 旧库已用 OFF 编号必须避开
 6. 补录乱序入库、同号同负责人幂等
 7. 同号异负责人 / 段外编号 -> 报单号、整批不入库
 8. 一段补录事务卡住时其他段仍可提交（行锁只锁本号段）
 9. /orders/seed 只新增普通编号，补录工单可在 /orders 查到
10. Swagger / openapi 可用

运行（需要可连通的 PostgreSQL）：
    python -m tests.selftest
"""

from __future__ import annotations

import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi.testclient import TestClient

from app.config import settings
from app.db import (
    BackfillRejected,
    allocate,
    backfill,
    connect,
    ensure_business_schema,
)
from app.main import app
from app.models import make_order_id

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"[{PASS if cond else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def reset_db() -> None:
    """每个场景前清空业务表，保证自测可重复执行。"""
    ensure_business_schema()
    with connect(autocommit=True) as conn:
        conn.execute(
            "TRUNCATE TABLE public.work_orders, public.order_segments, "
            "public.allocation_meta RESTART IDENTITY"
        )
    ensure_business_schema()


def case_allocate_basic() -> None:
    print("\n== 场景1：领号格式、连续性与数量边界 ==")
    reset_db()
    seg = allocate("PDA-1", "REQ-1", 5)
    check("编号格式为 OFF-正整数",
          seg.order_ids == ["OFF-1", "OFF-2", "OFF-3", "OFF-4", "OFF-5"],
          str(seg.order_ids))
    check("号段连续且起止正确",
          seg.start_order_id == "OFF-1" and seg.end_order_id == "OFF-5")
    check("新领 retried=false", seg.retried is False)

    seg2 = allocate("PDA-1", "REQ-2", 1)
    check("第二次领号紧接前一段", seg2.order_ids == ["OFF-6"], str(seg2.order_ids))

    seg3 = allocate("PDA-1", "REQ-3", settings.max_quantity)
    check("数量上限 50 被接受", len(seg3.order_ids) == 50
          and seg3.start_no == 7 and seg3.end_no == 56)

    client = TestClient(app)
    r = client.post("/allocate", json={
        "terminal_id": "PDA-1", "request_no": "REQ-BAD1", "quantity": 0})
    check("数量 0 被 422 拒绝", r.status_code == 422, str(r.status_code))
    r = client.post("/allocate", json={
        "terminal_id": "PDA-1", "request_no": "REQ-BAD2", "quantity": 51})
    check("数量 51 被 422 拒绝", r.status_code == 422, str(r.status_code))


def case_idempotent_retry() -> None:
    print("\n== 场景2：同请求号幂等重试，改数量拒绝 ==")
    reset_db()
    client = TestClient(app)
    payload = {"terminal_id": "PDA-7", "request_no": "REQ-A", "quantity": 3}
    r1 = client.post("/allocate", json=payload)
    first = r1.json()
    check("首次领号 200 且 retried=false",
          r1.status_code == 200 and first["retried"] is False, r1.text[:200])

    r2 = client.post("/allocate", json=payload)
    second = r2.json()
    check("同参重试 200 且 retried=true",
          r2.status_code == 200 and second["retried"] is True, r2.text[:200])
    check("重试返回原号段", second["order_ids"] == first["order_ids"]
          and second["start_order_id"] == first["start_order_id"],
          str(second["order_ids"]))

    r3 = client.post("/allocate", json={
        "terminal_id": "PDA-7", "request_no": "REQ-A", "quantity": 4})
    body = r3.json()
    check("改数量被 409 拒绝", r3.status_code == 409, str(r3.status_code))
    check("冲突响应带原数量与新数量",
          body["detail"]["existing_quantity"] == 3
          and body["detail"]["requested_quantity"] == 4,
          str(body["detail"]))

    # 改数量被拒后，原参重试仍返回原号段（拒绝没有副作用）
    r4 = client.post("/allocate", json=payload)
    check("拒绝后原参重试仍是原号段",
          r4.status_code == 200 and r4.json()["order_ids"] == first["order_ids"])


def case_concurrent_allocate() -> None:
    print("\n== 场景3：并发领号号段不重叠 ==")
    reset_db()
    n_threads, qty = 12, 5

    def worker(i: int):
        return allocate(f"PDA-C{i}", f"REQ-C{i}", qty)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        segs = list(pool.map(worker, range(n_threads)))

    all_ids: list[str] = []
    for s in segs:
        check(f"段 {s.terminal_id} 内部连续",
              s.order_ids == [make_order_id(n)
                              for n in range(s.start_no, s.end_no + 1)])
        all_ids.extend(s.order_ids)
    check("并发号段总数正确", len(all_ids) == n_threads * qty, str(len(all_ids)))
    check("并发号段无重叠", len(set(all_ids)) == len(all_ids),
          f"unique={len(set(all_ids))} total={len(all_ids)}")

    print("\n== 场景3b：同终端同请求号并发重试只生成一个号段 ==")
    barrier = threading.Barrier(8)

    def same_key_worker() -> Any:
        barrier.wait()
        return allocate("PDA-SAME", "REQ-SAME", 4)

    with ThreadPoolExecutor(max_workers=8) as pool:
        same_segs = list(pool.map(lambda _: same_key_worker(), range(8)))
    ranges = {(s.start_no, s.end_no) for s in same_segs}
    check("8 个并发同请求号只落一个号段", len(ranges) == 1, str(ranges))
    check("其中 7 个为重试返回",
          sum(1 for s in same_segs if s.retried) == 7
          and sum(1 for s in same_segs if not s.retried) == 1)


def case_restart_no_reissue() -> None:
    print("\n== 场景4：重启后不重发已领号码 ==")
    reset_db()
    seg = allocate("PDA-R", "REQ-R1", 3)
    before = set()
    with connect(autocommit=True) as conn:
        rows = conn.execute(
            "SELECT start_no, end_no, next_no FROM public.order_segments, "
            "public.allocation_meta "
            "WHERE terminal_id='PDA-R' AND request_no='REQ-R1' AND id=1"
        ).fetchall()
        check("号段与计数器已持久化", len(rows) == 1, str(rows))

    # 模拟服务重启：重新执行建表/初始化（CREATE TABLE IF NOT EXISTS +
    # meta 仅在缺失时初始化），再领新号
    ensure_business_schema()
    ensure_business_schema()
    seg2 = allocate("PDA-R", "REQ-R2", 2)
    check("重启后新号段不与旧号段重叠",
          seg2.start_no > seg.end_no,
          f"old=({seg.start_no},{seg.end_no}) new=({seg2.start_no},{seg2.end_no})")
    check("重启后同请求号仍回原号段",
          allocate("PDA-R", "REQ-R1", 3).order_ids == seg.order_ids)


def case_legacy_used_numbers_avoided() -> None:
    print("\n== 场景5：旧库已用 OFF 编号必须避开 ==")
    reset_db()
    # 模拟旧库迁移：work_orders 已存在 OFF-100（meta 尚未建立）
    with connect(autocommit=True) as conn:
        conn.execute(
            "TRUNCATE TABLE public.work_orders, public.order_segments, "
            "public.allocation_meta"
        )
        conn.execute(
            "INSERT INTO public.work_orders(order_id, assignee, source) "
            "VALUES ('OFF-100', 'legacy', 'seed')"
        )
    ensure_business_schema()  # 新部署面对旧库初始化计数器
    seg = allocate("PDA-L", "REQ-L1", 3)
    check("新号段整体跳过旧库已用编号",
          seg.order_ids == ["OFF-101", "OFF-102", "OFF-103"], str(seg.order_ids))
    check("不与旧库编号冲突", "OFF-100" not in seg.order_ids)

    # 旧库最大编号远高于水位时同样跳过
    with connect(autocommit=True) as conn:
        conn.execute(
            "INSERT INTO public.work_orders(order_id, assignee, source) "
            "VALUES ('OFF-999', 'legacy2', 'seed') "
            "ON CONFLICT DO NOTHING"
        )
    seg2 = allocate("PDA-L", "REQ-L2", 2)
    check("运行期遇到更大旧编号也跳过",
          seg2.order_ids == ["OFF-1000", "OFF-1001"], str(seg2.order_ids))


def case_backfill_happy_path() -> None:
    print("\n== 场景6：补录乱序入库与幂等 ==")
    reset_db()
    seg = allocate("PDA-B", "REQ-B1", 3)  # OFF-1..3
    client = TestClient(app)
    payload = {
        "terminal_id": "PDA-B",
        "request_no": "REQ-B1",
        "items": [
            {"order_id": "OFF-3", "assignee": "alice"},
            {"order_id": "OFF-1", "assignee": "bob"},
            {"order_id": "OFF-2", "assignee": "alice"},
        ],
    }
    r = client.post("/backfill", json=payload)
    body = r.json()
    check("乱序补录 200", r.status_code == 200, r.text[:300])
    check("新写入 3 单", body["inserted"] == 3, str(body))
    check("补录工单可在 /orders 查到",
          {o["order_id"]: o["assignee"] for o in client.get("/orders").json()["orders"]}
          == {"OFF-1": "bob", "OFF-2": "alice", "OFF-3": "alice"})

    # 同号同负责人重提：视为成功，不新增
    r2 = client.post("/backfill", json=payload)
    b2 = r2.json()
    check("同号同负责人重试 200 且幂等跳过",
          r2.status_code == 200 and b2["inserted"] == 0
          and b2["already_present"] == 3, str(b2))

    # 部分新单 + 部分幂等
    r3 = client.post("/backfill", json={
        "terminal_id": "PDA-B", "request_no": "REQ-B1",
        "items": [
            {"order_id": "OFF-1", "assignee": "bob"},      # 已存在同负责人
            {"order_id": "OFF-3", "assignee": "alice"},    # 已存在同负责人
        ],
    })
    b3 = r3.json()
    check("混合补录计数正确",
          b3["inserted"] == 0 and b3["already_present"] == 2, str(b3))


def case_backfill_conflicts() -> None:
    print("\n== 场景7：同号异负责人/段外编号 -> 报单号、整批不入库 ==")
    reset_db()
    client = TestClient(app)
    client.post("/allocate", json={
        "terminal_id": "PDA-F", "request_no": "REQ-F1", "quantity": 3})  # OFF-1..3
    # 先正常补录 OFF-1=bob
    client.post("/backfill", json={
        "terminal_id": "PDA-F", "request_no": "REQ-F1",
        "items": [{"order_id": "OFF-1", "assignee": "bob"}]})

    # 同号异负责人 + 一个合法单：整批必须拒绝
    r = client.post("/backfill", json={
        "terminal_id": "PDA-F", "request_no": "REQ-F1",
        "items": [
            {"order_id": "OFF-1", "assignee": "carol"},   # 同号异负责人
            {"order_id": "OFF-2", "assignee": "alice"},   # 合法但连带拒绝
        ],
    })
    detail = r.json()["detail"]
    check("同号异负责人 409", r.status_code == 409, str(r.status_code))
    check("报出冲突单号", detail["conflict_orders"] == ["OFF-1"], str(detail))
    orders = {o["order_id"] for o in client.get("/orders").json()["orders"]}
    check("整批不入库（OFF-2 未写入）", "OFF-2" not in orders and "OFF-1" in orders,
          str(orders))

    # 段外编号
    r2 = client.post("/backfill", json={
        "terminal_id": "PDA-F", "request_no": "REQ-F1",
        "items": [
            {"order_id": "OFF-9", "assignee": "alice"},   # 段外
            {"order_id": "OFF-3", "assignee": "alice"},   # 合法但连带拒绝
        ],
    })
    d2 = r2.json()["detail"]
    check("段外编号 409 且报出单号",
          r2.status_code == 409 and d2["out_of_range_orders"] == ["OFF-9"], str(d2))
    orders2 = {o["order_id"] for o in client.get("/orders").json()["orders"]}
    check("段外批次整批不入库", "OFF-3" not in orders2, str(orders2))

    # 批内自身同号异负责人
    r3 = client.post("/backfill", json={
        "terminal_id": "PDA-F", "request_no": "REQ-F1",
        "items": [
            {"order_id": "OFF-2", "assignee": "alice"},
            {"order_id": "OFF-2", "assignee": "mallory"},
        ],
    })
    check("批内同号异负责人 409",
          r3.status_code == 409
          and r3.json()["detail"]["conflict_orders"] == ["OFF-2"], r3.text[:200])

    # 直接层异常类型核查
    try:
        backfill("PDA-F", "REQ-F1", [("OFF-9", "alice")])
        check("BackfillRejected 被抛出", False)
    except BackfillRejected as exc:
        check("BackfillRejected 带段外单号", exc.out_of_range_orders == ["OFF-9"])

    # 非法格式（前导 0 / 非数字）在入参层 422
    r4 = client.post("/backfill", json={
        "terminal_id": "PDA-F", "request_no": "REQ-F1",
        "items": [{"order_id": "OFF-01", "assignee": "alice"}]})
    check("OFF-01 前导零格式被 422 拒绝", r4.status_code == 422, str(r4.status_code))

    # 不存在的号段 404
    r5 = client.post("/backfill", json={
        "terminal_id": "PDA-F", "request_no": "NO-SUCH",
        "items": [{"order_id": "OFF-1", "assignee": "alice"}]})
    check("未知号段补录 404", r5.status_code == 404, str(r5.status_code))


def case_segment_lock_isolation() -> None:
    print("\n== 场景8：一段补录卡住时其他段仍可补录并提交 ==")
    reset_db()
    seg_a = allocate("PDA-X", "REQ-X1", 2)   # OFF-1..2
    seg_b = allocate("PDA-X", "REQ-X2", 2)   # OFF-3..4

    # 独立连接开启事务并锁住 A 段行（模拟 A 段补录事务卡住）
    stuck = connect()
    stuck.execute("BEGIN")
    stuck.execute(
        "SELECT 1 FROM public.order_segments "
        "WHERE terminal_id=%s AND request_no=%s FOR UPDATE",
        ("PDA-X", "REQ-X1"),
    ).fetchall()

    result: dict[str, Any] = {}

    def other_segment() -> None:
        # 不同号段：争用不同的段行，不应被卡住的事务阻塞
        t0 = time.time()
        inserted, present, ids = backfill(
            "PDA-X", "REQ-X2", [("OFF-3", "alice"), ("OFF-4", "bob")]
        )
        result["elapsed"] = time.time() - t0
        result["ids"] = ids
        result["inserted"] = inserted

    t = threading.Thread(target=other_segment)
    t.start()
    t.join(timeout=5)
    check("其他段补录未被卡住事务阻塞", not t.is_alive(),
          f"elapsed={result.get('elapsed')}")
    check("其他段补录成功提交",
          result.get("inserted") == 2 and result.get("ids") == ["OFF-3", "OFF-4"],
          str(result))

    # 同段补录在卡住事务释放前确实等待（锁只作用于同段）
    same_result: dict[str, Any] = {}

    def same_segment() -> None:
        try:
            backfill("PDA-X", "REQ-X1", [("OFF-1", "alice")])
            same_result["done"] = True
        except Exception as exc:  # noqa: BLE001
            same_result["err"] = type(exc).__name__

    ts = threading.Thread(target=same_segment)
    ts.start()
    ts.join(timeout=2)
    check("同段补录在段锁释放前等待", ts.is_alive(), str(same_result))
    stuck.execute("ROLLBACK")
    stuck.close()
    ts.join(timeout=5)
    check("段锁释放后同段补录完成",
        same_result.get("done") is True and not ts.is_alive(), str(same_result))

    # B 段数据已落库，A 段随后也能补
    inserted, _, _ = backfill("PDA-X", "REQ-X1", [("OFF-2", "carol")])
    check("A 段补录在锁释放后正常入库", inserted == 1)
    with connect(autocommit=True) as conn:
        rows = dict(conn.execute(
            "SELECT order_id, assignee FROM public.work_orders "
            "WHERE order_id = ANY(%s)",
            (["OFF-1", "OFF-2", "OFF-3", "OFF-4"],),
        ).fetchall())
    check("两段数据各自完整",
          rows == {"OFF-1": "alice", "OFF-2": "carol",
                   "OFF-3": "alice", "OFF-4": "bob"},
          str(rows))


def case_seed_and_views() -> None:
    print("\n== 场景9：种子接口只新增普通编号，补录工单可查 ==")
    reset_db()
    client = TestClient(app)
    r = client.post("/orders/seed", json=["W-100", "W-101", "W-100"])
    check("普通编号种子写入（去重、幂等）",
          r.status_code == 200 and r.json()["seeded"] == ["W-100", "W-101"], r.text)
    r2 = client.post("/orders/seed", json=["OFF-7"])
    check("种子接口拒绝 OFF 领号专用编号", r2.status_code == 400, str(r2.status_code))
    r3 = client.post("/orders/seed", json=["   "])
    check("空编号列表 400", r3.status_code == 400)

    client.post("/allocate", json={
        "terminal_id": "PDA-V", "request_no": "REQ-V1", "quantity": 1})
    client.post("/backfill", json={
        "terminal_id": "PDA-V", "request_no": "REQ-V1",
        "items": [{"order_id": "OFF-1", "assignee": "dave"}]})
    orders = client.get("/orders").json()["orders"]
    idx = {o["order_id"]: o["assignee"] for o in orders}
    check("/orders 同时含普通种子与补录工单",
          idx.get("W-100") == "init" and idx.get("OFF-1") == "dave", str(idx))

    r4 = client.get("/segments/PDA-V/REQ-V1")
    check("号段查询接口 200 且内容一致",
          r4.status_code == 200 and r4.json()["start_order_id"] == "OFF-1"
          and r4.json()["quantity"] == 1, r4.text[:200])
    r5 = client.get("/segments/PDA-V/NOPE")
    check("号段查询不存在 404", r5.status_code == 404)


def case_api_docs() -> None:
    print("\n== 场景10：健康检查与 Swagger/openapi ==")
    client = TestClient(app)
    check("health 200", client.get("/health").status_code == 200)
    check("Swagger UI 200", client.get("/docs").status_code == 200)
    paths = client.get("/openapi.json").json()["paths"]
    check("openapi 含全部新路由",
          {"/allocate", "/backfill", "/orders", "/orders/seed",
           "/segments/{terminal_id}/{request_no}", "/health"} <= set(paths),
          str(sorted(paths)))
    check("旧 /precheck 路由已移除", "/precheck" not in paths)


def main() -> int:
    print(f"DATABASE_URL={settings.database_url}")
    t0 = time.time()
    case_allocate_basic()
    case_idempotent_retry()
    case_concurrent_allocate()
    case_restart_no_reissue()
    case_legacy_used_numbers_avoided()
    case_backfill_happy_path()
    case_backfill_conflicts()
    case_segment_lock_isolation()
    case_seed_and_views()
    case_api_docs()
    dt = time.time() - t0

    total = len(results)
    failed = [r for r in results if not r[1]]
    print(f"\n==== 自测结果：{total - len(failed)}/{total} 通过，用时 {dt:.1f}s ====")
    if failed:
        for name, _, detail in failed:
            print(f"  失败项: {name} -- {detail}")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
