# 巡检终端离线领号与联网补录服务

巡检终端进入**无网区**前先向本服务领取一段连续工单号（`OFF-` + 正整数），
离线期间在本地记录工单与负责人；恢复联网后携带领号时的终端标识和请求号
**乱序补录**入库。

## 1. 业务规则

### 领号 `POST /allocate`

- 入参：终端标识 `terminal_id`、请求号 `request_no`、数量 `quantity`（1～50）；
- 返回一段**连续编号**，格式 `OFF-<正整数>`，如 `OFF-1`～`OFF-5`；
- 同一终端用**原请求号、原数量**重试：幂等返回**原号段**（`retried=true`）；
- 同一终端同请求号**改数量**重试：HTTP 409 拒绝，返回原数量与新数量；
- 并发领号：号段由数据库单行计数器事务串行生成，**号段绝不重叠**；
- 计数器与号段均持久化，**服务/数据库重启后不会重发已领号码**；
- 初始化与每次分配都会扫描 `work_orders` 中已存在的 `OFF-n`（旧库迁移、
  历史数据），水位不足时整体跳过，**旧库已用编号必须避开**。

### 补录 `POST /backfill`

- 入参：终端标识、领号请求号、`items`（工单号 + 负责人，**允许乱序**）；
- 工单号必须全部落在该请求号对应的号段内；
- **同号同负责人**重复提交：视为成功，幂等跳过（计入 `already_present`）；
- 出现**同号异负责人**（含批内自相矛盾、与库内已存数据冲突）或
  **段外编号**：响应中报出全部问题单号，HTTP 409，**整批不入库**；
- 每笔补录是一个数据库事务，先 `SELECT ... FOR UPDATE` 锁定**本号段行**：
  同段补录串行，不同段争用不同行——**一段的补录事务卡住时，其他段仍能
  补录并提交**；
- 补录成功的工单写入 `work_orders`（`source='backfill'`），可在
  `GET /orders` 查到。

### 其他接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/orders` | 查看全部工单及负责人（种子 + 补录） |
| POST | `/orders/seed` | 幂等写入**普通编号**测试工单（负责人 `init`）；拒绝 `OFF-` 领号专用编号 |
| POST | `/allocate` | 领号（同请求号幂等） |
| POST | `/backfill` | 按号段补录（整批事务，冲突整批回滚） |
| GET | `/segments/{terminal_id}/{request_no}` | 查询某次领号的号段 |

## 2. 数据模型

```
work_orders        业务工单：order_id 主键、assignee、source(seed/backfill)
order_segments     已领号段：(terminal_id, request_no) 主键，start_no/end_no
allocation_meta    单行计数器 next_no，分配事务内 FOR UPDATE 推进
```

## 3. Docker Compose 启动

```bash
docker compose up --build
# API: http://localhost:8000/docs   （Swagger UI，内置领号/补录样例）
# PG:  localhost:5432 (postgres/postgres，库名 patrol)
```

### Swagger 演示流程

打开 `/docs`：

1. `POST /allocate` → Try it out，用样例 `allocate_new_segment` 执行，
   得到号段（如 `OFF-1`～`OFF-5`）；
2. 原样再执行一次（`allocate_retry_same_segment`）：返回同一号段且
   `retried=true`；改用 `allocate_retry_changed_quantity_expect_409`：
   返回 409；
3. `POST /backfill` → 用 `backfill_out_of_order` 乱序补录；再执行
   `backfill_idempotent_same_assignee`：全部幂等跳过；
4. 执行 `backfill_conflict_expect_409_whole_batch_rejected` 与
   `backfill_out_of_range_expect_409_whole_batch_rejected`：观察响应中的
   问题单号，并用 `GET /orders` 确认整批未入库。

等价 curl：

```bash
# 领号
curl -X POST http://localhost:8000/allocate -H 'Content-Type: application/json' \
  -d '{"terminal_id":"PDA-001","request_no":"REQ-20261125-001","quantity":5}'

# 乱序补录
curl -X POST http://localhost:8000/backfill -H 'Content-Type: application/json' \
  -d '{"terminal_id":"PDA-001","request_no":"REQ-20261125-001",
       "items":[{"order_id":"OFF-3","assignee":"alice"},
                {"order_id":"OFF-1","assignee":"bob"}]}'
```

## 4. 本地无 Docker 时的自测

仓库提供 `scripts/local_pg.sh`：在用户目录下载并解压 PostgreSQL 15
官方二进制（免 root），初始化临时实例并监听 5433，再启动 API。

```bash
./scripts/local_pg.sh start          # 初始化/启动本地 PG
DATABASE_URL=postgresql://postgres@127.0.0.1:5433/patrol \
  python -m tests.selftest           # 10 个场景 65 项端到端自测
./scripts/local_pg.sh stop           # 停库
```

## 5. 目录结构

```
app/
  config.py        环境变量配置（编号前缀、数量 1~50、lock_timeout）
  models.py        Pydantic 入参/出参模型、OFF-正整数 格式校验、Swagger 样例
  db.py            建表、号段分配（计数器事务 + 并发重试）、补录整批事务
  main.py          FastAPI 路由
tests/selftest.py  端到端自测（10 个场景 65 项断言）
docker-compose.yml / Dockerfile / docs/swagger_examples.json
```

## 6. 关键设计与边界

- 号段分配依赖 `allocation_meta` 单行 `FOR UPDATE` 串行化；同请求号并发
  请求在 `order_segments` 唯一约束上冲突时，输家事务整体回滚（计数器推进
  一并撤销，不浪费号码）并重试回读赢家号段；
- 补录的段锁只锁 `order_segments` 中的本号段行，段间无锁交集；
  `work_orders` 按主键写入，不同段的编号空间不重叠，不会互相阻塞；
- 补录要求条目携带合法 `OFF-<正整数>`（无前导零）；非法格式在入参层
  422 拒绝，不进入事务；
- `lock_timeout`（默认 10s，`SET LOCAL` 仅作用于当前事务）保证同段补录
  排队异常时能快速失败，而不是无限挂住。
