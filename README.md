# 埠灯公服·抄表工单分域

构造行级访问矩阵，在 PostgreSQL RLS 中以受限角色核对可见集。面向演示数据和本地复现，场景、操作剧本和业务边界见 [BUSINESS.md](BUSINESS.md)。

## 业务模块

- 区域委派
- 抄表工单
- 权限验签
- 差异清单

## 本地运行

```sh
cp -n .env.example .env
docker compose up -d --build --wait
```

- 工作台 `http://127.0.0.1:18294`
- FastAPI 文档 `http://127.0.0.1:19294/docs`
- PostgreSQL 16 端口 `127.0.0.1:17794`，账号 `app`，演示库 `workflow`
- `docker compose exec -T backend python -m scripts.seed_demo` 手动重跑演示数据
- `docker compose logs -f backend worker` 查看运行情况
- `docker compose down` 停容器并保留演示卷

### 演示账号

统一密码 `Demo-Walkthrough-2026`。

| 账号 | 角色 | 能执行的操作 |
|---|---|---|
| `operator` | 经办员 | 读取档案、创建实验输入、提交执行、导出结果 |
| `reviewer` | 复核员 | 读取、导出和签署结果 |
| `viewer` | 观察员 | 读取、导出已有结果 |

`SEED_DEMO=true` 启动时生成账号与一个业务样例。工作台新建档案会预填样例。可编辑表单中的数值、文本和结构化 JSON 后提交。业务说明中的原始文件是上游示例，演示数据通过表单录入。角色枚举位于后端 `src/constants/workflow.py` 与前端 `src/constants/workflow.js`。执行状态枚举也位于这两个文件，使用于队列、列表和执行详情。证据校验码是报告 JSON 按 UTF-8、键排序且省略多余空格序列化后的 SHA-256。

## 工程结构

| 服务 | 实现 |
|---|---|
| frontend | Nginx 与原生 HTML、CSS、JavaScript |
| backend | Python 3.11、FastAPI、SQLAlchemy |
| worker | Python 持久队列执行与 Redis 唤醒信号 |
| db | PostgreSQL 16，持久卷保存档案、执行、证据及审计 |
| redis | Redis 7，追加日志卷保存队列唤醒信号 |

编辑 `backend/src/services/domain.py` 可修改试算方法与业务表单说明。API 路由、数据定义和页面位于 `backend/src/routes/`、`backend/src/models/` 与 `frontend/src/pages/`。开发时更新源文件后重新构建对应服务。

## 整库恢复演练

`backend/scripts/restore_drill.py` 把整库备份恢复到隔离库，核对恢复副本是否与同一时点一致，并在副本里接续备份时尚未出证的单。它只读来源库，拒绝把隔离库指向来源库本身。

```sh
# 备份时接单与签署照常；截点、恢复、核对与续办一次完成
docker compose exec -T backend python -m scripts.restore_drill \
  --output /work/restore-drill
```

- **截点**：在 `REPEATABLE READ READ ONLY` 事务里先 `pg_export_snapshot()`，再用 `pg_dump --snapshot` 导入同一快照，事务保持打开到备份结束。五张表都以该快照的可见性为准，提交早于快照的行在内，晚于快照的行不在内；`taken_at`、`txid_snapshot` 与 `wal_lsn` 一并写入清单。
- **认出当时未出证的单**：以 `business_evidence` 是否已存在为准，而不是执行状态。`finish` 在同一事务里写执行状态和证据行，所以“截点处没有证据行”就是权威的“未出证”标记。其中 `queued`／`running` 的单可继续，`complete` 但无证据的行视为异常、只报人工核查、不会自动重跑，`failed` 视为终结。
- **隔离与重试**：恢复写入独立隔离库（默认 `<来源库>_drill`，可用 `DRILL_TARGET_DB`／`DRILL_TARGET_URL` 指定），`pg_restore --single-transaction --exit-on-error` 保证失败即整库回滚；每次重试前强制 `DROP DATABASE ... WITH (FORCE)`，不会留下半套数据。
- **不重复出证**：`finish` 以 `run_id` 唯一约束幂等插入证据，队列 `claim` 也不认领已有证据的执行，续办对已出证的单只做跳过。重跑演练不会新增第二条证据。
- **不丢记录**：续办前后对各表做内容摘要比对，执行记录集合、档案与账号内容不得改变，既有证据不得丢失或改写。

结果写在 `--output` 目录的 `manifest-<drill_id>.json` 与 `result.json`。`result` 为 `verified`、`failed`（附 `reasons`）或 `aborted`（隔离校验未通过，退出码 2）。`continue_evidence_ids` 给出可以继续执行的执行号与对应证据号。

需要 PostgreSQL 16+ 客户端 `pg_dump`／`pg_restore`（可用 `PG_DUMP`、`PG_RESTORE` 指定路径），以及具备 `CREATEDB`（建隔离库）与回连维护库权限的账号。

`.env.example` 提供 `FRONTEND_PORT`、`BACKEND_PORT`、`DB_PORT`、`DB_PASSWORD`、`AUTH_KEY` 、`SEED_DEMO` 与 `NETWORK_SUBNET`。修改 `NETWORK_SUBNET` 可避开本机现有容器网段。所有发布的端口绑定本机回环地址。接入独立部署前需配置自己的凭据并按业务范围做独立验收。

[MIT License](LICENSE)
