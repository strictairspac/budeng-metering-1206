"""Whole-database restore drill built around one exported visibility snapshot.

The drill answers three operational questions:

* Where is the cut?  A ``REPEATABLE READ READ ONLY`` transaction exports its
  snapshot with ``pg_export_snapshot()``; ``pg_dump --snapshot`` imports that
  exact snapshot, so every table in the dump shares one MVCC cut.  Rows
  committed after the cut are excluded, rows committed before are included,
  regardless of when the dump physically reads them.
* Which runs had not issued evidence at the cut?  Evidence absence, not run
  status.  ``finish`` writes the run status and its evidence row in one
  transaction, so "no row in ``business_evidence``" is the authoritative
  "not yet issued" marker at that instant.
* What may continue?  Only runs that were ``queued``/``running`` at the cut.
  ``complete`` runs with an evidence row are never re-run; ``failed`` runs are
  terminal; ``complete`` without evidence is an anomaly and is not re-run.

The dump is restored into a database that is required to be different from the
source, in a single transaction, and verified against per-table digests taken
at the same snapshot.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.engine import URL

from src.constants.workflow import Status
from src.models.tables import audit, cases, evidence, runs, users
from src.utils.encoding import canonical
from src.utils.time import utc_time

TABLES = (users, cases, runs, evidence, audit)
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

CUTOFF_RULE = (
    "截点取 pg_export_snapshot() 导出的可导入快照：在 REPEATABLE READ READ ONLY "
    "事务中先导出快照，再以 pg_dump --snapshot 导入同一快照，事务保持打开直到备份结束。"
    "副本五张表都以该快照的可见性为准，提交早于快照的行包含在内，晚于快照的行不包含。"
)


class DrillError(RuntimeError):
    """The drill cannot safely proceed."""


@dataclass(frozen=True)
class Cutoff:
    database: str
    system_identifier: str
    snapshot: str
    txid_snapshot: str
    wal_lsn: str
    taken_at: str
    rule: str = CUTOFF_RULE


@dataclass
class Manifest:
    drill_id: str
    source: dict
    cutoff: dict
    dump: dict
    snapshot_counts: dict
    snapshot_digests: dict
    target: dict = field(default_factory=dict)
    restore: dict = field(default_factory=dict)
    verify: dict = field(default_factory=dict)
    not_yet_evidenced_at_cutoff: list = field(default_factory=list)
    continuable_runs: list = field(default_factory=list)
    resume: dict = field(default_factory=dict)
    result: str = "pending"
    reasons: list = field(default_factory=list)

    def as_dict(self):
        return asdict(self)


def asdict_cutoff(cutoff: Cutoff) -> dict:
    return asdict(cutoff)


# --------------------------------------------------------------------------- #
# Snapshot + dump
# --------------------------------------------------------------------------- #

def _binary(env_name, default):
    path = os.getenv(env_name, default)
    if os.path.sep in path:
        if not Path(path).is_file():
            raise DrillError(f"找不到 {env_name}={path}")
        return path
    from shutil import which
    found = which(path)
    if not found:
        raise DrillError(
            f"找不到 {default}，请安装 PostgreSQL 16 客户端或设置 {env_name}。")
    return found


def _conninfo(url: URL) -> str:
    parts = []
    for key, value in (("host", url.host), ("port", url.port),
                       ("dbname", url.database), ("user", url.username)):
        if value in (None, ""):
            continue
        text = str(value).replace("\\", "\\\\").replace("'", "\\'")
        parts.append(f"{key}='{text}'")
    return " ".join(parts)


def _dump_env(url: URL) -> dict:
    env = os.environ.copy()
    if url.password:
        env["PGPASSWORD"] = url.password
    return env


def _digest(rows) -> str:
    return hashlib.sha256(canonical(rows)).hexdigest()


def _table_rows(conn):
    return {table.name: [dict(row) for row in
                         conn.execute(select(table).order_by(table.c.id)).mappings()]
            for table in TABLES}


def classify_runs(rows: dict) -> dict:
    """Classify runs by whether they had issued evidence, independent of restore.

    Used both at capture time (so a failed restore still records which runs were
    pending) and on the restored copy (the authoritative check).
    """
    run_rows = rows[runs.name]
    evidence_rows = rows[evidence.name]
    evidence_by_run = {}
    duplicate = []
    for row in evidence_rows:
        if row["run_id"] in evidence_by_run:
            duplicate.append(row["run_id"])
        evidence_by_run[row["run_id"]] = row
    issued = set(evidence_by_run)
    not_yet = [row for row in run_rows if row["id"] not in issued]
    complete_without = [row["id"] for row in not_yet if row["status"] == Status.COMPLETE]
    continuable = [{"run_id": row["id"], "case_id": row["case_id"],
                    "status": row["status"], "owner": row["owner"]}
                   for row in not_yet if row["status"] in (Status.QUEUED, Status.RUNNING)]
    return {
        "evidence_by_run": evidence_by_run,
        "duplicate_evidence_runs": duplicate,
        "not_yet_evidenced_at_cutoff": sorted(row["id"] for row in not_yet),
        "continuable_runs": continuable,
        "complete_without_evidence": complete_without,
    }


def capture_snapshot(source_engine, dump_path) -> tuple[Cutoff, dict, dict, dict]:
    """Export one snapshot and dump the whole database at that exact cut."""
    url = source_engine.url
    dump_path = Path(dump_path)
    dump_path.parent.mkdir(parents=True, exist_ok=True)
    pg_dump = _binary("PG_DUMP", "pg_dump")
    with source_engine.connect() as conn:
        conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        row = conn.exec_driver_sql("""
            SELECT pg_export_snapshot() AS snapshot,
                   txid_current_snapshot()::text AS txid_snapshot,
                   pg_current_wal_lsn()::text AS wal_lsn,
                   statement_timestamp()::text AS taken_at,
                   current_database() AS database,
                   (SELECT system_identifier::text FROM pg_control_system()) AS system_identifier
        """).mappings().one()
        cutoff = Cutoff(**{key: row[key] for key in (
            "database", "system_identifier", "snapshot", "txid_snapshot",
            "wal_lsn", "taken_at")})
        try:
            subprocess.run(
                [pg_dump, f"--snapshot={cutoff.snapshot}", "--format=custom",
                 "--no-owner", "--no-privileges", f"--dbname={_conninfo(url)}",
                 f"--file={dump_path}"],
                check=True, env=_dump_env(url), capture_output=True, text=True)
            # Read the reference rows in this same exported snapshot, before it
            # is released, so the reference and the dump share one cut.
            rows = _table_rows(conn)
            counts = {name: len(value) for name, value in rows.items()}
            digests = {name: _digest(value) for name, value in rows.items()}
            classified = classify_runs(rows)
        except subprocess.CalledProcessError as exc:
            raise DrillError(f"pg_dump 失败：{exc.stderr.strip()[:500]}") from None
        finally:
            conn.rollback()
    return cutoff, counts, digests, classified


# --------------------------------------------------------------------------- #
# Restore into an isolated database
# --------------------------------------------------------------------------- #

def _admin_connect(target_url: URL):
    """Connect to the maintenance database on the *target* server."""
    admin_db = os.getenv("DRILL_ADMIN_DB", "postgres")
    admin_url = target_url.set(database=admin_db)
    return create_engine(admin_url, pool_pre_ping=True)


def _same_server_database(a: URL, b: URL) -> bool:
    return (a.host, a.port, a.database) == (b.host, b.port, b.database)


def ensure_isolated(source_url: URL, target_url: URL) -> None:
    if not target_url.database or not IDENTIFIER.match(target_url.database):
        raise DrillError(f"隔离库名称不合法：{target_url.database!r}")
    if _same_server_database(source_url, target_url):
        raise DrillError(
            f"隔离库不能与来源库相同（{target_url.database}），演练拒绝写入生产库。")


def _recreate_database(admin_engine, target: str) -> None:
    with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{target}" WITH (FORCE)')
        conn.exec_driver_sql(f'CREATE DATABASE "{target}" TEMPLATE template0')


def _drop_database(admin_engine, target: str) -> None:
    with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{target}" WITH (FORCE)')


def restore_database(source_url: URL, target_url: URL, dump_path,
                     retries: int = 3, retry_delay: float = 1.0) -> dict:
    """Restore the dump into a fresh isolated database, retrying atomically."""
    bench = Path(dump_path)
    if not bench.is_file():
        raise DrillError(f"备份文件不存在：{bench}")
    pg_restore = _binary("PG_RESTORE", "pg_restore")
    admin = _admin_connect(target_url)
    try:
        admin.connect().close()
    except Exception as exc:
        admin.dispose()
        raise DrillError(f"无法连接隔离库服务器的维护库：{exc}") from None
    target = target_url.database
    attempts = []
    started = time.monotonic()
    last_error = None
    try:
        for attempt in range(1, max(1, retries) + 1):
            try:
                _recreate_database(admin, target)
                subprocess.run(
                    [pg_restore, "--single-transaction", "--exit-on-error",
                     "--no-owner", "--no-privileges", f"--dbname={_conninfo(target_url)}",
                     str(bench)],
                    check=True, env=_dump_env(target_url), capture_output=True, text=True)
                attempts.append({"attempt": attempt, "status": "ok"})
                return {"status": "ok", "attempts": attempts,
                        "elapsed_seconds": round(time.monotonic() - started, 3)}
            except subprocess.CalledProcessError as exc:
                last_error = (exc.stderr or "").strip()[:800] or f"pg_restore 退出码 {exc.returncode}"
                attempts.append({"attempt": attempt, "status": "failed", "error": last_error})
            except Exception as exc:  # connection / DDL failure
                last_error = f"{type(exc).__name__}: {exc}"[:800]
                attempts.append({"attempt": attempt, "status": "failed", "error": last_error})
            try:
                _drop_database(admin, target)
            except Exception:
                pass
            if attempt < retries:
                time.sleep(retry_delay * attempt)
    finally:
        admin.dispose()
    raise DrillError(f"恢复重试 {retries} 次仍失败：{last_error}")


# --------------------------------------------------------------------------- #
# Verification at the same cut
# --------------------------------------------------------------------------- #

def reconcile(restored_engine, cutoff: Cutoff, snapshot_counts: dict,
              snapshot_digests: dict, source_snapshot: dict | None = None) -> dict:
    """Compare the restored copy with the snapshot cut and classify every run.

    The per-table digests were taken in the exported snapshot, so equality proves
    the copy is one consistent cut rather than a mix of pre- and post-cut rows.
    ``source_snapshot`` (current source content) is used only to report how far
    the live source has moved past the cut; it never affects the verdict.
    """
    with restored_engine.connect() as conn:
        rows = _table_rows(conn)
        restored_system = conn.exec_driver_sql(
            "SELECT system_identifier::text FROM pg_control_system()").scalar_one()
    counts = {name: len(value) for name, value in rows.items()}
    digests = {name: _digest(value) for name, value in rows.items()}

    reasons = []
    anomalies = []
    counts_match = counts == snapshot_counts
    digests_match = digests == snapshot_digests
    if not counts_match:
        reasons.append(f"恢复后行数与快照不一致：{counts} != {snapshot_counts}")
    if not digests_match:
        bad = [name for name in digests if digests[name] != snapshot_digests.get(name)]
        reasons.append(f"恢复后表内容与快照不一致：{bad}")

    evidence_rows = rows[evidence.name]
    run_rows = rows[runs.name]
    case_ids = {row["id"] for row in rows[cases.name]}
    run_ids = {row["id"] for row in run_rows}
    classified = classify_runs(rows)
    evidence_by_run = classified["evidence_by_run"]
    digest_bad = []
    review_partial = []
    for row in evidence_rows:
        if hashlib.sha256(canonical(row["report"])).hexdigest() != row["digest"]:
            digest_bad.append(row["id"])
        signed = [row["reviewer"], row["review_note"], row["reviewed_at"]]
        if any(value is not None for value in signed) and not all(value is not None for value in signed):
            review_partial.append(row["id"])
    for run_id in classified["duplicate_evidence_runs"]:
        reasons.append(f"执行 {run_id} 存在多条证据")
    if digest_bad:
        reasons.append(f"证据校验码与报告不一致：{digest_bad}")
    if review_partial:
        reasons.append(f"签署意见字段不完整：{review_partial}")

    orphan_evidence = sorted(set(evidence_by_run) - run_ids)
    orphan_runs = sorted({row["case_id"] for row in run_rows} - case_ids)
    if orphan_evidence:
        reasons.append(f"证据引用了不存在的执行：{orphan_evidence}")
    if orphan_runs:
        reasons.append(f"执行引用了不存在的档案：{orphan_runs}")

    complete_without = classified["complete_without_evidence"]
    continuable = classified["continuable_runs"]
    if complete_without:
        # Faithfully reproduced source anomaly, not a restore fault: the copy is
        # still one coherent cut.  These rows are withheld from continuation and
        # flagged for manual review; the other pending runs may still proceed.
        anomalies.append(f"已完成但缺少证据，需人工核查且不自动重跑：{complete_without}")

    source_drift = {}
    if source_snapshot is not None:
        for table in (users.name, cases.name, runs.name, evidence.name, audit.name):
            source_drift[table] = len(source_snapshot[table]["rows"]) - snapshot_counts.get(table, 0)
    return {
        "counts": counts,
        "counts_match": counts_match,
        "digests": digests,
        "digests_match": digests_match,
        "restored_system_identifier": restored_system,
        "source_drift_since_cut": source_drift,
        "evidence": {
            "rows": len(evidence_rows),
            "digest_ok": len(evidence_rows) - len(digest_bad),
            "digest_bad": digest_bad,
            "signed": sum(1 for row in evidence_rows if row["reviewed_at"] is not None),
            "unsigned": sum(1 for row in evidence_rows if row["reviewed_at"] is None),
            "review_partial": review_partial,
        },
        "referential": {"orphan_evidence": orphan_evidence, "orphan_run_cases": orphan_runs},
        "complete_without_evidence": complete_without,
        "anomalies": anomalies,
        "not_yet_evidenced_at_cutoff": classified["not_yet_evidenced_at_cutoff"],
        "continuable_runs": continuable,
        "reasons": reasons,
    }


# --------------------------------------------------------------------------- #
# Continuation inside the isolated copy
# --------------------------------------------------------------------------- #

def _evidence_exists(conn, run_id):
    return conn.execute(select(evidence.c.id).where(evidence.c.run_id == run_id)).first() is not None


def resume_continuable(engine, run_ids, actor="restore-drill") -> dict:
    """Requeue and finish only the runs that had not issued evidence at the cut.

    Requeueing is idempotent and refuses any run that already owns evidence, so
    a retried drill can never issue a second proof.  ``claim``/``finish`` carry
    the same guard for any later worker pass.
    """
    from src.services.domain import solve
    from src.services.workflow import claim, finish

    requeued = []
    skipped = []
    with engine.begin() as conn:
        for run_id in run_ids:
            if _evidence_exists(conn, run_id):
                skipped.append(run_id)
                continue
            changed = conn.execute(
                runs.update().where(runs.c.id == run_id,
                                    runs.c.status.in_((Status.QUEUED, Status.RUNNING)))
                .values(status=Status.QUEUED, attempt_token=None)).rowcount
            if changed:
                conn.execute(audit.insert().values(
                    actor=actor, action="restore_resume", reference=str(run_id),
                    created_at=utc_time()))
                requeued.append(run_id)
            else:
                skipped.append(run_id)

    processed = []
    while True:
        job = claim(engine=engine)
        if not job:
            break
        run_id, token, payload = job
        try:
            result = solve(payload, engine)
            finish(run_id, token, result=result, engine=engine)
        except Exception as exc:  # keep the audit trail, mark failed, never lose the run
            finish(run_id, token, error=str(exc)[:500], engine=engine)
        processed.append(run_id)

    with engine.connect() as conn:
        statuses = {run_id: conn.execute(
            select(runs.c.status).where(runs.c.id == run_id)).scalar_one()
            for run_id in requeued}
        evidence_ids = {run_id: [row for row in conn.execute(
            select(evidence.c.id).where(evidence.c.run_id == run_id).order_by(evidence.c.id)).scalars()]
            for run_id in requeued}
    duplicates = {run_id: count for run_id, count in
                  ((run_id, len(ids)) for run_id, ids in evidence_ids.items()) if count > 1}
    return {"requeued": requeued, "skipped": skipped, "processed": processed,
            "statuses": statuses, "evidence_ids": evidence_ids,
            "duplicates": duplicates}


def verify_continuation(before: dict, after: dict, continuable_ids) -> list:
    """Assert no record loss and no duplicate evidence after a continuation pass."""
    problems = []
    for table in (users.name, cases.name):
        if before[table]["digest"] != after[table]["digest"]:
            problems.append(f"续办改变了 {table} 的内容")
    before_runs = {row["id"] for row in before[runs.name]["rows"]}
    after_runs = {row["id"] for row in after[runs.name]["rows"]}
    if before_runs != after_runs:
        problems.append("续办后执行记录集合发生变化")
    # Evidence rows may only be added (for the continuable runs); none may vanish.
    before_evidence = {row["id"]: row for row in before[evidence.name]["rows"]}
    after_evidence = {row["id"]: row for row in after[evidence.name]["rows"]}
    lost = sorted(set(before_evidence) - set(after_evidence))
    if lost:
        problems.append(f"续办丢失了既有证据：{lost}")
    changed = [row_id for row_id in before_evidence
               if row_id in after_evidence and before_evidence[row_id] != after_evidence[row_id]]
    if changed:
        problems.append(f"续办改写了既有证据：{changed}")
    per_run = {}
    for row in after[evidence.name]["rows"]:
        per_run[row["run_id"]] = per_run.get(row["run_id"], 0) + 1
    for run_id in continuable_ids:
        if per_run.get(run_id, 0) > 1:
            problems.append(f"执行 {run_id} 重复出证")
    for row in after[evidence.name]["rows"]:
        if hashlib.sha256(canonical(row["report"])).hexdigest() != row["digest"]:
            problems.append(f"证据 {row['id']} 校验码不一致")
            break
    return problems


def snapshot_rows(engine) -> dict:
    """Read the current table contents and per-table digests for before/after checks."""
    with engine.connect() as conn:
        rows = _table_rows(conn)
    return {name: {"rows": value, "digest": _digest(value)} for name, value in rows.items()}
