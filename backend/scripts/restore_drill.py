"""Run a whole-database restore drill into an isolated database.

Example::

    DRILL_TARGET_DB=workflow_drill \\
    python -m scripts.restore_drill --output work/restore-drill

The script never writes to the source database.  It refuses a target database
that is the source, recreates the isolated copy from a single exported
snapshot, verifies it against that cut, then requeues only the runs that had no
evidence at the cut.  Every step is written to the manifest so a failed or
retried drill leaves an audit trail instead of losing records.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine

from src.config.settings import engine as source_engine
from src.services import restore
from src.utils.time import utc_time


def _write(path: Path, manifest: dict, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8")
    (path.parent / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _target_url(source_url):
    database = os.getenv("DRILL_TARGET_DB", f"{source_url.database}_drill")
    override = os.getenv("DRILL_TARGET_URL")
    if override:
        from sqlalchemy.engine import make_url
        return make_url(override)
    return source_url.set(database=database)


def _summary(manifest: restore.Manifest) -> dict:
    return {
        "drill_id": manifest.drill_id,
        "result": manifest.result,
        "reasons": manifest.reasons,
        "anomalies": manifest.verify.get("anomalies", []),
        "cutoff": manifest.cutoff,
        "isolated_target": manifest.target,
        "restore_attempts": manifest.restore.get("attempts", []),
        "counts_match": manifest.verify.get("counts_match"),
        "digests_match": manifest.verify.get("digests_match"),
        "source_drift_since_cut": manifest.verify.get("source_drift_since_cut"),
        "snapshot_counts": manifest.snapshot_counts,
        "evidence_integrity": manifest.verify.get("evidence"),
        "not_yet_evidenced_at_cutoff": manifest.not_yet_evidenced_at_cutoff,
        "continuable_runs": manifest.continuable_runs,
        "continue_evidence_ids": _continue_evidence_ids(manifest),
        "resume": manifest.resume,
    }


def run(output: Path, retries: int, resume: bool) -> dict:
    source_url = source_engine.url
    target_url = _target_url(source_url)
    restore.ensure_isolated(source_url, target_url)

    drill_id = f"{utc_time().replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}"
    dump_path = output / f"dump-{drill_id}.dump"
    manifest = restore.Manifest(
        drill_id=drill_id,
        source={"database": source_url.database,
                "host": source_url.host or "local-socket"},
        cutoff={}, dump={}, snapshot_counts={}, snapshot_digests={})
    manifest.target = {"database": target_url.database,
                       "host": target_url.host or "local-socket"}

    try:
        cutoff, counts, digests, classified = restore.capture_snapshot(source_engine, dump_path)
        manifest.cutoff = restore.asdict_cutoff(cutoff)
        manifest.snapshot_counts = counts
        manifest.snapshot_digests = digests
        # Record the pending/continuable answer from the cut itself, so a failed
        # restore still tells operators which runs had not yet issued evidence.
        manifest.not_yet_evidenced_at_cutoff = classified["not_yet_evidenced_at_cutoff"]
        manifest.continuable_runs = classified["continuable_runs"]
        manifest.dump = {"path": str(dump_path), "bytes": dump_path.stat().st_size,
                         "format": "custom"}

        manifest.restore = restore.restore_database(
            source_url, target_url, dump_path, retries=retries)
        target_engine = create_engine(target_url, pool_pre_ping=True)
        try:
            source_now = restore.snapshot_rows(source_engine)
            verify = restore.reconcile(target_engine, cutoff, counts, digests,
                                       source_snapshot=source_now)
            manifest.verify = verify
            manifest.not_yet_evidenced_at_cutoff = verify["not_yet_evidenced_at_cutoff"]
            manifest.continuable_runs = verify["continuable_runs"]
            continuable_ids = [row["run_id"] for row in verify["continuable_runs"]]
            if verify["reasons"]:
                manifest.result = "failed"
                manifest.reasons = list(verify["reasons"])
            elif not resume:
                manifest.result = "verified"
            else:
                before = restore.snapshot_rows(target_engine)
                manifest.resume = restore.resume_continuable(target_engine, continuable_ids)
                after = restore.snapshot_rows(target_engine)
                problems = restore.verify_continuation(before, after, continuable_ids)
                manifest.resume["integrity"] = {"ok": not problems, "problems": problems}
                if problems:
                    manifest.result = "failed"
                    manifest.reasons.extend(problems)
                else:
                    manifest.result = "verified"
        finally:
            target_engine.dispose()
    except restore.DrillError as exc:
        # Persist the partial manifest before re-raising: a failed drill must still
        # leave an audit trail of how far it got and which runs were pending.
        manifest.result = "failed"
        manifest.reasons.append(str(exc))
        _write(output / f"manifest-{drill_id}.json", manifest.as_dict(), _summary(manifest))
        raise

    result = _summary(manifest)
    _write(output / f"manifest-{drill_id}.json", manifest.as_dict(), result)
    return result


def _continue_evidence_ids(manifest) -> list:
    """Evidence numbers that may continue, keyed back to their run and case."""
    resume = manifest.resume or {}
    evidence_ids = resume.get("evidence_ids", {})
    statuses = resume.get("statuses", {})
    rows = {row["run_id"]: row for row in manifest.continuable_runs}
    answer = []
    for run_id, ids in sorted(evidence_ids.items()):
        row = rows.get(run_id, {})
        answer.append({
            "run_id": run_id,
            "case_id": row.get("case_id"),
            "status_at_cutoff": row.get("status"),
            "status_now": statuses.get(run_id),
            "evidence_ids": ids,
        })
    return answer


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=os.getenv("DRILL_OUTPUT", "work/restore-drill"),
                        help="directory for the dump, manifest and result")
    parser.add_argument("--retries", type=int, default=3, help="restore attempts")
    parser.add_argument("--no-resume", dest="resume", action="store_false",
                        help="verify only; do not continue pending runs in the copy")
    options = parser.parse_args(argv)
    try:
        result = run(Path(options.output), max(1, options.retries), options.resume)
    except restore.DrillError as exc:
        print(json.dumps({"result": "aborted", "reason": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["result"] == "verified" else 1


if __name__ == "__main__":
    sys.exit(main())
