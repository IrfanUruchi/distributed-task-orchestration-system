import sqlite3
import time
import json
from pathlib import Path

import os
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "orchestrator.db"))


def now():
    return time.time()


def connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                payload TEXT,
                required_capability TEXT DEFAULT 'GENERAL',
                priority TEXT NOT NULL,
                status TEXT NOT NULL,
                scheduler_mode TEXT,
                final_worker TEXT,
                result TEXT,
                error TEXT,
                max_retries INTEGER DEFAULT 2,
                attempt_count INTEGER DEFAULT 0,
                created_at REAL,
                queued_at REAL,
                started_at REAL,
                finished_at REAL,
                updated_at REAL,
                duration_seconds REAL
            )
        """)

        existing_columns = [
            row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()
        ]

        if "required_capability" not in existing_columns:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN required_capability TEXT DEFAULT 'GENERAL'"
            )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                worker TEXT,
                status TEXT,
                error TEXT,
                started_at REAL,
                finished_at REAL,
                duration_seconds REAL
            )
        """)

        conn.commit()


def create_task(
    task_id,
    task_type,
    payload,
    priority,
    scheduler_mode,
    max_retries,
    required_capability="GENERAL"
):
    timestamp = now()

    with connect() as conn:
        conn.execute("""
            INSERT INTO tasks (
                task_id, task_type, payload, required_capability, priority, status,
                scheduler_mode, max_retries, created_at,
                queued_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task_id,
            task_type,
            payload,
            required_capability,
            priority,
            "PENDING",
            scheduler_mode,
            max_retries,
            timestamp,
            timestamp,
            timestamp
        ))

        conn.commit()


def get_task(task_id):
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?",
            (task_id,)
        ).fetchone()

        if not row:
            return None

        task = dict(row)
        task["attempts"] = get_attempts(task_id)
        return task


def get_attempts(task_id):
    with connect() as conn:
        rows = conn.execute("""
            SELECT * FROM task_attempts
            WHERE task_id = ?
            ORDER BY attempt ASC
        """, (task_id,)).fetchall()

        return [dict(row) for row in rows]


def list_tasks(limit=200):
    with connect() as conn:
        rows = conn.execute("""
            SELECT * FROM tasks
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

        tasks = []

        for row in rows:
            task = dict(row)
            task["attempts"] = get_attempts(task["task_id"])
            tasks.append(task)

        return tasks


def get_next_pending_task():
    with connect() as conn:
        row = conn.execute("""
            SELECT * FROM tasks
            WHERE status IN ('PENDING', 'RETRYING')
            ORDER BY
                CASE priority
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    WHEN 'LOW' THEN 3
                    ELSE 4
                END,
                queued_at ASC
            LIMIT 1
        """).fetchone()

        return dict(row) if row else None


def mark_running(task_id, worker, attempt):
    timestamp = now()

    with connect() as conn:
        conn.execute("""
            UPDATE tasks
            SET status = ?, final_worker = ?, started_at = COALESCE(started_at, ?),
                updated_at = ?, attempt_count = ?
            WHERE task_id = ?
        """, ("RUNNING", worker, timestamp, timestamp, attempt, task_id))

        conn.execute("""
            INSERT INTO task_attempts (
                task_id, attempt, worker, status, started_at
            )
            VALUES (?, ?, ?, ?, ?)
        """, (task_id, attempt, worker, "RUNNING", timestamp))

        conn.commit()


def mark_completed(task_id, worker, result, start_time):
    finish = now()

    with connect() as conn:
        conn.execute("""
            UPDATE tasks
            SET status = ?, final_worker = ?, result = ?, error = ?,
                finished_at = ?, updated_at = ?, duration_seconds = ?
            WHERE task_id = ?
        """, (
            "COMPLETED",
            worker,
            result,
            "",
            finish,
            finish,
            round(finish - start_time, 2),
            task_id
        ))

        conn.execute("""
            UPDATE task_attempts
            SET status = ?, finished_at = ?, duration_seconds = ?
            WHERE task_id = ?
            AND attempt = (
                SELECT MAX(attempt) FROM task_attempts WHERE task_id = ?
            )
        """, (
            "COMPLETED",
            finish,
            round(finish - start_time, 2),
            task_id,
            task_id
        ))

        conn.commit()


def mark_retrying(task_id, error, cooldown_seconds=3):
    timestamp = now()

    with connect() as conn:
        conn.execute("""
            UPDATE tasks
            SET status = ?, error = ?, queued_at = ?, updated_at = ?
            WHERE task_id = ?
        """, (
            "RETRYING",
            error,
            timestamp + cooldown_seconds,
            timestamp,
            task_id
        ))

        conn.execute("""
            UPDATE task_attempts
            SET status = ?, error = ?, finished_at = ?
            WHERE task_id = ?
            AND attempt = (
                SELECT MAX(attempt) FROM task_attempts WHERE task_id = ?
            )
        """, (
            "FAILED",
            error,
            timestamp,
            task_id,
            task_id
        ))

        conn.commit()


def mark_failed(task_id, error, start_time=None):
    finish = now()
    duration = round(finish - start_time, 2) if start_time else None

    with connect() as conn:
        conn.execute("""
            UPDATE tasks
            SET status = ?, error = ?, finished_at = ?, updated_at = ?,
                duration_seconds = COALESCE(duration_seconds, ?)
            WHERE task_id = ?
        """, ("FAILED", error, finish, finish, duration, task_id))

        conn.execute("""
            UPDATE task_attempts
            SET status = ?, error = ?, finished_at = ?,
                duration_seconds = COALESCE(duration_seconds, ?)
            WHERE task_id = ?
            AND attempt = (
                SELECT MAX(attempt) FROM task_attempts WHERE task_id = ?
            )
        """, ("FAILED", error, finish, duration, task_id, task_id))

        conn.commit()


def calculate_average_duration():
    with connect() as conn:
        row = conn.execute("""
            SELECT AVG(duration_seconds) AS avg_duration
            FROM tasks
            WHERE duration_seconds IS NOT NULL
        """).fetchone()

        return round(row["avg_duration"], 2) if row["avg_duration"] else 0
    
def mark_no_worker_retry(task_id, error, attempt, cooldown_seconds=5):
    timestamp = now()

    with connect() as conn:
        conn.execute("""
            UPDATE tasks
            SET status = ?, error = ?, queued_at = ?, updated_at = ?,
                attempt_count = ?
            WHERE task_id = ?
        """, (
            "RETRYING",
            error,
            timestamp + cooldown_seconds,
            timestamp,
            attempt,
            task_id
        ))

        conn.commit()