from fastapi import FastAPI, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc
import uuid
import asyncio
import time

from shared import orchestrator_pb2
from shared import orchestrator_pb2_grpc

from coordinator.worker_registry import WorkerRegistry
from coordinator.heartbeat import check_worker
from coordinator import storage


app = FastAPI()

templates = Jinja2Templates(directory="coordinator/templates")

app.mount(
    "/static",
    StaticFiles(directory="coordinator/static"),
    name="static"
)


class TaskRequest(BaseModel):
    task_type: str
    payload: str = ""
    priority: str = "MEDIUM"
    required_capability: str | None = None


class WorkerRegisterRequest(BaseModel):
    address: str
    capabilities: list[str] = ["GENERAL"]


class BenchmarkRequest(BaseModel):
    task_type: str
    count: int = 5
    priority: str = "MEDIUM"


MAX_RETRIES = 2
RETRY_COOLDOWN_SECONDS = 3
TASK_WAIT_TIMEOUT_SECONDS = 90

SCHEDULER_MODE = "load_aware"

TASK_CAPABILITY_MAP = {
    "quick_check": "GENERAL",
    "cpu_heavy": "CPU",
    "cpu_benchmark": "CPU",
    "data_processing": "MEMORY",
    "stress_test": "CPU",
}

ALLOWED_CAPABILITIES = {"GENERAL", "CPU", "MEMORY"}

registry = WorkerRegistry()


@app.on_event("startup")
async def startup_event():
    storage.init_db()
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(queue_loop())


async def heartbeat_loop():
    while True:
        for worker in registry.get_all_workers():
            check_worker(worker["address"], registry)

        await asyncio.sleep(5)


async def queue_loop():
    while True:
        task = storage.get_next_pending_task()

        if task:
            await asyncio.to_thread(process_queued_task, task)

        await asyncio.sleep(1)


def current_time():
    return time.time()


def resolve_required_capability(task_type, required_capability=None):
    capability = (
        required_capability
        or TASK_CAPABILITY_MAP.get(task_type, "GENERAL")
    )

    capability = capability.upper()

    if capability not in ALLOWED_CAPABILITIES:
        raise HTTPException(
            status_code=400,
            detail=f"Allowed capabilities: {sorted(ALLOWED_CAPABILITIES)}"
        )

    return capability


def normalize_priority(priority):
    priority = (priority or "MEDIUM").upper()

    if priority not in ["HIGH", "MEDIUM", "LOW"]:
        raise HTTPException(
            status_code=400,
            detail="Priority must be HIGH, MEDIUM, or LOW"
        )

    return priority


def select_worker(excluded_workers=None, required_capability=None):
    if excluded_workers is None:
        excluded_workers = set()

    if SCHEDULER_MODE == "round_robin":
        return registry.get_round_robin_worker(
            excluded_workers=excluded_workers,
            required_capability=required_capability
        )

    return registry.get_load_aware_worker(
        excluded_workers=excluded_workers,
        required_capability=required_capability
    )


def run_task_on_worker(worker_address, task_id, task):
    channel = grpc.insecure_channel(worker_address)
    stub = orchestrator_pb2_grpc.WorkerServiceStub(channel)

    return stub.RunTask(
        orchestrator_pb2.TaskRequest(
            task_id=task_id,
            task_type=task.task_type,
            payload=task.payload,
            required_capability=task.required_capability or "GENERAL"
        ),
        timeout=30
    )


def enqueue_task(task):
    task_id = str(uuid.uuid4())
    priority = normalize_priority(task.priority)
    required_capability = resolve_required_capability(
        task.task_type,
        task.required_capability
    )

    storage.create_task(
        task_id=task_id,
        task_type=task.task_type,
        payload=task.payload,
        priority=priority,
        scheduler_mode=SCHEDULER_MODE,
        max_retries=MAX_RETRIES,
        required_capability=required_capability
    )

    return {
        "task_id": task_id,
        "task_type": task.task_type,
        "status": "PENDING",
        "priority": priority,
        "required_capability": required_capability,
        "scheduler_mode": SCHEDULER_MODE,
        "message": "Task added to persistent queue"
    }


def process_queued_task(task):
    task_id = task["task_id"]
    attempt = int(task["attempt_count"]) + 1
    max_retries = int(task["max_retries"])

    required_capability = task.get("required_capability") or resolve_required_capability(
        task["task_type"]
    )

    selected_worker = select_worker(required_capability=required_capability)

    if selected_worker is None:
        if attempt <= max_retries:
            storage.mark_no_worker_retry(
                task_id=task_id,
                error=f"No healthy workers available for capability {required_capability}",
                attempt=attempt,
                cooldown_seconds=5
            )

            print(
                f"[RETRY] task={task_id} | no healthy workers | "
                f"attempt={attempt}/{max_retries}"
            )
        else:
            storage.mark_failed(
                task_id=task_id,
                error=(
                    "Task failed after retry limit: "
                    f"No healthy workers available for capability {required_capability}"
                )
            )

            print(
                f"[FAILED] task={task_id} | no healthy workers | "
                f"retry limit reached"
            )

        return

    worker_address = selected_worker["address"]
    start_time = current_time()

    registry.mark_task_started(worker_address)

    storage.mark_running(
        task_id=task_id,
        worker=worker_address,
        attempt=attempt
    )

    print(
        f"[QUEUE] task={task_id} | type={task['task_type']} | "
        f"priority={task['priority']} | attempt={attempt} | "
        f"worker={worker_address} | scheduler={SCHEDULER_MODE}"
    )

    try:
        task_obj = TaskRequest(
            task_type=task["task_type"],
            payload=task["payload"] or "",
            priority=task["priority"],
            required_capability=required_capability
        )

        response = run_task_on_worker(worker_address, task_id, task_obj)

        registry.mark_task_finished(worker_address)

        if response.status == "completed":
            storage.mark_completed(
                task_id=task_id,
                worker=worker_address,
                result=response.result,
                start_time=start_time
            )

            print(
                f"[COMPLETE] task={task_id} | worker={worker_address} | "
                f"duration={round(current_time() - start_time, 2)}s"
            )

            return

        handle_task_failure(
            task=task,
            task_id=task_id,
            worker_address=worker_address,
            error=response.error or "Worker returned failed status",
            start_time=start_time
        )

    except Exception as e:
        registry.mark_failed(worker_address)

        handle_task_failure(
            task=task,
            task_id=task_id,
            worker_address=worker_address,
            error=str(e),
            start_time=start_time
        )


def handle_task_failure(task, task_id, worker_address, error, start_time):
    attempt_count = int(task["attempt_count"]) + 1
    max_retries = int(task["max_retries"])

    registry.mark_task_finished(worker_address)

    if attempt_count <= max_retries:
        storage.mark_retrying(
            task_id=task_id,
            error=error,
            cooldown_seconds=RETRY_COOLDOWN_SECONDS
        )

        print(
            f"[RETRY] task={task_id} | attempt={attempt_count} | "
            f"max={max_retries} | worker={worker_address} | error={error}"
        )

        return

    storage.mark_failed(
        task_id=task_id,
        error="Task failed after retry limit: " + error,
        start_time=start_time
    )

    print(
        f"[FAILED] task={task_id} | retry limit reached | "
        f"worker={worker_address} | error={error}"
    )


def wait_for_task(task_id, timeout_seconds=TASK_WAIT_TIMEOUT_SECONDS):
    deadline = current_time() + timeout_seconds

    while current_time() < deadline:
        task = storage.get_task(task_id)

        if task and task["status"] in ["COMPLETED", "FAILED"]:
            return task

        time.sleep(0.4)

    task = storage.get_task(task_id)

    if not task:
        return {
            "task_id": task_id,
            "status": "FAILED",
            "error": "Task disappeared while waiting"
        }

    return task


def format_task_result(task):
    attempts = task.get("attempts", [])

    return {
        "task_id": task["task_id"],
        "selected_worker": task.get("final_worker"),
        "attempt": task.get("attempt_count", len(attempts)),
        "scheduler_mode": task.get("scheduler_mode"),
        "priority": task.get("priority"),
        "required_capability": task.get("required_capability"),
        "status": task.get("status"),
        "duration_seconds": task.get("duration_seconds"),
        "result": task.get("result") or "",
        "error": task.get("error") or "",
        "attempts": attempts
    }


@app.get("/")
def root():
    return {
        "status": "Coordinator running",
        "version": "V10 - Capability-aware persistent orchestration",
        "scheduler_mode": SCHEDULER_MODE,
        "workers": registry.get_all_workers()
    }


@app.post("/register")
def register_worker(worker: WorkerRegisterRequest):
    registered_worker = registry.register_worker(
        worker.address,
        worker.capabilities
    )

    return {
        "status": "registered",
        "worker": registered_worker
    }


@app.delete("/workers/{address}")
def remove_worker(address: str):
    removed = registry.remove_worker(address)

    if not removed:
        raise HTTPException(status_code=404, detail="Worker not found")

    return {
        "status": "removed",
        "address": address
    }


@app.get("/workers")
def list_workers():
    all_workers = registry.get_all_workers()
    healthy_workers = registry.get_alive_workers()

    return {
        "workers": all_workers,
        "total_workers": len(all_workers),
        "healthy_workers": len(healthy_workers),
        "unavailable_workers": len(all_workers) - len(healthy_workers)
    }


@app.get("/health")
def all_worker_health():
    return {
        "workers": registry.get_all_workers()
    }


@app.get("/scheduler/mode")
def get_scheduler_mode():
    return {
        "mode": SCHEDULER_MODE
    }


@app.post("/scheduler/mode/{mode}")
def set_scheduler_mode(mode: str):
    global SCHEDULER_MODE

    allowed_modes = ["round_robin", "load_aware"]

    if mode not in allowed_modes:
        raise HTTPException(
            status_code=400,
            detail=f"Allowed scheduler modes: {allowed_modes}"
        )

    SCHEDULER_MODE = mode

    return {
        "status": "updated",
        "mode": SCHEDULER_MODE
    }


@app.get("/tasks")
def list_tasks():
    tasks = storage.list_tasks()

    return {
        "tasks": tasks,
        "count": len(tasks),
        "average_duration_seconds": storage.calculate_average_duration()
    }


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    task = storage.get_task(task_id)

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return task


@app.post("/submit")
def submit_task(task: TaskRequest):
    return enqueue_task(task)


@app.post("/submit/wait")
def submit_task_and_wait(task: TaskRequest):
    queued = enqueue_task(task)
    finished_task = wait_for_task(queued["task_id"])

    return format_task_result(finished_task)


@app.post("/benchmark")
def run_benchmark(benchmark: BenchmarkRequest):
    if benchmark.count <= 0:
        raise HTTPException(
            status_code=400,
            detail="Benchmark count must be greater than 0"
        )

    if benchmark.count > 50:
        raise HTTPException(
            status_code=400,
            detail="Benchmark count should not be higher than 50"
        )

    priority = normalize_priority(benchmark.priority)
    benchmark_start = current_time()
    results = []

    for _ in range(benchmark.count):
        queued = enqueue_task(
            TaskRequest(
                task_type=benchmark.task_type,
                payload=benchmark.task_type,
                priority=priority
            )
        )

        finished_task = wait_for_task(queued["task_id"])
        results.append(format_task_result(finished_task))

    benchmark_finish = current_time()

    completed = len([
        result for result in results
        if result.get("status") == "COMPLETED"
    ])

    failed = len(results) - completed

    durations = [
        result["duration_seconds"] for result in results
        if result.get("duration_seconds") is not None
    ]

    average_duration = (
        round(sum(durations) / len(durations), 2)
        if durations else 0
    )

    return {
        "workload": benchmark.task_type,
        "scheduler_mode": SCHEDULER_MODE,
        "execution_mode": "sequential",
        "priority": priority,
        "tasks_submitted": benchmark.count,
        "completed": completed,
        "failed": failed,
        "total_time_seconds": round(
            benchmark_finish - benchmark_start,
            2
        ),
        "average_task_duration_seconds": average_duration,
        "results": results
    }


@app.post("/benchmark/parallel")
def run_parallel_benchmark(benchmark: BenchmarkRequest):
    if benchmark.count <= 0:
        raise HTTPException(
            status_code=400,
            detail="Benchmark count must be greater than 0"
        )

    if benchmark.count > 50:
        raise HTTPException(
            status_code=400,
            detail="Benchmark count should not be higher than 50"
        )

    priority = normalize_priority(benchmark.priority)
    benchmark_start = current_time()
    queued_tasks = []

    for _ in range(benchmark.count):
        queued_tasks.append(
            enqueue_task(
                TaskRequest(
                    task_type=benchmark.task_type,
                    payload=benchmark.task_type,
                    priority=priority
                )
            )
        )

    results = []

    with ThreadPoolExecutor(max_workers=benchmark.count) as executor:
        futures = [
            executor.submit(wait_for_task, task["task_id"])
            for task in queued_tasks
        ]

        for future in as_completed(futures):
            results.append(format_task_result(future.result()))

    benchmark_finish = current_time()

    completed = len([
        result for result in results
        if result.get("status") == "COMPLETED"
    ])

    failed = len(results) - completed

    durations = [
        result["duration_seconds"]
        for result in results
        if result.get("duration_seconds") is not None
    ]

    average_duration = (
        round(sum(durations) / len(durations), 2)
        if durations else 0
    )

    return {
        "workload": benchmark.task_type,
        "scheduler_mode": SCHEDULER_MODE,
        "execution_mode": "parallel",
        "priority": priority,
        "tasks_submitted": benchmark.count,
        "completed": completed,
        "failed": failed,
        "total_time_seconds": round(
            benchmark_finish - benchmark_start,
            2
        ),
        "average_task_duration_seconds": average_duration,
        "results": results
    }


@app.post("/benchmark/compare")
def compare_schedulers(benchmark: BenchmarkRequest):
    global SCHEDULER_MODE

    if benchmark.count <= 0:
        raise HTTPException(
            status_code=400,
            detail="Benchmark count must be greater than 0"
        )

    if benchmark.count > 50:
        raise HTTPException(
            status_code=400,
            detail="Benchmark count should not be higher than 50"
        )

    previous_mode = SCHEDULER_MODE

    try:
        SCHEDULER_MODE = "round_robin"
        round_robin_result = run_benchmark(benchmark)

        SCHEDULER_MODE = "load_aware"
        load_aware_result = run_benchmark(benchmark)

    finally:
        SCHEDULER_MODE = previous_mode

    rr_avg = round_robin_result["average_task_duration_seconds"]
    la_avg = load_aware_result["average_task_duration_seconds"]

    if rr_avg == la_avg:
        winner = "tie"
    elif rr_avg < la_avg:
        winner = "round_robin"
    else:
        winner = "load_aware"

    return {
        "workload": benchmark.task_type,
        "tasks_per_scheduler": benchmark.count,
        "priority": normalize_priority(benchmark.priority),
        "round_robin": round_robin_result,
        "load_aware": load_aware_result,
        "winner": winner,
        "previous_scheduler_restored": previous_mode
    }


@app.get("/dashboard")
def dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html"
    )
