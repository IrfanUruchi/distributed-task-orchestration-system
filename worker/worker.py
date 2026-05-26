import subprocess
import sys
import os
import socket
import json
import urllib.request
import time
import threading
from concurrent import futures

import grpc
import psutil

from shared import orchestrator_pb2
from shared import orchestrator_pb2_grpc


def get_worker_address(port):
    worker_host = os.getenv(
        "WORKER_HOST",
        socket.gethostbyname(socket.gethostname())
    )

    return f"{worker_host}:{port}"


def get_worker_capabilities():
    raw = os.getenv("WORKER_CAPABILITIES", "GENERAL")

    capabilities = []
    for item in raw.split(","):
        capability = item.strip().upper()
        if capability and capability not in capabilities:
            capabilities.append(capability)

    return capabilities or ["GENERAL"]


def register_with_coordinator_once(port):
    coordinator_url = os.getenv("COORDINATOR_URL")

    if not coordinator_url:
        print("[REGISTER] COORDINATOR_URL not set, skipping auto-registration")
        return False

    worker_address = get_worker_address(port)

    capabilities = get_worker_capabilities()

    data = json.dumps({
        "address": worker_address,
        "capabilities": capabilities
    }).encode("utf-8")

    request = urllib.request.Request(
        f"{coordinator_url}/register",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            print(
                f"[REGISTER] Worker registered as {worker_address} "
                f"with capabilities={capabilities}"
            )
            print(response.read().decode("utf-8"))
            return True

    except Exception as e:
        print(f"[REGISTER] Failed to register worker: {e}")
        return False


def auto_register_loop(port):
    while True:
        registered = register_with_coordinator_once(port)

        if registered:
            time.sleep(10)
        else:
            time.sleep(3)


class WorkerService(orchestrator_pb2_grpc.WorkerServiceServicer):

    def __init__(self, worker_id):
        self.worker_id = worker_id

    def RunTask(self, request, context):

        print(
            f"[{self.worker_id}] TASK RECEIVED: {request.task_id} | "
            f"type={request.task_type} | "
            f"capability={request.required_capability}"
        )

        try:
            task_path = f"tasks/{request.task_type}.py"

            result = subprocess.run(
                ["python", task_path],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:

                return orchestrator_pb2.TaskResponse(
                    task_id=request.task_id,
                    status="completed",
                    result=result.stdout.strip(),
                    error=""
                )

            return orchestrator_pb2.TaskResponse(
                task_id=request.task_id,
                status="failed",
                result="",
                error=result.stderr.strip()
            )

        except Exception as e:

            return orchestrator_pb2.TaskResponse(
                task_id=request.task_id,
                status="failed",
                result="",
                error=str(e)
            )

    def HealthCheck(self, request, context):

        cpu = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory().percent

        return orchestrator_pb2.HealthResponse(
            worker_id=self.worker_id,
            alive=True,
            cpu_percent=cpu,
            memory_percent=memory
        )


def serve(port):

    worker_id = f"worker-{port}"

    threading.Thread(
        target=auto_register_loop,
        args=(port,),
        daemon=True
    ).start()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    orchestrator_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerService(worker_id),
        server
    )

    server.add_insecure_port(f"[::]:{port}")

    print(
        f"{worker_id} running on port {port} "
        f"with capabilities={get_worker_capabilities()}"
    )

    server.start()
    server.wait_for_termination()


if __name__ == "__main__":

    port = os.getenv("WORKER_PORT", "50051")

    if len(sys.argv) > 1:
        port = sys.argv[1]

    serve(port)