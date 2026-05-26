import grpc

from shared import orchestrator_pb2
from shared import orchestrator_pb2_grpc


def check_worker(address, registry):
    if not registry.can_probe(address):
        return

    try:
        if registry.workers[address]["state"] == "UNAVAILABLE":
            registry.mark_recovering(address)

        channel = grpc.insecure_channel(address)
        stub = orchestrator_pb2_grpc.WorkerServiceStub(channel)

        response = stub.HealthCheck(
            orchestrator_pb2.HealthRequest(worker_id=address),
            timeout=3
        )

        registry.mark_alive(
            address,
            response.cpu_percent,
            response.memory_percent
        )

        print(
            f"[HEARTBEAT] {address} alive | "
            f"CPU={response.cpu_percent:.1f}% | "
            f"MEM={response.memory_percent:.1f}%"
        )

    except Exception as e:
        registry.mark_failed(address)
        print(f"[HEARTBEAT] {address} failed | {e}")