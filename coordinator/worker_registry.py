import time


class WorkerRegistry:
    def __init__(self, cooldown_seconds=15):
        self.cooldown_seconds = cooldown_seconds
        self.workers = {}
        self.round_robin_index = 0
        self.last_load_aware_worker = None

    def normalize_capabilities(self, capabilities=None):
        if not capabilities:
            return ["GENERAL"]

        normalized = []
        for capability in capabilities:
            value = str(capability).strip().upper()
            if value and value not in normalized:
                normalized.append(value)

        return normalized or ["GENERAL"]

    def register_worker(self, address, capabilities=None):
        normalized_capabilities = self.normalize_capabilities(capabilities)

        if address not in self.workers:
            self.workers[address] = {
                "address": address,
                "state": "HEALTHY",
                "alive": True,
                "cpu_percent": 0.0,
                "memory_percent": 0.0,
                "score": 0.0,
                "last_heartbeat": None,
                "last_failure": None,
                "failed_count": 0,
                "active_tasks": 0,
                "completed_tasks": 0,
                "last_selected": None,
                "capabilities": normalized_capabilities
            }

        else:
            self.workers[address]["capabilities"] = normalized_capabilities

        return self.workers[address]

    def remove_worker(self, address):
        if address in self.workers:
            del self.workers[address]
            return True
        return False

    def calculate_score(self, worker):
        cpu_score = worker["cpu_percent"]
        memory_score = worker["memory_percent"]
        active_task_penalty = worker["active_tasks"] * 25

        recent_selection_penalty = 0
        if worker["last_selected"] is not None:
            seconds_since_selected = time.time() - worker["last_selected"]

            if seconds_since_selected < 3:
                recent_selection_penalty = 80
            elif seconds_since_selected < 8:
                recent_selection_penalty = 40
            elif seconds_since_selected < 15:
                recent_selection_penalty = 15

        return (
            cpu_score
            + memory_score
            + active_task_penalty
            + recent_selection_penalty
        )

    def refresh_worker_score(self, address):
        if address not in self.workers:
            return

        worker = self.workers[address]
        worker["score"] = self.calculate_score(worker)

    def mark_alive(self, address, cpu_percent, memory_percent):
        if address not in self.workers:
            self.register_worker(address)

        worker = self.workers[address]

        worker["state"] = "HEALTHY"
        worker["alive"] = True
        worker["cpu_percent"] = cpu_percent
        worker["memory_percent"] = memory_percent
        worker["score"] = self.calculate_score(worker)
        worker["last_heartbeat"] = time.time()
        worker["failed_count"] = 0

    def mark_failed(self, address):
        if address not in self.workers:
            return

        worker = self.workers[address]

        worker["state"] = "UNAVAILABLE"
        worker["alive"] = False
        worker["last_failure"] = time.time()
        worker["failed_count"] += 1

        if worker["active_tasks"] > 0:
            worker["active_tasks"] -= 1

        self.refresh_worker_score(address)

    def mark_task_started(self, address):
        if address not in self.workers:
            return

        worker = self.workers[address]

        worker["active_tasks"] += 1
        worker["last_selected"] = time.time()

        self.refresh_worker_score(address)

    def mark_task_finished(self, address):
        if address not in self.workers:
            return

        worker = self.workers[address]

        if worker["active_tasks"] > 0:
            worker["active_tasks"] -= 1

        worker["completed_tasks"] += 1

        self.refresh_worker_score(address)

    def can_probe(self, address):
        if address not in self.workers:
            return False

        worker = self.workers[address]

        if worker["state"] == "HEALTHY":
            return True

        if worker["last_failure"] is None:
            return True

        return time.time() - worker["last_failure"] >= self.cooldown_seconds

    def mark_recovering(self, address):
        if address in self.workers:
            self.workers[address]["state"] = "RECOVERING"

    def get_all_workers(self):
        for address in self.workers:
            self.refresh_worker_score(address)

        return list(self.workers.values())

    def get_alive_workers(self):
        for address in self.workers:
            self.refresh_worker_score(address)

        return [
            worker for worker in self.workers.values()
            if worker["state"] == "HEALTHY"
        ]

    def worker_supports_capability(self, worker, required_capability=None):
        if not required_capability:
            return True

        return required_capability.upper() in worker.get("capabilities", ["GENERAL"])

    def get_capable_workers(self, required_capability=None, excluded_workers=None):
        if excluded_workers is None:
            excluded_workers = set()

        return [
            worker for worker in self.get_alive_workers()
            if worker["address"] not in excluded_workers
            and self.worker_supports_capability(worker, required_capability)
        ]

    def get_round_robin_worker(self, excluded_workers=None, required_capability=None):
        if excluded_workers is None:
            excluded_workers = set()

        workers = self.get_capable_workers(required_capability, excluded_workers)

        if not workers:
            return None

        worker = workers[self.round_robin_index % len(workers)]

        self.round_robin_index += 1

        return worker

    def get_load_aware_worker(self, excluded_workers=None, required_capability=None):
        if excluded_workers is None:
            excluded_workers = set()

        workers = self.get_capable_workers(required_capability, excluded_workers)

        if not workers:
            return None

        sorted_workers = sorted(
            workers,
            key=lambda worker: worker["score"]
        )

        best_worker = sorted_workers[0]

        if (
            self.last_load_aware_worker == best_worker["address"]
            and len(sorted_workers) > 1
        ):
            second_worker = sorted_workers[1]

            score_gap = second_worker["score"] - best_worker["score"]

            if score_gap < 25:
                best_worker = second_worker

        self.last_load_aware_worker = best_worker["address"]

        return best_worker