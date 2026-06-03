# Distributed Task Orchestration System

A lightweight distributed task orchestration system developed using Python, FastAPI, gRPC, Docker, Docker Compose and SQLite.

This project was developed as a Capstone Project at South East European University, Faculty of Contemporary Sciences and Technologies.

## Project Overview

The goal of this project was to design and implement a distributed orchestration system that can execute workloads across multiple worker nodes instead of depending on only one machine.

The system follows a coordinator-worker architecture. The coordinator receives tasks, stores them, tracks workers, makes scheduling decisions and monitors the state of the system. Worker nodes register to the coordinator, report their health and resource usage, execute tasks and return results.

The project intentionally keeps the architecture lightweight. It is not trying to replace systems such as Kubernetes, Apache Airflow or Celery. Instead, it focuses on the core ideas behind distributed orchestration, including scheduling, worker monitoring, fault handling, persistent task queues, retry handling and capability-aware routing.

## Main Features

The final Version 10 system includes distributed task execution, dynamic worker registration, round-robin scheduling, load-aware scheduling, capability-aware scheduling, heartbeat-based worker monitoring, retry handling, persistent SQLite task storage, task attempt history, automatic worker re-registration, Docker deployment, Docker Compose support and a dashboard for monitoring and benchmark testing.

The system supports both local Docker-based testing and distributed testing across multiple physical devices connected through a local network.

## System Architecture

The system uses a centralized coordinator-worker model.

The coordinator is the main control component. It handles task submission, task storage, worker registry, scheduling, queue processing, retry handling, benchmark execution and dashboard data.

The worker nodes are responsible for task execution. Each worker starts a gRPC server, registers itself to the coordinator, sends heartbeat information and reports CPU and memory usage. When the coordinator assigns a task, the worker executes the matching workload script and returns the result.

FastAPI is used for external REST communication and dashboard interaction. gRPC and Protocol Buffers are used for internal communication between the coordinator and worker nodes. SQLite is used for persistent task storage.

## Version 10 Summary

Version 10 is the final version of the system. It adds capability-aware scheduling on top of the previous persistent queue and retry system.

Before Version 10, the coordinator could check whether a worker was healthy and how much CPU or memory it was using, but it did not know if that worker was actually suitable for a specific workload. Version 10 fixes this by allowing workers to register with capabilities such as GENERAL, CPU and MEMORY.

When a task is submitted, the coordinator first checks the required capability of the task. It then filters the worker list and only keeps workers that support that capability. After that, it applies the selected scheduling strategy.

This means CPU-based tasks are assigned only to workers that support CPU workloads, while memory-based tasks are assigned only to workers that support memory workloads.

## Scheduling

The system supports multiple scheduling approaches.

Round-robin scheduling distributes tasks in order between eligible workers. This is simple and useful when workers have similar resources.

Load-aware scheduling checks CPU usage, memory usage and active task count before assigning a task. This helps the coordinator avoid sending too much work to workers that are already busy.

Capability-aware scheduling adds another step before normal scheduling. The coordinator first checks whether the worker supports the required task capability. Only valid workers are then passed to the scheduler.

## Persistent Queue and Task Lifecycle

Version 9 added a persistent SQLite queue. Instead of sending a task directly to a worker and waiting for the result, the coordinator first stores the task in SQLite with a PENDING state.

A background queue loop then picks pending tasks, selects a valid worker and sends the task for execution. During execution, the task can move through states such as PENDING, RUNNING, COMPLETED, FAILED and RETRYING.

The coordinator also stores task attempts. Every attempt contains information about the selected worker, status, start time, finish time, duration and possible error message. This makes task history easier to inspect and makes retry behavior easier to prove.

## Fault Tolerance and Recovery

The system uses heartbeat monitoring to detect worker failures. Workers send regular updates to the coordinator. If a worker stops responding, the coordinator marks it as unavailable and skips it during scheduling.

If a task fails or no healthy worker is available, the coordinator can mark the task as RETRYING and try again later until the retry limit is reached.

Workers also support automatic re-registration. If the coordinator restarts, workers keep trying to register again in the background. When the coordinator becomes available again, the workers reconnect without manual restart.

## Dashboard

The project includes a dashboard for monitoring and testing the system from the browser.

The dashboard shows worker status, worker capabilities, CPU usage, memory usage, scheduling score, task history, task attempts, benchmark results and scheduler comparison results.

It can also be used to submit workloads, switch scheduling mode, run benchmarks and observe worker distribution.

## Technologies Used

The system was developed mainly using Python because it makes networking, process execution and monitoring practical to implement.

FastAPI is used for REST API endpoints and dashboard communication. gRPC is used for internal coordinator-worker communication. Protocol Buffers define the messages exchanged between services. SQLite stores persistent task data. Docker and Docker Compose are used for deployment and testing. The psutil library is used for worker CPU and memory monitoring.

## Repository Structure

```text
coordinator/         Coordinator service, scheduler, queue logic and dashboard
worker/              Worker node implementation and gRPC service
proto/               Protocol Buffer definitions
shared/              Shared utilities and database logic
tasks/               Example workload scripts
data/                Local SQLite storage folder
docker-compose.yml   Docker Compose deployment configuration
requirements.txt     Python dependencies
README.md            Project documentation
```

## Requirements

For the Docker setup, install Docker and Docker Compose.

On Ubuntu, Docker can be installed using:

```bash
sudo apt update
sudo apt install docker.io docker-compose-plugin -y
sudo systemctl enable docker
sudo systemctl start docker
```

To run Docker commands without sudo:

```bash
sudo usermod -aG docker $USER
newgrp docker
```

On macOS or Windows, Docker Desktop can be used.

For running directly from source, Python 3.11 or newer is recommended.

## Local Docker Compose Deployment

The repository already includes a `docker-compose.yml` file. This is the easiest way to run the system locally because it starts the coordinator, worker nodes, shared Docker network and persistent SQLite storage together.

First clone the repository:

```bash
git clone https://github.com/IrfanUruchi/distributed-task-orchestration-system.git
cd distributed-task-orchestration-system
```

Create the local data folder used by SQLite:

```bash
mkdir -p data
```

Start the full local cluster:

```bash
docker compose up --build
```

After the containers start, open the dashboard:

```text
http://localhost:8000/dashboard
```

The workers should register automatically to the coordinator. The dashboard can then be used to submit tasks, check worker status, run benchmarks and compare schedulers.

To stop the local cluster:

```bash
docker compose down
```

This stops the containers but keeps the `data` folder, so the SQLite task history remains available.

To reset the local environment completely:

```bash
docker compose down
rm -rf data
mkdir -p data
docker compose up --build
```

This removes the old SQLite database and starts the system again with a clean state.

## Local Docker Compose Limitations

Docker Compose is useful for development, testing and demonstration, but all containers still run on the same physical machine.

This means all worker containers share the same CPU, memory, disk and network interface. Because of that, benchmark results may not fully represent real distributed performance.

Local Docker Compose testing is best for checking worker registration, queue handling, scheduling logic, dashboard behavior, retry handling and general system flow.

For more realistic evaluation, the distributed network setup should be used with workers running on separate physical devices or virtual machines.

## Running from Published Docker Images

The project is also published as separate Docker images.

Coordinator image:

```text
irfanuruchi/distributed-orchestrator-coordinator:v10
```

Worker image:

```text
irfanuruchi/distributed-orchestrator-worker:v10
```

This option is useful when running the coordinator and workers manually or when deploying workers across different machines.

First create the Docker network and data folder:

```bash
docker network create orchestrator-net
mkdir -p data
```

Start the coordinator:

```bash
docker run -d \
  --name orchestrator-coordinator \
  --network orchestrator-net \
  -p 8000:8000 \
  -v "$(pwd)/data:/app/data" \
  -e DB_PATH=/app/data/orchestrator.db \
  irfanuruchi/distributed-orchestrator-coordinator:v10
```

Open the dashboard:

```text
http://localhost:8000/dashboard
```

Start a CPU-capable worker:

```bash
docker run -d \
  --name worker-50051 \
  --network orchestrator-net \
  -e WORKER_HOST=worker-50051 \
  -e WORKER_PORT=50051 \
  -e WORKER_CAPABILITIES=GENERAL,CPU \
  -e COORDINATOR_URL=http://orchestrator-coordinator:8000 \
  irfanuruchi/distributed-orchestrator-worker:v10
```

Start a memory-capable worker:

```bash
docker run -d \
  --name worker-50052 \
  --network orchestrator-net \
  -e WORKER_HOST=worker-50052 \
  -e WORKER_PORT=50052 \
  -e WORKER_CAPABILITIES=GENERAL,MEMORY \
  -e COORDINATOR_URL=http://orchestrator-coordinator:8000 \
  irfanuruchi/distributed-orchestrator-worker:v10
```

Start a mixed-capability worker:

```bash
docker run -d \
  --name worker-50053 \
  --network orchestrator-net \
  -e WORKER_HOST=worker-50053 \
  -e WORKER_PORT=50053 \
  -e WORKER_CAPABILITIES=GENERAL,CPU,MEMORY \
  -e COORDINATOR_URL=http://orchestrator-coordinator:8000 \
  irfanuruchi/distributed-orchestrator-worker:v10
```

After starting the workers, refresh the dashboard. The coordinator should show the registered workers and their capabilities.

## Distributed Network Deployment

For a more realistic setup, the coordinator can run on one machine and workers can run on separate devices connected to the same local network.

In the final project evaluation, the coordinator was deployed on a Proxmox Ubuntu virtual machine and workers were deployed on separate physical devices. This made the testing more realistic because each worker had its own CPU and memory resources.

Start the coordinator on the main machine:

```bash
mkdir -p data
```

```bash
docker run -d \
  --name orchestrator-coordinator \
  -p 8000:8000 \
  -v "$(pwd)/data:/app/data" \
  -e DB_PATH=/app/data/orchestrator.db \
  irfanuruchi/distributed-orchestrator-coordinator:v10
```

The dashboard will be available at:

```text
http://COORDINATOR_IP:8000/dashboard
```

Example:

```text
http://192.168.0.205:8000/dashboard
```

On each worker machine, start a worker and point it to the coordinator IP address.

Example CPU worker:

```bash
docker run -d \
  --name worker-cpu \
  -p 50052:50052 \
  -e WORKER_HOST=WORKER_MACHINE_IP \
  -e WORKER_PORT=50052 \
  -e WORKER_CAPABILITIES=GENERAL,CPU \
  -e COORDINATOR_URL=http://192.168.0.205:8000 \
  irfanuruchi/distributed-orchestrator-worker:v10
```

Example memory worker:

```bash
docker run -d \
  --name worker-memory \
  -p 50053:50053 \
  -e WORKER_HOST=WORKER_MACHINE_IP \
  -e WORKER_PORT=50053 \
  -e WORKER_CAPABILITIES=GENERAL,MEMORY \
  -e COORDINATOR_URL=http://192.168.0.205:8000 \
  irfanuruchi/distributed-orchestrator-worker:v10
```

Example mixed worker:

```bash
docker run -d \
  --name worker-mixed \
  -p 50054:50054 \
  -e WORKER_HOST=WORKER_MACHINE_IP \
  -e WORKER_PORT=50054 \
  -e WORKER_CAPABILITIES=GENERAL,CPU,MEMORY \
  -e COORDINATOR_URL=http://192.168.0.205:8000 \
  irfanuruchi/distributed-orchestrator-worker:v10
```

Replace `WORKER_MACHINE_IP` with the actual IP address of the worker device.

Example:

```text
192.168.0.243
```

The coordinator machine must allow access to port `8000`. Each worker machine must expose its gRPC port, such as `50052`, `50053` or `50054`.

Workers must be able to reach the coordinator using `COORDINATOR_URL`, and the coordinator must be able to reach the worker address defined in `WORKER_HOST` and `WORKER_PORT`.

If workers do not appear in the dashboard, check the IP addresses, firewall settings, Docker port mappings and whether all machines are connected to the same network.

## Running from Source

The system can also be run directly from source code.

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the coordinator:

```bash
cd coordinator
python main.py
```

Start a worker in another terminal:

```bash
cd worker
python worker.py
```

For distributed source testing, start the coordinator on one machine and configure workers on other machines to use the coordinator IP address.

## Example Workloads

The system includes multiple workloads used during testing.

The `quick_check` workload is used for simple communication and execution testing. The `cpu_benchmark` and `cpu_heavy` workloads are used to test CPU-based execution. The `data_processing` workload is used for memory-oriented testing. The `stress_test` workload is used to observe behavior under heavier load.

These workloads are also used for scheduler comparison and capability-aware routing.

## Useful API Commands

Submit a CPU benchmark task:

```bash
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{"task_type":"cpu_benchmark","payload":"cpu_benchmark","priority":"MEDIUM"}'
```

Check registered workers:

```bash
curl http://localhost:8000/workers
```

Check task history:

```bash
curl http://localhost:8000/tasks
```

Run a benchmark:

```bash
curl -X POST http://localhost:8000/benchmark \
  -H "Content-Type: application/json" \
  -d '{"task_type":"cpu_benchmark","count":5,"priority":"MEDIUM"}'
```

## Evaluation Setup

The final evaluation used one coordinator and three worker nodes across the local network.

The coordinator was deployed on a Proxmox Ubuntu virtual machine. The worker nodes were deployed on separate physical devices with different capabilities. This allowed the scheduler to choose between workers based on both worker health and workload capability.

The final tests included scheduler comparison, CPU-heavy benchmarks, data processing workloads, stress testing, sequential and parallel benchmark execution, worker failure detection and worker recovery.

## Limitations

The current system is designed as a lightweight capstone implementation, not as a production-ready orchestration platform.

The coordinator is still a single node, so a larger deployment would need backup coordinator support or leader election. The system was tested inside a trusted local network, so TLS and worker authentication were not added in this version.

Local Docker Compose testing is useful but limited because all workers share the same host resources. More realistic performance testing requires multiple physical devices or virtual machines.

SQLite worked well for this project because it kept the system lightweight and persistent, but larger workloads could benefit from a message broker such as Redis or RabbitMQ.

## Future Improvements

Future improvements could include TLS support, worker authentication, backup coordinator support, leader election, Redis or RabbitMQ integration, distributed state replication and larger cluster testing.

The scheduler could also be improved by using more worker information, such as previous execution time, hardware type, network latency and failure history.

## Author

Irfan Uruchi

South East European University
Faculty of Contemporary Sciences and Technologies

Capstone Project 2026
