FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY worker ./worker
COPY tasks ./tasks

ENV PYTHONPATH=/app
ENV WORKER_PORT=50051
ENV COORDINATOR_URL=
ENV WORKER_HOST=

EXPOSE 50051

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD python -c "import socket, os; s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1', int(os.getenv('WORKER_PORT', '50051')))); s.close()"

CMD ["sh", "-c", "python worker/worker.py $WORKER_PORT"]