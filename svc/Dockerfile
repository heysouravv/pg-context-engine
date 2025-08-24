FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml and install dependencies using pip
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

# Copy the service code
COPY svc/ .

CMD ["python", "-c", "print('edge-svc image ready')"]
