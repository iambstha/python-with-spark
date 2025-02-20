FROM debian:bullseye-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-venv \
    python3-dev \
    gcc \
    g++ \
    libpq-dev \
    curl \
    gnupg2 \
    ca-certificates \
    lsb-release \
    && apt-get clean

RUN python3 -m venv /app/venv
RUN /app/venv/bin/pip install --upgrade pip
RUN /app/venv/bin/pip install pyspark psutil python-dotenv

COPY . /app

EXPOSE 4040 7077 8080

CMD ["/app/venv/bin/python", "process_file.py"]
