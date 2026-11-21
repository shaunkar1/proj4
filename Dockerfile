FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp .
RUN g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -o /shopping-core main.cpp

FROM --platform=linux/amd64 python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir duckdb==1.4.1
COPY --from=build /shopping-core /app/shopping-core
COPY write_parquet.py run.sh /app/
RUN chmod +x /app/run.sh && mkdir -p /scratch /output && chown -R 1000:1000 /scratch /output
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["/app/run.sh"]
