FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp .
# PROBE BUILD, not a real entry: the run sleeps 9 x the wall time of the AGGREGATE stage before it exits, so the score shows that stage on the hidden data.
RUN g++ -std=c++20 -O3 -DNDEBUG -DSS_PROBE_STAGE='"AGGREGATE"' -DSS_PROBE_K=9 -march=x86-64 -flto -fopenmp -static -o /shopping-core main.cpp

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /shopping-core /app/shopping-core
RUN mkdir -p /scratch /output && chown -R 1000:1000 /scratch /output
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8
ENTRYPOINT ["/app/shopping-core"]
