# DIAGNOSTIC BRANCH - not a scored entry. Runs the pipeline several ways, prints machine facts and
# stage timings to stderr, then adds extra campaign_daily rows that carry the measurements, so the dev check rejects it and shows them.
FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp main_ring.cpp main_bkt.cpp diag.cpp ./
RUN F="-std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp"; \
    g++ $F -o /shopping-core main.cpp & g++ $F -o /core-ring main_ring.cpp & g++ $F -o /core-bkt main_bkt.cpp & g++ -std=c++20 -O2 -pthread -o /diag diag.cpp & wait; \
    test -x /shopping-core && test -x /core-ring && test -x /core-bkt && test -x /diag

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY --from=build /shopping-core /app/shopping-core
COPY --from=build /core-ring /app/core-ring
COPY --from=build /core-bkt /app/core-bkt
COPY --from=build /diag /app/diag
RUN mkdir -p /scratch /output && chown -R 1000:1000 /scratch /output
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8
ENTRYPOINT ["/app/diag"]
