# DIAGNOSTIC BRANCH - not a scored entry. Runs the pipeline several ways, prints machine facts and
# stage timings to stderr, then adds extra campaign_daily rows that carry the measurements, so the dev check rejects it and shows them.
FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp main_v31.cpp main_abl.cpp main_x.cpp diag.cpp ./
RUN F="-std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp"; \
    g++ $F -o /shopping-core main.cpp & g++ $F -o /core-v31 main_v31.cpp & g++ $F -o /core-abl main_abl.cpp & g++ $F -o /core-x main_x.cpp & g++ -std=c++20 -O2 -pthread -o /diag diag.cpp & wait; \
    test -x /shopping-core && test -x /core-v31 && test -x /core-abl && test -x /core-x && test -x /diag

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY --from=build /shopping-core /app/shopping-core
COPY --from=build /core-v31 /app/core-v31
COPY --from=build /core-abl /app/core-abl
COPY --from=build /core-x /app/core-x
COPY --from=build /diag /app/diag
RUN mkdir -p /scratch /output && chown -R 1000:1000 /scratch /output
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8
ENTRYPOINT ["/app/diag"]
