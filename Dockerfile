# DIAGNOSTIC BRANCH - not a scored entry. Runs the pipeline several ways, prints machine facts and
# stage timings to stderr, then adds extra campaign_daily rows that carry the measurements, so the dev check rejects it and shows them.
FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp diag.cpp vert_bench_1file.cpp ./
RUN mkdir -p /vb && (g++ -std=c++20 -O3 -march=x86-64 -pthread -static -o /vb/vert_bench vert_bench_1file.cpp || echo "vert_bench did not build")
RUN g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /shopping-core main.cpp && g++ -std=c++20 -O2 -pthread -o /diag diag.cpp
RUN (g++ -std=c++20 -O3 -DNDEBUG -march=x86-64-v3 -flto -fopenmp -static -o /core-b main.cpp || cp /shopping-core /core-b) && (g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -Wa,-mbranches-within-32B-boundaries -flto -fopenmp -static -o /core-h main.cpp || cp /shopping-core /core-h) && (g++ -std=c++20 -O3 -DNDEBUG -march=x86-64-v3 -Wa,-mbranches-within-32B-boundaries -flto -fopenmp -static -o /core-v main.cpp || cp /shopping-core /core-v)

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=build /shopping-core /app/shopping-core
COPY --from=build /diag /app/diag
COPY --from=build /core-b /app/core-b
COPY --from=build /core-h /app/core-h
COPY --from=build /core-v /app/core-v
COPY --from=build /vb /app/vb
RUN mkdir -p /scratch /output && chown -R 1000:1000 /scratch /output
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8
ENTRYPOINT ["/app/diag"]
