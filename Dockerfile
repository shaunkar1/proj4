# DIAGNOSTIC BRANCH - not a scored entry. Runs the pipeline several ways, prints machine facts and
# stage timings to stderr, then adds extra campaign_daily rows that carry the measurements, so the dev check rejects it and shows them.
FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp main_m.cpp main_b.cpp main_c.cpp diag.cpp vert_bench_1file.cpp ./
RUN mkdir -p /vb && (g++ -std=c++20 -O3 -march=x86-64 -pthread -static -o /vb/vert_bench vert_bench_1file.cpp || echo "vert_bench did not build")
RUN g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /shopping-core main.cpp && g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /core-m main_m.cpp && g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /core-b main_b.cpp && g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /core-c main_c.cpp && g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /core-c && g++ -std=c++20 -O2 -pthread -o /diag diag.cpp

FROM --platform=linux/amd64 debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
COPY --from=build /shopping-core /app/shopping-core
COPY --from=build /diag /app/diag
COPY --from=build /core-m /app/core-m
COPY --from=build /core-b /app/core-b
COPY --from=build /core-c /app/core-c
COPY --from=build /vb /app/vb
RUN mkdir -p /scratch /output && chown -R 1000:1000 /scratch /output
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8
ENTRYPOINT ["/app/diag"]
