FROM --platform=linux/amd64 debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY main.cpp .
RUN g++ -std=c++20 -O3 -DNDEBUG -march=x86-64 -flto -fopenmp -static -o /shopping-core main.cpp
RUN mkdir -p /rootfs/app /rootfs/scratch /rootfs/output /rootfs/tmp && cp /shopping-core /rootfs/app/shopping-core && chown -R 1000:1000 /rootfs/scratch /rootfs/output && chmod 1777 /rootfs/tmp

# Probe: the same padded one-second run, on an empty base image (one layer) instead of debian-slim.
FROM scratch
COPY --from=build /rootfs/ /
USER 1000:1000
ENV HOME=/scratch DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch NCPU=8
ENTRYPOINT ["/app/shopping-core"]
