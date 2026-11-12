FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake pkg-config \
      protobuf-compiler libprotobuf-dev \
      libzmq3-dev cppzmq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY proto ./proto
COPY cpp ./cpp

RUN cmake -S cpp -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)" \
    && mkdir -p /app /data \
    && cp build/broker build/proxy build/server build/client /app/

WORKDIR /app
# O comando (broker, server ou client) é definido no docker-compose.yaml
CMD ["/app/server"]