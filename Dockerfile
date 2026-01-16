FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    g++ cmake make && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN mkdir -p build && cd build && cmake .. && cmake --build .

EXPOSE 8080

CMD ["./build/tinydb"]
