FROM rust:1.88 as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo install --path .

FROM debian:12-slim
RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/sigen-forward /usr/local/bin/sigen-forward
CMD ["sigen-forward"]
