FROM rust:1 AS builder

WORKDIR /builder

COPY . .

RUN cargo build --release

FROM ubuntu:24.04

WORKDIR /app

COPY --from=builder /builder/target/release/tronbin .

EXPOSE 3000
EXPOSE 3001

CMD ["/app/tronbin"]
