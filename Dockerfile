FROM rust:bookworm as builder

WORKDIR /work

COPY ./crates /work/crates
COPY ./benches /work/benches
COPY ./Cargo.toml /work/Cargo.toml
COPY ./Cargo.lock /work/Cargo.lock

RUN apt update && apt install libclang-dev -y

RUN cargo build --release

FROM rust:bookworm as runner

COPY --from=builder /work/target/release/mdbx /bin/mdbx

ENTRYPOINT [ "/bin/mdbx" ]