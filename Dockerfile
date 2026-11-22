# Shopping Sessions: a single static Rust binary in an empty image.
FROM rust:1.100-alpine AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked --features selfbench

FROM scratch
COPY --from=build /src/target/release/shopping /shopping
COPY --from=build /src/target/release/probe /probe
CMD ["/shopping"]
