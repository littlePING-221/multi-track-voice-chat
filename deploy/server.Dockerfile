FROM rust:1.96-alpine AS build
ARG http_proxy
ARG https_proxy
ARG all_proxy
ENV http_proxy=${http_proxy} https_proxy=${https_proxy} all_proxy=${all_proxy}
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY crates ./crates
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo test --release --workspace && \
    cargo build --release -p voice-server && \
    cp /src/target/release/voice-server /src/voice-server
FROM alpine:3.22
COPY --from=build /src/voice-server /usr/local/bin/voice-server
CMD ["voice-server"]
