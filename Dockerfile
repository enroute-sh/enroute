# syntax=docker/dockerfile:1
#
# The deploy image. `rust-toolchain.toml` pins the compiler and rustup installs
# that pin here, so the version has one home. A `rust` base image would be a
# second: the registry publishes a point release days after rustup does, and a
# tag naming the pin fails the build until it does.
#
# Nothing names an architecture: the binary is for the machine that built it.

ARG ENROUTE_COMMIT=unknown
ARG ENROUTE_VERSION=unknown
# `release` for what ships; a fast-loop caller passes `lab` to skip the fat-LTO
# relink and keep incremental state in the target cache.
ARG ENROUTE_PROFILE=release

FROM debian:bookworm-slim AS build
ARG ENROUTE_PROFILE
ARG TARGETARCH
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
# What the compiler and the crates that build C need. No cmake and no protoc:
# `aws-lc-sys` ships the bindings that would have needed cmake, and the build
# script reads the contract with protox.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl gcc libc6-dev make pkg-config \
    && rm -rf /var/lib/apt/lists/*
# `--default-toolchain none`, because the pin names it and is not copied yet.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal --default-toolchain none
WORKDIR /src
# The pin on its own, ahead of the source, so editing a crate does not reinstall
# the compiler. rustup reads it for the version and both components.
COPY rust-toolchain.toml .
RUN rustup toolchain install --no-self-update
COPY . .
# The registry and the build both cache. Neither is in a layer afterwards, so
# the image carries the binary and nothing that produced it.
#
# Each binary is asked for with the package that holds it. The workspace has
# `default-members`, so a bare `--bin` looks in the server crate alone.
RUN --mount=type=cache,id=enroute-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=enroute-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=enroute-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo build --profile "${ENROUTE_PROFILE}" --locked \
        -p enroute --bin enroute \
        -p enroute-postgres --bin enroute-schema \
    && install -m 755 "target/${ENROUTE_PROFILE}/enroute" /enroute \
    && install -m 755 "target/${ENROUTE_PROFILE}/enroute-schema" /enroute-schema

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ARG ENROUTE_COMMIT
ARG ENROUTE_VERSION
LABEL org.opencontainers.image.title="enroute" \
      org.opencontainers.image.description="The headless git platform." \
      org.opencontainers.image.source="https://github.com/enroute-sh/enroute" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${ENROUTE_COMMIT}" \
      org.opencontainers.image.version="${ENROUTE_VERSION}"
COPY --from=build /enroute /usr/local/bin/enroute
# Every table, applied by the code that reads it. `compose.yaml` gates Enroute
# on it, so leaving it out of the image stops the stack rather than a query.
COPY --from=build /enroute-schema /usr/local/bin/enroute-schema
# The contract's own definition, so an integrator generates a client from the
# image that serves it rather than from a checkout they have to keep in step
# with it. `docker cp` is the whole of what reads this; the binary never does.
COPY proto /usr/share/enroute/proto
# The contract and the git front door, which are two callers and two listeners.
EXPOSE 8080 50051
ENTRYPOINT ["/usr/local/bin/enroute"]
