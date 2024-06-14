ARG UBUNTU_VERSION

FROM ubuntu:${UBUNTU_VERSION} as build
ENV DEBIAN_FRONTEND noninteractive
COPY rust-toolchain /tmp/rust-toolchain
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libssl-dev \
    ca-certificates \
    libudev-dev \
    zlib1g-dev \
    pkg-config \
    llvm \
    clang \
    make \
    protobuf-compiler \
    software-properties-common \
    openssh-client \
    jq \
    git \
    unzip \
    libgoogle-perftools-dev
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $(cat /tmp/rust-toolchain)
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup component add clippy && rustup component add rustfmt
RUN cargo install cargo-rpm --version 0.8.0 &&\
    cargo install cargo-audit
RUN mkdir -p ~/.ssh/ && ssh-keyscan -H github.com >> ~/.ssh/known_hosts

COPY . .

ENV NEON_REVISION=true
RUN cargo build --all-targets --color=always --release
RUN mkdir -p /opt/data/schemas
RUN cp target/release/indexer /usr/sbin/.
RUN cp target/release/proxy /usr/sbin/.
RUN cp target/release/sol2neon /usr/sbin/.


FROM ubuntu:${UBUNTU_VERSION} as dist
RUN \
  apt-get update -y && \
  apt-get install --no-install-recommends -y \
      libssl-dev \
      ca-certificates ; \
  apt-get clean ; \
  rm -rf /var/lib/apt/lists/*


COPY schemas/scheme.sql /opt/data/schemas/scheme.sql
COPY --from=build /usr/sbin/indexer /usr/sbin/indexer
COPY --from=build /usr/sbin/proxy /usr/sbin/proxy
COPY --from=build /usr/sbin/sol2neon /usr/sbin/sol2neon


WORKDIR /usr/sbin
ENTRYPOINT ["./proxy"]