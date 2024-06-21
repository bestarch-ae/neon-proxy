ARG UBUNTU_VERSION

FROM ubuntu:${UBUNTU_VERSION} as build
ARG NEON_REVISION

ENV DEBIAN_FRONTEND noninteractive
ENV PATH="/root/.cargo/bin:${PATH}"

COPY . .
RUN apt-get update &&\
    apt-get install -y \
        curl \
        ca-certificates \
        libssl-dev \
        build-essential \
        openssh-client \
        git &&\
    mkdir -p ~/.ssh/ &&\
    ssh-keyscan -H github.com >> ~/.ssh/known_hosts &&\
    curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $(cat rust-toolchain) &&\
    rustup component add clippy &&\
    rustup component add rustfmt &&\
    cargo install cargo-audit &&\
    cargo build --all-targets --color=always --release &&\
    mkdir -p /opt/data/schemas &&\
    cp target/release/indexer /usr/sbin/. &&\
    cp target/release/proxy /usr/sbin/.


FROM ubuntu:${UBUNTU_VERSION} as dist
RUN \
  apt-get update -y && \
  apt-get install --no-install-recommends -y \
      libssl-dev \
      ca-certificates \
      postgresql-client && \
  apt-get autoremove && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

COPY schemas/scheme.sql /opt/data/schemas/scheme.sql
COPY --from=build /usr/sbin/indexer /usr/sbin/indexer
COPY --from=build /usr/sbin/proxy /usr/sbin/proxy

WORKDIR /usr/sbin
ENTRYPOINT ["./proxy"]
