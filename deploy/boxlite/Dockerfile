# Build and export only the exact BoxLite v0.9.0 aarch64 runtime, replacing its
# static-glibc shim with a source-identical shim linked to Bookworm system glibc.
FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS builder
RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261018T000000Z bookworm main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261018T000000Z bookworm-updates main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20261018T000000Z bookworm-security main' \
      > /etc/apt/sources.list \
    && apt-get update && apt-get install -y --no-install-recommends \
      build-essential=12.9 ca-certificates=20250419~deb12u1 \
      clang=1:14.0-55.7~deb12u1 llvm=1:14.0-55.7~deb12u1 libclang-dev=1:14.0-55.7~deb12u1 \
      libssl-dev=3.0.22-1~deb12u1 pkg-config=1.8.1-1 protobuf-compiler=3.21.12-3+deb12u1 \
      patchelf=0.14.3-1+b1 file=1:5.44-3 git=1:2.39.5-0+deb12u3 curl=7.88.1-10+deb12u15 \
      binutils=2.40-2 meson=1.0.1-5 ninja-build=1.11.1-2~deb12u1 libcap-dev=1:2.66-4+deb12u3+b1 \
    && rm -rf /var/lib/apt/lists/*
RUN curl --fail --location --retry 3 https://go.dev/dl/go1.24.6.linux-arm64.tar.gz -o /tmp/go.tgz \
    && echo '124ea6033a8bf98aa9fbab53e58d134905262d45a022af3a90b73320f3c3afd5  /tmp/go.tgz' | sha256sum -c - \
    && tar -C /usr/local -xzf /tmp/go.tgz && rm /tmp/go.tgz
ENV PATH="/usr/local/go/bin:${PATH}" RUSTUP_TOOLCHAIN=1.90.0 CARGO_BUILD_JOBS=3

FROM builder AS shim-build
WORKDIR /build/source
RUN git init . && git remote add origin https://github.com/boxlite-ai/boxlite.git \
    && git fetch --depth 1 origin 690794512fde9491836c1aa3d087d64c80bf4849 \
    && git checkout --detach FETCH_HEAD \
    && git submodule update --init --recursive
COPY build.sh /recipe/build.sh
RUN mkdir /output && bash /recipe/build.sh

FROM builder AS original-runtime
RUN mkdir /runtime \
    && curl --fail --location --retry 3 \
      https://github.com/boxlite-ai/boxlite/releases/download/v0.9.0/boxlite-runtime-v0.9.0-linux-arm64-gnu.tar.gz \
      -o /tmp/runtime.tgz \
    && echo 'cce6512be469c826c0cb62c604af240849980fa130ac8708e8d1e5ca289d0f31  /tmp/runtime.tgz' | sha256sum -c - \
    && tar -xzf /tmp/runtime.tgz -C /runtime --strip-components=1 \
    && rm /tmp/runtime.tgz

FROM scratch AS runtime-export
COPY --from=original-runtime /runtime /runtime
COPY --from=shim-build /output/boxlite-shim /runtime/boxlite-shim
COPY --from=shim-build /output /build-evidence
