
# https://dev.to/sergeyzenchenko/actix-web-in-docker-how-to-build-small-and-secure-images-2mjd
# https://shaneutt.com/blog/rust-fast-small-docker-image-builds/
# https://blog.logrocket.com/packaging-a-rust-web-service-using-docker/
# maybe someday? https://benjamincongdon.me/blog/2019/12/04/Fast-Rust-Docker-Builds-with-cargo-vendor/
# https://github.com/actix/actix-web/issues/1103

FROM rust:1.46.0 as build

# ARG TARGET=x86_64-unknown-linux-musl
# ENV PKG_CONFIG_ALLOW_CROSS=1

# RUN apt-get update \
#     && apt-get install -y ca-certificates cmake musl-tools libssl-dev \
#     && rm -rf /var/lib/apt/lists/* \
#     && rustup target add $TARGET

WORKDIR /usr/src/onetime-downloader

COPY Cargo.toml Cargo.lock ./
RUN mkdir src/ \
    && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs \
#    && cargo build --release --target $TARGET \
    && cargo build --release \
    && rm src/*.rs

COPY . .
RUN rm target/release/deps/onetime_downloader* \
#    && cargo build --release --target $TARGET
    && cargo build --release

FROM ubuntu:bionic
COPY --from=build /usr/src/onetime-downloader/target/release/onetime-downloader /usr/local/bin/

# for ssl
RUN apt-get update \
    && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

CMD ["onetime-downloader"]
