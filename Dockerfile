FROM scratch
COPY target/x86_64-unknown-linux-musl/release/minerva-service /usr/bin/
ENTRYPOINT ["/usr/bin/minerva-service"]
