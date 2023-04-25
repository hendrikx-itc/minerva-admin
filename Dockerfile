FROM scratch
COPY target/release/minerva-service /usr/bin/
ENTRYPOINT ["/usr/bin/minerva-service"]
