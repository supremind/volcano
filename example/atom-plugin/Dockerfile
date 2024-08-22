FROM golang:1.23 AS builder1
WORKDIR /volcano
COPY . /volcano

RUN wget http://musl.libc.org/releases/musl-1.2.1.tar.gz &&\
    tar -xf musl-1.2.1.tar.gz && cd musl-1.2.1 &&\
    ./configure &&\
    make && make install

WORKDIR /volcano
RUN CC=/usr/local/musl/bin/musl-gcc CGO_ENABLED=1 go build -buildmode=plugin -o example/atom-plugin/atom.so example/atom-plugin/atom.go
RUN CC=/usr/local/musl/bin/musl-gcc SUPPORT_PLUGINS="yes" make vc-scheduler



FROM alpine:latest
COPY --from=builder1 /volcano/_output/bin/vc-scheduler /vc-scheduler


# Create the plugins directory
RUN mkdir -p /plugins

# Copy the built plugin from the builder stage
COPY --from=builder1 /volcano/example/atom-plugin/atom.so /plugins/atom.so
WORKDIR /volcano
ENTRYPOINT ["/vc-scheduler"]
# Set the entrypoint or command if needed
# CMD ["your", "command", "here"]