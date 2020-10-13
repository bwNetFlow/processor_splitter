FROM golang:1.14 AS builder

# Add your files into the container
ADD . /opt/build
WORKDIR /opt/build

# build the binary
RUN CGO_ENABLED=0 go build -o splitter -v
FROM alpine:3.12
WORKDIR /

# COPY binary from previous stegae to your desired location
COPY --from=builder /opt/build/splitter .
ENTRYPOINT /splitter --kafka.brokers=${KAFKA_BROKERS} --kafka.in.topic=${KAFKA_IN_TOPIC} --kafka.out.topicprefix=${KAFKA_OUT_TOPICPREFIX} --kafka.consumer_group=${CONSUMER_GROUP} --kafka.disable_auth=${DISABLE_AUTH} --kafka.disable_tls=${DISABLE_TLS} --kafka.auth_anon=${AUTH_ANON} --cids=${CIDS}
