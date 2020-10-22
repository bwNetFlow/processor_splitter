Splitter
=

This is the Splitter component of the [bwNetFlow][bwNetFlow] platform. It
supports taking protobuf-encoded [flow messages][protobuf] from a specified
Kafka topic, splitting it along its customer ID field, and writing the result
back into a number of Kafka topic in accordings with the customer ID.

This processor currently assumes that the only relevant split is along the
numeric customer ID field `Cid`. It is however a conceivable use case to create
topics for different protocol numbers or similar, and would have to be
implemented as a configurable option.

Usage
====

The simplest call could look like this, which would start the reducer process
with TLS encryption and SASL auth enabled and all outputs working.

```
export KAFKA_SASL_USER=prod-splitter
export KAFKA_SASL_PASS=somesecurepass`
./splitter \
        --kafka.brokers=kafka.local:9093 \
        --kafka.in.topic=flows-enriched \
        --kafka.out.topicprefix=flows-customer \
        --kafka.consumer_group=splitter-prod \
        --cids "100,101,102" # will create only flows-customer-100, ... topics`
```

[bwNetFlow]: https://github.com/bwNetFlow/bwNetFlow
[protobuf]: https://github.com/bwNetFlow/protobuf
