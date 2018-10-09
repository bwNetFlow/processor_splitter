splitter
==

This is a small Kafka Processor, i.e. a Consumer and a Producer, which supports
splitting a stream of Protobuf messages in one Topic based on a single Protobuf
field. The intention is to split the main Flow Topic into customer-specific
Topics, each with custom Kafka ACLs to enable representatives
of the customer to access their specific feed.
Right now, the Splitter works only on the `Cid` (Customer ID) field, and limits
its output to customer IDs provided using the CLI flag.

Usage
====
First, have the proper environment variables set for authenticating with your Kafka Cluster:

`KAFKA_SASL_USER=splitter KAFKA_SASL_PASS=somesecurepass`

Note the splitter user must have Produce priviledges on any topic it might produce to.


As standalone command:

`./splitter --kafka.brokers "broker01:9093,broker02:9093" --kafka.in.topic flow-messages-enriched --kafka.out.topicprefix flows --kafka.consumer_group splitter-dev --cids "100,101,102" # will create flows-100, flows-101, ... topics`


Using Systemd:
```
[Unit]
Description=Default kafka-processor Splitter process
After=network.target

[Service]
EnvironmentFile=/opt/kafka-processor-splitter/config/authdata
User=kafka-processor-splitter
WorkingDirectory=/opt/kafka-processor-splitter
ExecStart=/opt/kafka-processor-splitter/splitter --kafka.brokers "broker01:9093,broker02:9093" --kafka.in.topic flow-messages-enriched --kafka.out.topicprefix flows --kafka.consumer_group processor-splitter --cids 100,101,102
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

TODOs
====

 * support different Protobuf fields for splitting on
