package main

import (
	"flag"
	"fmt"
	kafka "github.com/bwNetFlow/kafkaconnector"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

var (
	LogFile = flag.String("log", "./processor_splitter.log", "Location of the log file.")

	KafkaConsumerGroup  = flag.String("kafka.consumer_group", "splitter_debug", "Kafka Consumer Group")
	KafkaInTopic        = flag.String("kafka.in.topic", "flow-messages-enriched", "Kafka topic to consume from")
	KafkaOutTopicPrefix = flag.String("kafka.out.topicprefix", "flows", "Kafka topic prefix to produce to, will be prepended to '-$cid'")
	KafkaBroker         = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	// TODO: allow splitting on arbitrary fields
	Cids = flag.String("cids", "100,101,102", "Which Cid topics will be created")
)

func main() {
	flag.Parse()
	var err error

	// initialize logger
	logfile, err := os.OpenFile(*LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		println("Error opening file for logging: %v", err)
		return
	}
	defer logfile.Close()
	mw := io.MultiWriter(os.Stdout, logfile)
	log.SetOutput(mw)
	log.Println("-------------------------- Started.")

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// connect to the BelWue Kafka cluster
	var kafkaConn = kafka.Connector{}
	err = kafkaConn.SetAuthFromEnv()
	if err != nil {
		log.Println(err)
		log.Println("Resuming as user anon.")
		kafkaConn.SetAuthAnon()
	}
	err = kafkaConn.StartConsumer(*KafkaBroker, []string{*KafkaInTopic}, *KafkaConsumerGroup, -1) // offset -1 is the most recent flow
	if err != nil {
		log.Println("StartConsumer:", err)
		return
	}
	err = kafkaConn.StartProducer(*KafkaBroker)
	if err != nil {
		log.Println("StartProducer:", err)
		return
	}
	defer kafkaConn.Close()

	cidSet := make(map[uint32]struct{})
	for _, cid_str := range strings.Split(*Cids, ",") {
		cid, err := strconv.ParseUint(cid_str, 10, 32)
		if err != nil {
			log.Fatalln(err)
		}
		cidSet[uint32(cid)] = struct{}{}
	}
	log.Println("Writing topics for Cids:", cidSet)
	// receive flows in a loop
	for {
		select {
		case flowmsg, ok := <-kafkaConn.ConsumerChannel():
			if !ok {
				log.Println("Splitter ConsumerChannel closed. Investigate this!")
				return
			}
			if _, present := cidSet[flowmsg.Cid]; present {
				topic := fmt.Sprintf("%s-%d", *KafkaOutTopicPrefix, flowmsg.Cid)
				kafkaConn.ProducerChannel(topic) <- flowmsg
			}
		case <-signals:
			log.Println("Received Signal to quit.")
			return
		}
	}
}
