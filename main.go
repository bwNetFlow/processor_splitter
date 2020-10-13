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
	"time"
)

var (
	LogFile = flag.String("log", "./processor_splitter.log", "Location of the log file.")

	KafkaBroker         = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	KafkaConsumerGroup  = flag.String("kafka.consumer_group", "splitter_debug", "Kafka Consumer Group")
	KafkaInTopic        = flag.String("kafka.in.topic", "flow-messages-enriched", "Kafka topic to consume from")
	KafkaOutTopicPrefix = flag.String("kafka.out.topicprefix", "flows", "Kafka topic prefix to produce to, will be prepended to '-$cid'")
	// TODO: allow splitting on arbitrary fields
	Cids = flag.String("cids", "100,101,102", "Which Cid topics will be created")

	kafkaUser = flag.String("kafka.user", "", "Kafka username to authenticate with")
	kafkaPass = flag.String("kafka.pass", "", "Kafka password to authenticate with")
	//disable kafka tls or auth if set to false
	KafkaAuthAnon    = flag.Bool("kafka.auth_anon", true, "Set Kafka Auth Anon")
	KafkaDisableTLS  = flag.Bool("kafka.disable_tls", false, "Whether to use tls or not")
	KafkaDisableAuth = flag.Bool("kafka.disable_auth", false, "Whether to use auth or not")
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
		// Set kafka auth
		if *kafkaUser != "" {
			kafkaConn.SetAuth(*kafkaUser, *kafkaPass)
		} else {
			// TODO: make configurable
			if *KafkaDisableTLS {
				log.Println("KafkaDisableTLS ...")
				kafkaConn.DisableTLS()
			}
			if *KafkaDisableAuth {
				log.Println("KafkaDisableAuth ...")
				kafkaConn.DisableAuth()
			}
			if *KafkaAuthAnon {
				kafkaConn.SetAuthAnon()
			}
		}
	}
	err = kafkaConn.StartConsumer(*KafkaBroker, []string{*KafkaInTopic}, *KafkaConsumerGroup, -1) // offset -1 is the most recent flow
	if err != nil {
		log.Println("StartConsumer:", err)
		// sleep to make auto restart not too fast and spamming connection retries
		time.Sleep(5 * time.Second)
		return
	}
	err = kafkaConn.StartProducer(*KafkaBroker)
	if err != nil {
		log.Println("StartProducer:", err)
		// sleep to make auto restart not too fast and spamming connection retries
		time.Sleep(5 * time.Second)
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
