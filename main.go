package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	kafka "github.com/bwNetFlow/kafkaconnector"
)

var (
	logFile = flag.String("log", "./processor_splitter.log", "Location of the log file.")

	kafkaBroker         = flag.String("kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	kafkaConsumerGroup  = flag.String("kafka.consumer_group", "splitter_debug", "Kafka Consumer Group")
	kafkaInTopic        = flag.String("kafka.in.topic", "flow-messages-enriched", "Kafka topic to consume from")
	kafkaOutTopicPrefix = flag.String("kafka.out.topicprefix", "flows", "Kafka topic prefix to produce to, will be prepended to '-$cid'")

	kafkaUser        = flag.String("kafka.user", "", "Kafka username to authenticate with")
	kafkaPass        = flag.String("kafka.pass", "", "Kafka password to authenticate with")
	kafkaAuthAnon    = flag.Bool("kafka.auth_anon", false, "Set Kafka Auth Anon")
	kafkaDisableTLS  = flag.Bool("kafka.disable_tls", false, "Whether to use tls or not")
	kafkaDisableAuth = flag.Bool("kafka.disable_auth", false, "Whether to use auth or not")

	// TODO: allow splitting on arbitrary fields
	cids = flag.String("cids", "100,101,102", "Which Cid topics will be created")
)

func main() {
	flag.Parse()
	var err error

	// initialize logger
	logfile, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

	// disable TLS if requested
	if *kafkaDisableTLS {
		log.Println("kafkaDisableTLS ...")
		kafkaConn.DisableTLS()
	}
	if *kafkaDisableAuth {
		log.Println("kafkaDisableAuth ...")
		kafkaConn.DisableAuth()
	} else { // set Kafka auth
		if *kafkaAuthAnon {
			kafkaConn.SetAuthAnon()
		} else if *kafkaUser != "" {
			kafkaConn.SetAuth(*kafkaUser, *kafkaPass)
		} else {
			err = kafkaConn.SetAuthFromEnv()
			if err != nil {
				log.Println("No Credentials available, using 'anon:anon'.")
				kafkaConn.SetAuthAnon()
			}
		}
	}
	err = kafkaConn.StartConsumer(*kafkaBroker, []string{*kafkaInTopic}, *kafkaConsumerGroup, -1) // offset -1 is the most recent flow
	if err != nil {
		log.Println("StartConsumer:", err)
		// sleep to make auto restart not too fast and spamming connection retries
		time.Sleep(5 * time.Second)
		return
	}
	err = kafkaConn.StartProducer(*kafkaBroker)
	if err != nil {
		log.Println("StartProducer:", err)
		// sleep to make auto restart not too fast and spamming connection retries
		time.Sleep(5 * time.Second)
		return
	}
	defer kafkaConn.Close()

	cidSet := make(map[uint32]struct{})
	for _, cid_str := range strings.Split(*cids, ",") {
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
				topic := fmt.Sprintf("%s-%d", *kafkaOutTopicPrefix, flowmsg.Cid)
				kafkaConn.ProducerChannel(topic) <- flowmsg
			}
		case <-signals:
			log.Println("Received Signal to quit.")
			return
		}
	}
}
