package main

import (
	"copy-topic-go/config"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	env := flag.String("env", "dev", "Environment (dev, test, prod)")
	source_topic := flag.String("source_topic", "", "Source topic name")
	target_topic := flag.String("target_topic", "", "Target topic name")
	//preservePartitions := true
	flag.Parse()

	if *source_topic == "" || *target_topic == "" {
		fmt.Println("Usage: go run main.go --source_topic <source_topic_name> --target_topic <target_topic_name>")
		os.Exit(1)
	}

	config.LoadDotEnv(*env)

	consumer, err := kafka.NewConsumer(config.ConnCfg{}.ConsumerConfig())
	if err != nil {
		log.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(config.ConnCfg{}.ProducerConfig())
	if err != nil {
		log.Fatalf("producer init: %v", err)
	}
	defer producer.Close()

	// Delivery reports (optional)
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("[deliver] failed: %v", ev.TopicPartition)
				}
			case kafka.Error:
				log.Printf("[producer] error: %v", ev)
			}
		}
	}()

	// Subscribe started
	if err := consumer.Subscribe(*source_topic, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	sigc := make(chan os.Signal, 1)
	osSignal := []os.Signal{os.Interrupt}
	signal.Notify(sigc, osSignal...)

	msgCount := 0
	lastCommit := time.Now()

	run := true
	for run {
		select {
		case <-sigc:
			run = false

		default:
			ev := consumer.Poll(100)
			switch m := ev.(type) {
			case *kafka.Message:
				tp := kafka.TopicPartition{Topic: target_topic, Partition: kafka.PartitionAny}
				tp.Partition = m.TopicPartition.Partition
				if err := producer.Produce(&kafka.Message{
					TopicPartition: tp,
					Key:            m.Key,
					Value:          m.Value,
					Headers:        m.Headers,
					Timestamp:      m.Timestamp,
				}, nil); err != nil {
					log.Printf("produce error: %v", err)
				}

				msgCount++
				// simple periodic commit
				if msgCount%10_000 == 0 || time.Since(lastCommit) > time.Second {
					if _, err := consumer.Commit(); err != nil {
						log.Printf("commit error: %v", err)
					} else {
						lastCommit = time.Now()
					}
				}

			case kafka.Error:
				log.Printf("[consumer] error: %v", m)
			}
		}
	}

	log.Printf("Flushing producer...")
	producer.Flush(15_000)
	log.Printf("Done.")
}
