package main

import (
	"copy-topic-go/config"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

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

	consumer, err := kafka.NewConsumer(config.GetCfgFromEnv(*env).ConsumerConfig())
	if err != nil {
		log.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	md, err := consumer.GetMetadata(source_topic, false, 5000)
	if err != nil {
		log.Fatalf("get metadata: %v", err)
	}

	var topicPartitions []kafka.TopicPartition
	for _, p := range md.Topics[*source_topic].Partitions {
		topicPartitions = append(topicPartitions, kafka.TopicPartition{
			Topic:     source_topic,
			Partition: p.ID,
			Offset:    kafka.OffsetBeginning,
		})
	}
	if err := consumer.Assign(topicPartitions); err != nil {
		log.Fatalf("assign: %v", err)
	}

	producer, err := kafka.NewProducer(config.GetCfgFromEnv(*env).ProducerConfig())
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

	sigc := make(chan os.Signal, 1)
	osSignal := []os.Signal{os.Interrupt}
	signal.Notify(sigc, osSignal...)
	msgCount := 0

	// 파티션별 EOF 상태 추적용 맵
	eofMap := make(map[int32]bool)
	totalPartitionCount := len(md.Topics[*source_topic].Partitions)

	run := true
	for run {
		select {
		case <-sigc:
			run = false

		default:
			ev := consumer.Poll(100)
			switch m := ev.(type) {
			case *kafka.Message:
				tp := kafka.TopicPartition{
					Topic: target_topic,
					// Partitioner decides the partition
					Partition: kafka.PartitionAny,
				}

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
			case kafka.PartitionEOF:
				if !eofMap[m.Partition] {
					log.Printf("%% Reached %v", m)
					eofMap[m.Partition] = true // 해당 파티션이 EOF에 도달했음을 기록
				}
				if len(eofMap) == totalPartitionCount {
					log.Printf("Messages %v are processed\n", msgCount)
					run = false
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
