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
	sourceTopic := flag.String("source_topic", "", "Source topic name")
	targetTopic := flag.String("target_topic", "", "Target topic name")
	//preservePartitions := true
	flag.Parse()

	if *sourceTopic == "" || *targetTopic == "" {
		fmt.Println("Usage: go run main.go --source_topic <source_topic_name> --target_topic <target_topic_name>")
		os.Exit(1)
	}

	consumer, err := kafka.NewConsumer(config.GetCfgFromEnv(*env).ConsumerConfig())
	if err != nil {
		log.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	/**
	1. Stop the application producing to the source topic.
	2. Get the End offsets for all partitions of the source topic.
	3. Start consuming from the source topic.
	4. For each partition, keep track of the last consumed offset.
	5. When the last consumed offset for a partition reaches the End offset - 1, stop consuming from that partition.
	6. Once all partitions have reached their End offsets, stop the consumer.
	*/

	md, err := consumer.GetMetadata(sourceTopic, false, 5000)
	if err != nil {
		log.Fatalf("get metadata: %v", err)
	}

	var prevAssignedPartitions []kafka.TopicPartition
	var endOffsetMap = make(map[int32]kafka.Offset)
	for _, p := range md.Topics[*sourceTopic].Partitions {
		prevAssignedPartitions = append(prevAssignedPartitions, kafka.TopicPartition{
			Topic:     sourceTopic,
			Partition: p.ID,
			Offset:    kafka.OffsetBeginning,
		})
		_, endOffset, _ := consumer.QueryWatermarkOffsets(*sourceTopic, p.ID, 5000)
		endOffsetMap[p.ID] = kafka.Offset(endOffset - 1) // last offset recorded
	}

	if err := consumer.Assign(prevAssignedPartitions); err != nil {
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
					Topic: targetTopic,
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

				// Reached to End of Offset for this partition
				if m.TopicPartition.Offset >= endOffsetMap[m.TopicPartition.Partition] {
					err := consumer.IncrementalUnassign([]kafka.TopicPartition{{
						Topic:     sourceTopic,
						Partition: m.TopicPartition.Partition,
					}})
					if err != nil {
						log.Fatalf("Failed to incremental unassign: %v", err)
					}

					log.Printf("Reached end offset for partition %v, unassigned it", m.TopicPartition.Partition)
					var assignment, _ = consumer.Assignment()

					// No more assigned partitions, we are done
					if len(assignment) == 0 {
						run = false
					}
				}

				msgCount++
			case kafka.Error:
				log.Printf("[consumer] error: %v", m)
			}
		}
	}

	log.Printf("Flushing producer...")
	producer.Flush(15_000)
	log.Printf("Done.")
}
