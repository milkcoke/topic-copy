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

	md, err := consumer.GetMetadata(sourceTopic, false, 5000)
	if err != nil {
		log.Fatalf("get metadata: %v", err)
	}

	var prevAssignedPartitions []kafka.TopicPartition
	for _, p := range md.Topics[*sourceTopic].Partitions {
		prevAssignedPartitions = append(prevAssignedPartitions, kafka.TopicPartition{
			Topic:     sourceTopic,
			Partition: p.ID,
			Offset:    kafka.OffsetBeginning,
		})
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

	// 파티션별 마지막 오프셋 추적용 맵, 기본 값은 OffsetBeginning
	lastCommitOffsetMap := make(map[int32]kafka.Offset)
	for _, p := range md.Topics[*sourceTopic].Partitions {
		lastCommitOffsetMap[p.ID] = kafka.OffsetBeginning
	}

	// 파티션별 EOF 상태 추적용 맵
	partitionEofMap := make(map[int32]bool)
	totalPartitionCount := len(md.Topics[*sourceTopic].Partitions)

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

				lastCommitOffsetMap[m.TopicPartition.Partition] = m.TopicPartition.Offset + 1
				msgCount++
			case kafka.PartitionEOF:
				if !partitionEofMap[m.Partition] {
					log.Printf("%% Reached %v", m)
					partitionEofMap[m.Partition] = true // 해당 파티션이 EOF에 도달했음을 기록
				}
				if len(partitionEofMap) == totalPartitionCount {
					log.Printf("Messages %v are processed\n", msgCount)
					consumer.Unassign()
					run = false
				}

				var newAssignedPartitions []kafka.TopicPartition
				for _, tp := range prevAssignedPartitions {
					if tp.Partition == m.Partition {
						continue
					}
					newAssignedPartitions = append(newAssignedPartitions, kafka.TopicPartition{
						Topic:     tp.Topic,
						Partition: tp.Partition,
						Offset:    lastCommitOffsetMap[tp.Partition],
					})
				}
				if len(newAssignedPartitions) == 0 {
					continue
				}
				if err := consumer.Assign(newAssignedPartitions); err != nil {
					log.Fatalf("Error occurs re-assign after EOF: %v", err)
				}
				prevAssignedPartitions = newAssignedPartitions

			case kafka.Error:
				log.Printf("[consumer] error: %v", m)
			}
		}
	}

	log.Printf("Flushing producer...")
	producer.Flush(15_000)
	log.Printf("Done.")
}
