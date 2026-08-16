package kafka

import (
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type TopicConfig struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

func EnsureTopicsExist(brokerURL string, topics []TopicConfig) error {
	conn, err := kafka.Dial("tcp", brokerURL)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := make([]kafka.TopicConfig, len(topics))
	for i, topic := range topics {
		topicConfigs[i] = kafka.TopicConfig{
			Topic:             topic.Name,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		}
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf("Note: CreateTopics returned: %v (topics may already exist)", err)
		return nil
	}

	log.Printf("Successfully ensured %d topics exist", len(topics))
	return nil
}
