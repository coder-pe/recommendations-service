package ingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type DLQPublisher struct {
	writer *kafka.Writer
	topic  string
}

type DLQMessage struct {
	EventID         string `json:"eventId"`
	EventType       string `json:"eventType"`
	EventVersion    string `json:"eventVersion"`
	Timestamp       string `json:"timestamp"`
	SourceService   string `json:"sourceService"`
	FailedAt        string `json:"failedAt"`
	SourceTopic     string `json:"sourceTopic"`
	SourcePartition int    `json:"sourcePartition"`
	SourceOffset    int64  `json:"sourceOffset"`
	SourceKey       string `json:"sourceKey,omitempty"`
	Error           string `json:"error"`
	Attempts        int    `json:"attempts"`
	PayloadBase64   string `json:"payloadBase64"`
}

func NewDLQPublisher(brokers []string, topic string) *DLQPublisher {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return nil
	}
	return &DLQPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
		topic: topic,
	}
}

func (p *DLQPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func (p *DLQPublisher) Publish(ctx context.Context, msg kafka.Message, attempts int, err error) error {
	if p == nil || p.writer == nil {
		return nil
	}
	envelope := DLQMessage{
		EventID:         msg.Topic + ":" + fmt.Sprintf("%d:%d", msg.Partition, msg.Offset),
		EventType:       "RecommendationEventFailed",
		EventVersion:    "1.0",
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
		SourceService:   "recommendations-service",
		FailedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		SourceTopic:     msg.Topic,
		SourcePartition: msg.Partition,
		SourceOffset:    msg.Offset,
		SourceKey:       string(msg.Key),
		Error:           fmt.Sprintf("%v", err),
		Attempts:        attempts,
		PayloadBase64:   base64.StdEncoding.EncodeToString(msg.Value),
	}
	payload, mErr := json.Marshal(envelope)
	if mErr != nil {
		return mErr
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.Topic + ":" + fmt.Sprintf("%d", msg.Offset)),
		Value: payload,
		Time:  time.Now().UTC(),
	})
}
