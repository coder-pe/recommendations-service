package ingest

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"

	kafka "github.com/segmentio/kafka-go"
)

type Consumer struct {
	brokers []string
	groupID string
	topics  []string
	handle  *Handler
}

func NewConsumer(brokers []string, groupID string, topics []string, h *Handler) *Consumer {
	return &Consumer{
		brokers: brokers,
		groupID: groupID,
		topics:  topics,
		handle:  h,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	if len(c.topics) == 0 {
		log.Printf("[kafka] no topics configured")
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(c.topics))

	for _, topic := range c.topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}

		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:  c.brokers,
				GroupID:  c.groupID,
				Topic:    topic,
				MinBytes: 1,
				MaxBytes: 10e6,
			})
			defer r.Close()

			log.Printf("[kafka] consumer started topic=%s group=%s", topic, c.groupID)
			for {
				msg, err := r.FetchMessage(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					errCh <- err
					return
				}

				if err := c.handle.Handle(ctx, topic, msg.Value); err != nil {
					log.Printf("[kafka] handler failed topic=%s offset=%d: %v", topic, msg.Offset, err)
				}
				if err := r.CommitMessages(ctx, msg); err != nil {
					log.Printf("[kafka] commit failed topic=%s offset=%d: %v", topic, msg.Offset, err)
				}
			}
		}(topic)
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}
