package ingest

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Consumer struct {
	brokers         []string
	groupID         string
	topics          []string
	handle          *Handler
	dlq             *DLQPublisher
	maxAttempts     int
	retryBase       time.Duration
	dlqForTransient bool
}

func NewConsumer(
	brokers []string,
	groupID string,
	topics []string,
	h *Handler,
	dlq *DLQPublisher,
	maxAttempts int,
	retryBase time.Duration,
) *Consumer {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if retryBase <= 0 {
		retryBase = 500 * time.Millisecond
	}
	return &Consumer{
		brokers:     brokers,
		groupID:     groupID,
		topics:      topics,
		handle:      h,
		dlq:         dlq,
		maxAttempts: maxAttempts,
		retryBase:   retryBase,
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

				if err := c.processMessage(ctx, topic, msg); err != nil {
					log.Printf("[kafka] process failed topic=%s offset=%d: %v", topic, msg.Offset, err)
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

func (c *Consumer) processMessage(ctx context.Context, topic string, msg kafka.Message) error {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		err := c.handle.Handle(ctx, topic, msg.Value)
		if err == nil {
			return nil
		}
		lastErr = err

		if IsPermanentError(err) {
			if c.dlq != nil {
				if dlqErr := c.dlq.Publish(ctx, msg, attempt, err); dlqErr != nil {
					log.Printf("[kafka] dlq publish failed topic=%s offset=%d: %v", topic, msg.Offset, dlqErr)
				} else {
					log.Printf("[kafka] moved to dlq topic=%s offset=%d reason=permanent_error", topic, msg.Offset)
				}
			}
			return err
		}

		if attempt < c.maxAttempts {
			backoff := time.Duration(attempt) * c.retryBase
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if c.dlq != nil && lastErr != nil {
		if dlqErr := c.dlq.Publish(ctx, msg, c.maxAttempts, lastErr); dlqErr != nil {
			log.Printf("[kafka] dlq publish failed topic=%s offset=%d: %v", topic, msg.Offset, dlqErr)
		} else {
			log.Printf("[kafka] moved to dlq topic=%s offset=%d reason=max_attempts", topic, msg.Offset)
		}
	}
	return lastErr
}
