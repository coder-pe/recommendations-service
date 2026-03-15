package idempotency

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client    *redis.Client
	ttl       time.Duration
	keyPrefix string
}

func NewRedisStore(redisURL string, ttl time.Duration, keyPrefix string) (*RedisStore, error) {
	opts, err := redis.ParseURL(strings.TrimSpace(redisURL))
	if err != nil {
		return nil, err
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "qhato:recommendations:ingest"
	}
	return &RedisStore{
		client:    redis.NewClient(opts),
		ttl:       ttl,
		keyPrefix: keyPrefix,
	}, nil
}

func (s *RedisStore) TryMarkProcessed(ctx context.Context, key string) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return true, nil
	}
	namespaced := s.keyPrefix + ":" + key
	return s.client.SetNX(ctx, namespaced, 1, s.ttl).Result()
}
