package idempotency

import "context"

type Store interface {
	TryMarkProcessed(ctx context.Context, key string) (bool, error)
}
