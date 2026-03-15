# recommendations-service

Adapter service between Qhato events/APIs and Gorse recommendation engine.

## Endpoints
- `GET /health`
- `GET /recommendations/health`
- `GET /recommendations/api/v1/users/:userId/home?n=20&offset=0&tenantCode=PLATFORM_PE`
- `POST /recommendations/api/v1/feedback`

## Kafka topics consumed
- `qhato.users.user_registered`
- `qhato.catalog.product.created`
- `qhato.catalog.product.updated`
- `qhato.inventory.store_product.upsert`
- `qhato.reservations.status_changed`
- `qhato.recommendations.feedback`

## Run local
```bash
cp .env.example .env
go run ./cmd/main.go
```

## Notes
- Kafka ingest uses `eventId` deduplication in Redis (`SETNX + TTL`) when idempotency is enabled.
- To enforce strict contracts, keep `IDEMPOTENCY_SKIP_IF_NO_EVENT_ID=true` in environments where all producer events include `eventId`.
- Consumer retries transient failures (`KAFKA_CONSUMER_MAX_ATTEMPTS`) and sends failed records to `KAFKA_TOPIC_DLQ`.
- Contract gates supported: `CONTRACT_REQUIRE_EVENT_ID` and `CONTRACT_REQUIRE_EVENT_VERSION`.
