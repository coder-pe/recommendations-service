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
