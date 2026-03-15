package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/qhato/recommendations-service/internal/gorse"
	"github.com/qhato/recommendations-service/internal/idempotency"
)

type Handler struct {
	gorse                 *gorse.Client
	tenantNamespaceEnable bool
	idempotency           idempotency.Store
	skipIfNoEventID       bool
	requireEventID        bool
	requireEventVersion   bool
}

func NewHandler(
	gorseClient *gorse.Client,
	tenantNamespaceEnable bool,
	idempotencyStore idempotency.Store,
	skipIfNoEventID bool,
	requireEventID bool,
	requireEventVersion bool,
) *Handler {
	return &Handler{
		gorse:                 gorseClient,
		tenantNamespaceEnable: tenantNamespaceEnable,
		idempotency:           idempotencyStore,
		skipIfNoEventID:       skipIfNoEventID,
		requireEventID:        requireEventID,
		requireEventVersion:   requireEventVersion,
	}
}

func (h *Handler) Handle(ctx context.Context, topic string, payload []byte) error {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return newPermanentError(err)
	}

	eventID := strings.TrimSpace(asString(event["eventId"]))
	if h.requireEventID && eventID == "" {
		return newPermanentError(fmt.Errorf("missing eventId"))
	}
	eventVersion := strings.TrimSpace(asString(event["eventVersion"]))
	if h.requireEventVersion && eventVersion == "" {
		return newPermanentError(fmt.Errorf("missing eventVersion"))
	}
	if eventVersion != "" && !strings.HasPrefix(eventVersion, "1.") {
		return newPermanentError(fmt.Errorf("unsupported eventVersion: %s", eventVersion))
	}

	if h.idempotency != nil {
		if eventID == "" && h.skipIfNoEventID {
			return nil
		}
		if eventID != "" {
			first, err := h.idempotency.TryMarkProcessed(ctx, topic+"::"+eventID)
			if err != nil {
				return err
			}
			if !first {
				return nil
			}
		}
	}

	body := extractMap(event, "payload")
	if body == nil {
		body = event
	}
	if err := h.validateTopicPayload(topic, body); err != nil {
		return newPermanentError(err)
	}

	switch topic {
	case "qhato.users.user_registered":
		return h.handleUserRegistered(ctx, body)
	case "qhato.catalog.product.created", "qhato.catalog.product.updated":
		return h.handleProductUpsert(ctx, body)
	case "qhato.inventory.store_product.upsert":
		return h.handleStoreProductUpsert(ctx, body)
	case "qhato.reservations.status_changed":
		return h.handleReservationStatus(ctx, body)
	case "qhato.recommendations.feedback":
		return h.handleFeedback(ctx, body)
	default:
		log.Printf("[ingest] unsupported topic=%s", topic)
		return nil
	}
}

func (h *Handler) validateTopicPayload(topic string, body map[string]any) error {
	switch topic {
	case "qhato.users.user_registered":
		if firstNonEmpty(asString(body["userId"]), asString(body["id"])) == "" {
			return fmt.Errorf("invalid payload: missing userId")
		}
	case "qhato.catalog.product.created", "qhato.catalog.product.updated":
		if firstNonEmpty(asString(body["productId"]), asString(body["id"])) == "" {
			return fmt.Errorf("invalid payload: missing productId")
		}
	case "qhato.inventory.store_product.upsert":
		if asString(body["storeProductId"]) == "" {
			return fmt.Errorf("invalid payload: missing storeProductId")
		}
		if asString(body["productId"]) == "" {
			return fmt.Errorf("invalid payload: missing productId")
		}
		if asString(body["storeId"]) == "" {
			return fmt.Errorf("invalid payload: missing storeId")
		}
	case "qhato.reservations.status_changed":
		if asString(body["userId"]) == "" {
			return fmt.Errorf("invalid payload: missing userId")
		}
		if asString(body["newStatus"]) == "" {
			return fmt.Errorf("invalid payload: missing newStatus")
		}
	case "qhato.recommendations.feedback":
		if asString(body["userId"]) == "" || asString(body["itemId"]) == "" {
			return fmt.Errorf("invalid payload: missing userId/itemId")
		}
	}
	return nil
}

func (h *Handler) handleUserRegistered(ctx context.Context, body map[string]any) error {
	userID := firstNonEmpty(
		asString(body["userId"]),
		asString(body["id"]),
	)
	if userID == "" {
		return nil
	}

	tenant := firstNonEmpty(asString(body["tenantCode"]), asString(body["tenantId"]))
	labels := compact([]string{
		"tenant:" + tenant,
		"status:" + firstNonEmpty(asString(body["status"]), "ACTIVE"),
	})

	return h.gorse.UpsertUser(ctx, h.scopeID(tenant, userID), labels)
}

func (h *Handler) handleProductUpsert(ctx context.Context, body map[string]any) error {
	productID := firstNonEmpty(asString(body["productId"]), asString(body["id"]))
	if productID == "" {
		return nil
	}
	tenant := firstNonEmpty(asString(body["tenantCode"]), asString(body["tenantId"]))
	merchantID := asString(body["merchantId"])
	categoryID := firstNonEmpty(asString(body["categoryId"]), asString(body["primaryCategoryId"]))
	name := firstNonEmpty(asString(body["name"]), asString(body["title"]))

	categories := compact([]string{categoryID})
	labels := compact([]string{
		"tenant:" + tenant,
		"merchant:" + merchantID,
		"type:product",
	})

	return h.gorse.UpsertItem(ctx, h.scopeID(tenant, productID), categories, labels, name)
}

func (h *Handler) handleStoreProductUpsert(ctx context.Context, body map[string]any) error {
	storeProductID := asString(body["storeProductId"])
	if storeProductID == "" {
		return nil
	}
	tenant := firstNonEmpty(asString(body["tenantCode"]), asString(body["tenantId"]))
	productID := asString(body["productId"])
	storeID := asString(body["storeId"])

	labels := compact([]string{
		"tenant:" + tenant,
		"store:" + storeID,
		"product:" + productID,
		"type:store_product",
	})

	comment := firstNonEmpty(asString(body["localSku"]), asString(body["name"]), "store-product")
	return h.gorse.UpsertItem(ctx, h.scopeID(tenant, storeProductID), nil, labels, comment)
}

func (h *Handler) handleReservationStatus(ctx context.Context, body map[string]any) error {
	status := strings.ToUpper(asString(body["newStatus"]))
	if status == "" {
		return nil
	}
	if status != "CONFIRMED" && status != "COMPLETED" && status != "PICKED_UP" {
		return nil
	}

	userID := asString(body["userId"])
	if userID == "" {
		return nil
	}
	tenant := firstNonEmpty(asString(body["tenantCode"]), asString(body["tenantId"]))
	scopedUser := h.scopeID(tenant, userID)

	items := extractList(body, "items")
	for _, item := range items {
		storeProductID := firstNonEmpty(asString(item["storeProductId"]), asString(item["itemId"]))
		if storeProductID == "" {
			continue
		}
		if err := h.gorse.InsertFeedback(ctx, "purchase", scopedUser, h.scopeID(tenant, storeProductID), time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) handleFeedback(ctx context.Context, body map[string]any) error {
	feedbackType := strings.ToLower(firstNonEmpty(asString(body["feedbackType"]), "click"))
	userID := asString(body["userId"])
	itemID := asString(body["itemId"])
	tenant := firstNonEmpty(asString(body["tenantCode"]), asString(body["tenantId"]))
	if userID == "" || itemID == "" {
		return fmt.Errorf("invalid feedback payload: missing userId/itemId")
	}

	return h.gorse.InsertFeedback(ctx, feedbackType, h.scopeID(tenant, userID), h.scopeID(tenant, itemID), time.Now().UTC())
}

func (h *Handler) scopeID(tenant, id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if !h.tenantNamespaceEnable {
		return id
	}
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return id
	}
	return tenant + "::" + id
}

func extractMap(event map[string]any, key string) map[string]any {
	v, ok := event[key]
	if !ok {
		return nil
	}
	m, _ := v.(map[string]any)
	return m
}

func extractList(body map[string]any, key string) []map[string]any {
	v, ok := body[key]
	if !ok {
		return nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if ok {
			out = append(out, m)
		}
	}
	return out
}

func asString(v any) string {
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func compact(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || strings.HasSuffix(v, ":") {
			continue
		}
		out = append(out, v)
	}
	return out
}
