package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port        string
	ContextPath string
	APIBasePath string

	CORSOrigins string
	CORSMethods string
	CORSHeaders string

	KafkaEnabled bool
	KafkaBrokers []string
	KafkaGroupID string

	KafkaTopicUserRegistered     string
	KafkaTopicProductCreated     string
	KafkaTopicProductUpdated     string
	KafkaTopicStoreProductUpsert string
	KafkaTopicReservationStatus  string
	KafkaTopicFeedback           string

	GorseEnabled          bool
	GorseEndpoint         string
	GorseAPIKey           string
	GorseDefaultN         int
	GorseTimeoutSeconds   int
	TenantNamespaceEnable bool
}

func Load() *Config {
	return &Config{
		Port:        getenv("PORT", "8088"),
		ContextPath: normalizePath(getenv("CONTEXT_PATH", "/recommendations")),
		APIBasePath: normalizePath(getenv("API_BASE_PATH", "/api/v1")),

		CORSOrigins: getenv("CORS_ALLOWED_ORIGINS", "*"),
		CORSMethods: getenv("CORS_ALLOWED_METHODS", "GET,POST,OPTIONS"),
		CORSHeaders: getenv("CORS_ALLOWED_HEADERS", "Origin,Content-Type,Accept,Authorization"),

		KafkaEnabled: getenvBool("KAFKA_ENABLED", true),
		KafkaBrokers: splitCSV(getenv("KAFKA_BROKERS", "127.0.0.1:9092")),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "recommendations-service"),

		KafkaTopicUserRegistered:     getenv("KAFKA_TOPIC_USER_REGISTERED", "qhato.users.user_registered"),
		KafkaTopicProductCreated:     getenv("KAFKA_TOPIC_PRODUCT_CREATED", "qhato.catalog.product.created"),
		KafkaTopicProductUpdated:     getenv("KAFKA_TOPIC_PRODUCT_UPDATED", "qhato.catalog.product.updated"),
		KafkaTopicStoreProductUpsert: getenv("KAFKA_TOPIC_STORE_PRODUCT_UPSERT", "qhato.inventory.store_product.upsert"),
		KafkaTopicReservationStatus:  getenv("KAFKA_TOPIC_RESERVATION_STATUS_CHANGED", "qhato.reservations.status_changed"),
		KafkaTopicFeedback:           getenv("KAFKA_TOPIC_FEEDBACK", "qhato.recommendations.feedback"),

		GorseEnabled:          getenvBool("GORSE_ENABLED", true),
		GorseEndpoint:         strings.TrimRight(getenv("GORSE_ENDPOINT", "http://127.0.0.1:8090"), "/"),
		GorseAPIKey:           getenv("GORSE_API_KEY", ""),
		GorseDefaultN:         getenvInt("GORSE_RECOMMEND_DEFAULT_N", 20),
		GorseTimeoutSeconds:   getenvInt("GORSE_TIMEOUT_SECONDS", 5),
		TenantNamespaceEnable: getenvBool("TENANT_NAMESPACE_ENABLED", true),
	}
}

func (c *Config) TopicList() []string {
	topics := []string{
		c.KafkaTopicUserRegistered,
		c.KafkaTopicProductCreated,
		c.KafkaTopicProductUpdated,
		c.KafkaTopicStoreProductUpsert,
		c.KafkaTopicReservationStatus,
		c.KafkaTopicFeedback,
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(topics))
	for _, t := range topics {
		t = strings.TrimSpace(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getenvBool(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"127.0.0.1:9092"}
	}
	return out
}

func normalizePath(path string) string {
	p := strings.TrimSpace(path)
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimRight(p, "/")
}
