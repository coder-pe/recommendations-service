package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/joho/godotenv"
	"github.com/qhato/recommendations-service/internal/config"
	"github.com/qhato/recommendations-service/internal/gorse"
	"github.com/qhato/recommendations-service/internal/ingest"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg := config.Load()
	gorseClient := gorse.New(cfg.GorseEndpoint, cfg.GorseAPIKey, time.Duration(cfg.GorseTimeoutSeconds)*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cfg.KafkaEnabled && cfg.GorseEnabled {
		h := ingest.NewHandler(gorseClient, cfg.TenantNamespaceEnable)
		consumer := ingest.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.TopicList(), h)
		go func() {
			if err := consumer.Start(ctx); err != nil && err != context.Canceled {
				log.Printf("kafka consumer stopped with error: %v", err)
			}
		}()
	} else {
		log.Printf("kafka ingest disabled: kafka=%v gorse=%v", cfg.KafkaEnabled, cfg.GorseEnabled)
	}

	app := fiber.New(fiber.Config{
		AppName:      "Qhato Recommendations Service v1.0",
		ServerHeader: "Qhato-Recommendations-Service",
	})

	app.Use(recover.New())
	app.Use(logger.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: cfg.CORSOrigins,
		AllowMethods: cfg.CORSMethods,
		AllowHeaders: cfg.CORSHeaders,
	}))

	health := func(c *fiber.Ctx) error {
		resp := fiber.Map{
			"status":  "healthy",
			"service": "recommendations-service",
			"version": "1.0.0",
			"checks": fiber.Map{
				"kafkaEnabled": cfg.KafkaEnabled,
				"gorseEnabled": cfg.GorseEnabled,
			},
		}

		statusCode := fiber.StatusOK
		if cfg.GorseEnabled {
			if err := gorseClient.Healthy(c.UserContext()); err != nil {
				resp["status"] = "degraded"
				resp["checks"].(fiber.Map)["gorse"] = fiber.Map{"status": "down", "error": err.Error()}
				statusCode = fiber.StatusServiceUnavailable
			} else {
				resp["checks"].(fiber.Map)["gorse"] = fiber.Map{"status": "up"}
			}
		} else {
			resp["checks"].(fiber.Map)["gorse"] = fiber.Map{"status": "disabled"}
		}

		return c.Status(statusCode).JSON(resp)
	}

	app.Get("/health", health)
	base := app.Group(cfg.ContextPath)
	base.Get("/health", health)
	api := base.Group(cfg.APIBasePath)

	api.Get("/users/:userId/home", func(c *fiber.Ctx) error {
		userID := strings.TrimSpace(c.Params("userId"))
		if userID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "userId is required"})
		}
		tenantCode := strings.TrimSpace(c.Query("tenantCode"))

		n := cfg.GorseDefaultN
		if q := strings.TrimSpace(c.Query("n")); q != "" {
			if nParsed, err := strconv.Atoi(q); err == nil && nParsed > 0 {
				n = nParsed
			}
		}
		offset := 0
		if q := strings.TrimSpace(c.Query("offset")); q != "" {
			if offParsed, err := strconv.Atoi(q); err == nil && offParsed >= 0 {
				offset = offParsed
			}
		}

		scopedUser := userID
		if cfg.TenantNamespaceEnable && tenantCode != "" {
			scopedUser = tenantCode + "::" + userID
		}

		result, err := gorseClient.RecommendForUser(c.UserContext(), scopedUser, n, offset)
		if err != nil {
			return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"userId":     userID,
			"scopedUser": scopedUser,
			"items":      result.Items,
			"count":      len(result.Items),
		})
	})

	api.Post("/feedback", func(c *fiber.Ctx) error {
		var req struct {
			FeedbackType string `json:"feedbackType"`
			UserID       string `json:"userId"`
			ItemID       string `json:"itemId"`
			TenantCode   string `json:"tenantCode"`
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid payload"})
		}
		if strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.ItemID) == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "userId and itemId are required"})
		}
		feedbackType := strings.TrimSpace(req.FeedbackType)
		if feedbackType == "" {
			feedbackType = "click"
		}

		scopedUser := strings.TrimSpace(req.UserID)
		scopedItem := strings.TrimSpace(req.ItemID)
		if cfg.TenantNamespaceEnable && strings.TrimSpace(req.TenantCode) != "" {
			scopedUser = req.TenantCode + "::" + scopedUser
			scopedItem = req.TenantCode + "::" + scopedItem
		}

		if err := gorseClient.InsertFeedback(c.UserContext(), strings.ToLower(feedbackType), scopedUser, scopedItem, time.Now().UTC()); err != nil {
			return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "accepted"})
	})

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("recommendations-service listening on :%s%s", cfg.Port, cfg.ContextPath)
		if err := app.Listen(":" + cfg.Port); err != nil {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	select {
	case <-quit:
	case err := <-serverErr:
		log.Printf("server error: %v", err)
	}

	cancel()
	_ = app.Shutdown()
}
