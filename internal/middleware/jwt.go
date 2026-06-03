package middleware

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

// Claims representa los claims del JWT emitido por users-auth-service.
type Claims struct {
	UserID     string   `json:"userId"`
	Email      string   `json:"email"`
	TenantID   string   `json:"tenantId,omitempty"`
	TenantCode string   `json:"tenantCode,omitempty"`
	Roles      []string `json:"roles"`
	jwt.RegisteredClaims
}

const claimsContextKey = "user"

// adminRoles son los roles que pueden consultar/operar sobre cualquier usuario.
var adminRoles = map[string]struct{}{
	"ADMIN":                {},
	"PLATFORM_ADMIN":       {},
	"PLATFORM_SUPER_ADMIN": {},
	"TENANT_ADMIN":         {},
	"TENANT_OWNER":         {},
}

// NewJWTMiddleware valida el bearer token y deja los claims en el contexto.
func NewJWTMiddleware(secret string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Missing authorization header"})
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid authorization header format. Expected: Bearer <token>"})
		}

		token, err := jwt.ParseWithClaims(parts[1], &Claims{}, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(secret), nil
		})
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid or expired token"})
		}

		claims, ok := token.Claims.(*Claims)
		if !ok || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid token claims"})
		}

		c.Locals(claimsContextKey, claims)
		return c.Next()
	}
}

// GetClaims extrae los claims del contexto Fiber.
func GetClaims(c *fiber.Ctx) (*Claims, bool) {
	claims, ok := c.Locals(claimsContextKey).(*Claims)
	return claims, ok
}

// IsAdmin indica si los claims incluyen un rol administrativo.
func (cl *Claims) IsAdmin() bool {
	for _, r := range cl.Roles {
		if _, ok := adminRoles[strings.ToUpper(strings.TrimSpace(r))]; ok {
			return true
		}
	}
	return false
}

// CanActOnUser indica si el usuario autenticado puede operar sobre targetUserID.
// Un usuario solo puede operar sobre sí mismo, salvo que tenga rol administrativo.
func (cl *Claims) CanActOnUser(targetUserID string) bool {
	if cl.IsAdmin() {
		return true
	}
	return strings.TrimSpace(cl.UserID) != "" && strings.EqualFold(strings.TrimSpace(cl.UserID), strings.TrimSpace(targetUserID))
}
