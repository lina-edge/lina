package main

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"golang.org/x/crypto/bcrypt"
)

// MQTTAuthServer is a lightweight HTTP server for NanoMQ's HTTP authentication callbacks.
// It is started on a dedicated address before the MQTT client connects, so NanoMQ can
// verify the device service's own credentials during its initial CONNECT.
type MQTTAuthServer struct {
	repo   *DeviceRepository
	cfg    Config
	server *http.Server
}

func NewMQTTAuthServer(repo *DeviceRepository, cfg Config) *MQTTAuthServer {
	return &MQTTAuthServer{repo: repo, cfg: cfg}
}

// Serve handles requests on ln until stopped via Stop. The caller should create ln with
// net.Listen so the socket is bound before the MQTT client connects to the broker.
func (s *MQTTAuthServer) Serve(ln net.Listener) error {
	router := gin.Default()
	router.Use(gin.Recovery())
	router.Use(otelgin.Middleware("device-service-auth"))

	api := router.Group("/api/v1/mqtt")
	api.POST("/auth", s.handleAuth)
	api.POST("/super", s.handleSuper)
	api.POST("/acl", s.handleACL)

	s.server = &http.Server{Handler: router}
	logger.Infof(context.Background(), "MQTT auth server listening on %s", ln.Addr().String())
	return s.server.Serve(ln)
}

func (s *MQTTAuthServer) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// handleAuth is called by NanoMQ for every MQTT CONNECT to verify username/password.
// Returns 200 to allow, 403 to deny.
func (s *MQTTAuthServer) handleAuth(c *gin.Context) {
	username := c.PostForm("username")
	password := c.PostForm("password")
	ctx := c.Request.Context()

	// Device service account: compare directly against the configured password.
	if username == s.cfg.MQTTUsername {
		if password != "" && password == s.cfg.MQTTPassword {
			c.Status(http.StatusOK)
			return
		}
		logger.Warnf(ctx, "MQTT auth failed for service account: %s", username)
		c.Status(http.StatusForbidden)
		return
	}

	// Device account: verify against the stored bcrypt hash.
	hash, err := s.repo.GetDeviceSecretHash(ctx, username)
	if err != nil || hash == "" {
		logger.Warnf(ctx, "MQTT auth: device not found or no secret stored: %s", username)
		c.Status(http.StatusForbidden)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		logger.Warnf(ctx, "MQTT auth: invalid password for device: %s", username)
		c.Status(http.StatusForbidden)
		return
	}

	c.Status(http.StatusOK)
}

// handleSuper is called by NanoMQ to check whether a connected client is a superuser.
// Superusers bypass all topic ACL rules. Only the device service account is a superuser.
// Returns 200 for superuser, 403 otherwise.
func (s *MQTTAuthServer) handleSuper(c *gin.Context) {
	username := c.PostForm("username")
	if username == s.cfg.MQTTUsername {
		c.Status(http.StatusOK)
		return
	}
	c.Status(http.StatusForbidden)
}

// handleACL is called by NanoMQ for HTTP ACL checks (topic + access). Static rules in
// nanomq.conf enforce the same topic layout for publish/subscribe at the broker.
// Returns 200 to allow, 403 to deny.
func (s *MQTTAuthServer) handleACL(c *gin.Context) {
	username := c.PostForm("username")
	topic := c.PostForm("topic")
	access := c.PostForm("access") // "1" = subscribe, "2" = publish

	// Block $SYS topics for all non-superusers.
	if strings.HasPrefix(topic, "$SYS/") {
		c.Status(http.StatusForbidden)
		return
	}

	// Device service account has unrestricted access (also covered by super_req).
	if username == s.cfg.MQTTUsername {
		c.Status(http.StatusOK)
		return
	}

	// Devices may only access their own devices/{username}/... subtree. Clients and NanoMQ's
	// HTTP ACL placeholder %t may use either "devices/..." or "/devices/..."; normalize so both match.
	topicNorm := strings.TrimPrefix(topic, "/")
	ownPrefix := "devices/" + username + "/"
	if strings.HasPrefix(topicNorm, ownPrefix) || topicNorm == "devices/"+username {
		c.Status(http.StatusOK)
		return
	}

	_ = access
	c.Status(http.StatusForbidden)
}
