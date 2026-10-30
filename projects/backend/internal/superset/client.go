package superset

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client calls Superset REST API for guest tokens / Client gọi Superset REST để lấy guest token
type Client struct {
	host     string
	username string
	password string
	http     *http.Client

	mu         sync.Mutex
	accessToken string
	csrfToken   string
}

func NewClient(host, username, password string) *Client {
	// Cookie jar keeps Superset session for CSRF validation / Giữ session Superset để validate CSRF
	jar, _ := cookiejar.New(nil)
	return &Client{
		host:     host,
		username: username,
		password: password,
		http: &http.Client{
			Timeout: 30 * time.Second,
			Jar:     jar,
		},
	}
}

func (c *Client) Enabled() bool {
	return c.host != "" && c.username != ""
}

func (c *Client) Host() string {
	return c.host
}

type loginResponse struct {
	AccessToken string `json:"access_token"`
}

type csrfResponse struct {
	Result string `json:"result"`
}

type guestTokenResponse struct {
	Token string `json:"token"`
}

// clearSessionLocked drops cached JWT/CSRF (caller must hold mu).
// Xóa JWT/CSRF cache (caller phải đang giữ mu).
func (c *Client) clearSessionLocked() {
	c.accessToken = ""
	c.csrfToken = ""
}

func (c *Client) ensureSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loginLocked(ctx)
}

// loginLocked refreshes access + CSRF tokens. Caller must hold mu.
// Làm mới access + CSRF. Caller phải đang giữ mu.
func (c *Client) loginLocked(ctx context.Context) error {
	if c.accessToken != "" && c.csrfToken != "" {
		return nil
	}

	loginBody, _ := json.Marshal(map[string]any{
		"username": c.username,
		"password": c.password,
		"provider": "db",
		"refresh":  true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/api/v1/security/login", bytes.NewReader(loginBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("superset login: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("superset login %d: %s", resp.StatusCode, string(body))
	}

	var login loginResponse
	if err := json.NewDecoder(resp.Body).Decode(&login); err != nil {
		return err
	}
	c.accessToken = login.AccessToken

	csrfReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.host+"/api/v1/security/csrf_token/", nil)
	if err != nil {
		return err
	}
	csrfReq.Header.Set("Authorization", "Bearer "+c.accessToken)
	csrfResp, err := c.http.Do(csrfReq)
	if err != nil {
		return fmt.Errorf("superset csrf: %w", err)
	}
	defer csrfResp.Body.Close()
	if csrfResp.StatusCode >= 400 {
		body, _ := io.ReadAll(csrfResp.Body)
		return fmt.Errorf("superset csrf %d: %s", csrfResp.StatusCode, string(body))
	}
	var csrf csrfResponse
	if err := json.NewDecoder(csrfResp.Body).Decode(&csrf); err != nil {
		return err
	}
	c.csrfToken = csrf.Result
	return nil
}

// GuestToken mints a short-lived embed token / Tạo guest token nhúng dashboard
func (c *Client) GuestToken(ctx context.Context, dashboardID int) (string, error) {
	// One retry after re-login when admin JWT expired (~15m TTL)
	// Thử lại sau re-login khi JWT admin hết hạn (~15 phút)
	token, err := c.guestTokenOnce(ctx, dashboardID)
	if err == nil {
		return token, nil
	}
	if !isUnauthorized(err) {
		return "", err
	}
	c.mu.Lock()
	c.clearSessionLocked()
	c.mu.Unlock()
	return c.guestTokenOnce(ctx, dashboardID)
}

func isUnauthorized(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "401") ||
		strings.Contains(msg, "Token has expired") ||
		strings.Contains(msg, "Signature has expired")
}

func (c *Client) guestTokenOnce(ctx context.Context, dashboardID int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.loginLocked(ctx); err != nil {
		return "", err
	}

	id := strconv.Itoa(dashboardID)
	payload, _ := json.Marshal(map[string]any{
		"user": map[string]string{
			"username":   "kaizen_embed",
			"first_name": "Kaizen",
			"last_name":  "Report",
		},
		"resources": []map[string]string{
			{"type": "dashboard", "id": id},
		},
		"rls": []any{},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/api/v1/security/guest_token/", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	req.Header.Set("X-CSRFToken", c.csrfToken)
	req.Header.Set("Referer", c.host+"/")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("guest token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		// Stale session → clear so caller can re-login / Session cũ → xóa để caller login lại
		if resp.StatusCode == http.StatusUnauthorized {
			c.clearSessionLocked()
		}
		return "", fmt.Errorf("guest token %d: %s", resp.StatusCode, string(body))
	}

	var out guestTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("empty guest token from superset")
	}
	return out.Token, nil
}
