package service

import (
	"context"
	"fmt"

	"github.com/kaizen/backend-report-api/internal/superset"
)

// BIService resolves embed config and guest tokens / Service cấu hình nhúng Superset
type BIService struct {
	client      *superset.Client
	embedDomain string
}

func NewBIService(client *superset.Client, embedDomain string) *BIService {
	if embedDomain == "" {
		embedDomain = client.Host()
	}
	return &BIService{client: client, embedDomain: embedDomain}
}

type EmbedDashboardInfo struct {
	Slug         string `json:"slug"`
	EmbeddedUUID string `json:"embeddedUuid"`
}

type EmbedConfig struct {
	SupersetDomain string               `json:"supersetDomain"`
	Enabled        bool                 `json:"enabled"`
	Dashboards     []EmbedDashboardInfo `json:"dashboards"`
}

func (s *BIService) GetEmbedConfig() EmbedConfig {
	slugs := superset.ListDashboardSlugs()
	dashboards := make([]EmbedDashboardInfo, 0, len(slugs))
	for _, slug := range slugs {
		d := superset.DashboardBySlug[slug]
		dashboards = append(dashboards, EmbedDashboardInfo{
			Slug:         slug,
			EmbeddedUUID: d.EmbeddedUUID,
		})
	}
	return EmbedConfig{
		SupersetDomain: s.embedDomain,
		Enabled:        s.client.Enabled(),
		Dashboards:     dashboards,
	}
}

func (s *BIService) GuestToken(ctx context.Context, slug string) (string, string, error) {
	if !s.client.Enabled() {
		return "", "", fmt.Errorf("superset embed is not configured")
	}
	d, ok := superset.LookupDashboard(slug)
	if !ok {
		return "", "", fmt.Errorf("unknown dashboard slug: %s", slug)
	}
	token, err := s.client.GuestToken(ctx, d.DashboardID)
	if err != nil {
		return "", "", err
	}
	return token, d.EmbeddedUUID, nil
}
