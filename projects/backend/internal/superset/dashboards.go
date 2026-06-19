package superset

import (
	_ "embed"
	"encoding/json"
	"sort"
)

// EmbeddedDashboard maps Superset slug → embed UUID / Ánh xạ slug dashboard → UUID nhúng
type EmbeddedDashboard struct {
	DashboardID  int
	EmbeddedUUID string
}

//go:embed dashboards.embed.json
var embedDashboardJSON []byte

// defaultDashboardBySlug — fallback when embed JSON missing / Dự phòng khi thiếu file JSON
var defaultDashboardBySlug = map[string]EmbeddedDashboard{
	"quy-chi-tiet-don-hang":         {DashboardID: 1, EmbeddedUUID: "f3cdbac4-b137-4965-93ee-cd81e85f9cf7"},
	"quy-pttt-cua-hang":             {DashboardID: 2, EmbeddedUUID: "c3687265-25c1-4e1e-abba-833c942e9386"},
	"quy-tong-hop-cua-hang":         {DashboardID: 3, EmbeddedUUID: "2eed120e-1fac-49c3-98ac-9a007cd40c5f"},
	"tc-chi-tiet-don-hang":          {DashboardID: 4, EmbeddedUUID: "055c2343-5abb-4920-8918-85d0dd4bf6e7"},
	"tc-chi-tiet-san-pham-cua-hang": {DashboardID: 5, EmbeddedUUID: "c3f916e7-d152-47f2-9b00-0f06dcb67d7e"},
	"tc-tong-hop-cua-hang":          {DashboardID: 6, EmbeddedUUID: "fa70b6fb-4872-4a98-9f60-6dcf892723f7"},
	"tc-tong-hop-san-pham":          {DashboardID: 7, EmbeddedUUID: "087d9ae6-6838-4bdf-a4a8-0b649b0e8553"},
}

// DashboardBySlug is populated from dashboards.embed.json (regen: enable-embed)
// Được nạp từ dashboards.embed.json — chạy lại enable-embed sau deploy
var DashboardBySlug map[string]EmbeddedDashboard

type embedJSONEntry struct {
	DashboardID  int    `json:"dashboardId"`
	EmbeddedUUID string `json:"embeddedUuid"`
}

func init() {
	DashboardBySlug = make(map[string]EmbeddedDashboard, len(defaultDashboardBySlug))
	for slug, d := range defaultDashboardBySlug {
		DashboardBySlug[slug] = d
	}

	var raw map[string]embedJSONEntry
	if err := json.Unmarshal(embedDashboardJSON, &raw); err != nil || len(raw) == 0 {
		return
	}
	for slug, entry := range raw {
		if entry.EmbeddedUUID == "" {
			continue
		}
		DashboardBySlug[slug] = EmbeddedDashboard{
			DashboardID:  entry.DashboardID,
			EmbeddedUUID: entry.EmbeddedUUID,
		}
	}
}

func LookupDashboard(slug string) (EmbeddedDashboard, bool) {
	d, ok := DashboardBySlug[slug]
	return d, ok
}

// ListDashboardSlugs returns slugs in stable order / Trả slug theo thứ tự ổn định
func ListDashboardSlugs() []string {
	slugs := make([]string, 0, len(DashboardBySlug))
	for slug := range DashboardBySlug {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	return slugs
}
