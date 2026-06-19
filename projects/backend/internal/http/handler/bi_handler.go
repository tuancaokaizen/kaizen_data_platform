package handler

import (
	"encoding/json"
	"net/http"

	"github.com/kaizen/backend-report-api/internal/http/response"
	"github.com/kaizen/backend-report-api/internal/service"
)

type BIHandler struct {
	biService *service.BIService
}

func NewBIHandler(biService *service.BIService) *BIHandler {
	return &BIHandler{biService: biService}
}

func (h *BIHandler) GetEmbedConfig(w http.ResponseWriter, _ *http.Request) {
	response.JSON(w, http.StatusOK, h.biService.GetEmbedConfig())
}

type guestTokenRequest struct {
	Slug string `json:"slug"`
}

func (h *BIHandler) PostGuestToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response.Error(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	slug := r.URL.Query().Get("slug")
	if slug == "" && r.Method == http.MethodPost {
		var body guestTokenRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil && body.Slug != "" {
			slug = body.Slug
		}
	}
	if slug == "" {
		response.Error(w, http.StatusBadRequest, "slug is required")
		return
	}

	token, embeddedUUID, err := h.biService.GuestToken(r.Context(), slug)
	if err != nil {
		response.Error(w, http.StatusBadGateway, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]string{
		"token":        token,
		"embeddedUuid": embeddedUUID,
		"slug":         slug,
	})
}
