package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// newTestRouter monta o roteador sem repositório: os casos testados
// são rejeitados na validação, antes de tocar no banco.
func newTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return NewRouter(NewJobHandler(nil, "gateway-go"))
}

func TestCreateJobRejectsInvalidBody(t *testing.T) {
	cases := map[string]string{
		`{}`:                  "type is required",
		`{"type":""}`:         "type is required",
		`{"payload":{"a":1}}`: "type is required",
		`not json`:            "invalid request body",
	}
	for body, wantDetail := range cases {
		req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		newTestRouter().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("body %q: status = %d, want 422", body, rec.Code)
		}
		var got ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Detail != wantDetail {
			t.Fatalf("body %q: detail = %q (err=%v), want %q", body, got.Detail, err, wantDetail)
		}
	}
}

func TestDocsRootRedirectsToSwaggerUI(t *testing.T) {
	for _, path := range []string{"/docs", "/docs/"} {
		router := newTestRouter()
		RegisterDocs(router)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/docs/index.html" {
			t.Fatalf("%s: status = %d location = %q", path, rec.Code, rec.Header().Get("Location"))
		}
	}
}
