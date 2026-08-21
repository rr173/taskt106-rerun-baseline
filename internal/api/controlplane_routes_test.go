package api

import (
	"github.com/gin-gonic/gin"
	"task106/internal/lockbudget"
	"testing"
)

func TestRegisterRoutesExposesCoordinationControlPlane(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	budgetManager := lockbudget.NewManager(nil)
	NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, budgetManager, nil, nil).RegisterRoutes(router)
	seen := map[string]bool{}
	for _, route := range router.Routes() {
		seen[route.Method+" "+route.Path] = true
	}
	for _, expected := range []string{"POST /api/v1/coordination/resources", "POST /api/v1/coordination/fencing/validate", "POST /api/v1/coordination/recovery/run"} {
		if !seen[expected] {
			t.Errorf("missing route %q", expected)
		}
	}
}
