package api

import (
	"testing"

	"github.com/gin-gonic/gin"
	"task106/internal/lockbudget"
)

func TestRegisterRoutesExposesOperationalAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	// Route registration only needs the handler dependencies to be non-panicking;
	// the endpoint behavior is covered by the manager tests.
	budgetManager := lockbudget.NewManager(nil)
	NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, budgetManager, nil, nil).RegisterRoutes(router)

	routes := router.Routes()
	if len(routes) < 20 {
		t.Fatalf("expected at least 20 operational routes, got %d", len(routes))
	}
	seen := make(map[string]bool, len(routes))
	for _, route := range routes {
		seen[route.Method+" "+route.Path] = true
	}
	for _, expected := range []string{
		"GET /health",
		"POST /api/v1/locks/:name/acquire",
		"GET /api/v1/ratelimit/stats",
		"GET /api/v1/topology/graph",
		"GET /api/v1/handovers",
	} {
		if !seen[expected] {
			t.Errorf("missing required route %q", expected)
		}
	}
}
