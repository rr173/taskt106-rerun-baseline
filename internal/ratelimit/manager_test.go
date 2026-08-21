package ratelimit

import (
	"os"
	"task106/internal/model"
	"task106/internal/storage"
	"testing"
	"time"
)

func setupTestManager(t *testing.T) (*Manager, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "ratelimit_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	s, err := storage.New(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		t.Fatalf("failed to create storage: %v", err)
	}

	m := NewManager(s)
	if err := m.Start(); err != nil {
		s.Close()
		os.Remove(tmpPath)
		t.Fatalf("failed to start manager: %v", err)
	}

	cleanup := func() {
		m.Stop()
		s.Close()
		os.Remove(tmpPath)
	}

	return m, cleanup
}

func TestCreateReservation(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now.Add(1 * time.Hour)
	endAt := now.Add(2 * time.Hour)

	result, err := m.CreateReservation("test-policy", "caller-1", 30, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got: %s", result.Message)
	}
	if result.Reservation == nil {
		t.Fatal("expected reservation in result")
	}
	if result.Reservation.Tokens != 30 {
		t.Errorf("expected 30 tokens, got %d", result.Reservation.Tokens)
	}
	if result.Reservation.Status != model.ReservationStatusPending {
		t.Errorf("expected pending status, got %s", result.Reservation.Status)
	}
}

func TestCreateReservationConflict(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}
	_, err = m.BindCaller("caller-2", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now.Add(1 * time.Hour)
	endAt := now.Add(2 * time.Hour)

	result1, err := m.CreateReservation("test-policy", "caller-1", 60, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create first reservation: %v", err)
	}
	if !result1.Success {
		t.Fatalf("expected first reservation success, got: %s", result1.Message)
	}

	result2, err := m.CreateReservation("test-policy", "caller-2", 50, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create second reservation: %v", err)
	}
	if result2.Success {
		t.Fatal("expected second reservation to fail due to conflict, but it succeeded")
	}
}

func TestReservationActivation(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now.Add(100 * time.Millisecond)
	endAt := now.Add(200 * time.Millisecond)

	result, err := m.CreateReservation("test-policy", "caller-1", 30, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got: %s", result.Message)
	}

	statusBefore, err := m.GetCallerStatus("caller-1")
	if err != nil {
		t.Fatalf("failed to get caller status: %v", err)
	}
	if statusBefore.ReservedTokens != 0 {
		t.Errorf("expected 0 reserved tokens before activation, got %d", statusBefore.ReservedTokens)
	}

	time.Sleep(150 * time.Millisecond)

	statusDuring, err := m.GetCallerStatus("caller-1")
	if err != nil {
		t.Fatalf("failed to get caller status: %v", err)
	}
	if statusDuring.ReservedTokens != 30 {
		t.Errorf("expected 30 reserved tokens during reservation, got %d", statusDuring.ReservedTokens)
	}

	time.Sleep(100 * time.Millisecond)

	statusAfter, err := m.GetCallerStatus("caller-1")
	if err != nil {
		t.Fatalf("failed to get caller status: %v", err)
	}
	if statusAfter.ReservedTokens != 0 {
		t.Errorf("expected 0 reserved tokens after reservation, got %d", statusAfter.ReservedTokens)
	}
}

func TestCancelReservation(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now.Add(1 * time.Hour)
	endAt := now.Add(2 * time.Hour)

	result, err := m.CreateReservation("test-policy", "caller-1", 30, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got: %s", result.Message)
	}

	resID := result.Reservation.ID

	cancelResult, err := m.CancelReservation(resID)
	if err != nil {
		t.Fatalf("failed to cancel reservation: %v", err)
	}
	if !cancelResult.Success {
		t.Fatalf("expected cancel success, got: %s", cancelResult.Message)
	}
	if cancelResult.Reservation.Status != model.ReservationStatusCancelled {
		t.Errorf("expected cancelled status, got %s", cancelResult.Reservation.Status)
	}

	reservations, err := m.ListReservations("test-policy", "", "cancelled")
	if err != nil {
		t.Fatalf("failed to list reservations: %v", err)
	}
	if len(reservations) != 1 {
		t.Errorf("expected 1 cancelled reservation, got %d", len(reservations))
	}
}

func TestListReservations(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("policy-a", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}
	_, err = m.CreatePolicy("policy-b", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "policy-a", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}
	_, err = m.BindCaller("caller-2", "policy-b", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now.Add(1 * time.Hour)
	endAt := now.Add(2 * time.Hour)

	_, err = m.CreateReservation("policy-a", "caller-1", 30, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}
	_, err = m.CreateReservation("policy-b", "caller-2", 40, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}

	allReservations, err := m.ListReservations("", "", "")
	if err != nil {
		t.Fatalf("failed to list all reservations: %v", err)
	}
	if len(allReservations) != 2 {
		t.Errorf("expected 2 reservations, got %d", len(allReservations))
	}

	policyAReservations, err := m.ListReservations("policy-a", "", "")
	if err != nil {
		t.Fatalf("failed to list policy-a reservations: %v", err)
	}
	if len(policyAReservations) != 1 {
		t.Errorf("expected 1 reservation for policy-a, got %d", len(policyAReservations))
	}
}

func TestReservationEffectiveLimit(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 10.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now
	endAt := now.Add(1 * time.Hour)

	result, err := m.CreateReservation("test-policy", "caller-1", 30, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got: %s", result.Message)
	}

	status, err := m.GetCallerStatus("caller-1")
	if err != nil {
		t.Fatalf("failed to get caller status: %v", err)
	}

	if status.ReservedTokens != 30 {
		t.Errorf("expected 30 reserved tokens, got %d", status.ReservedTokens)
	}

	expectedRemaining := 50 + 30
	if status.Remaining != expectedRemaining {
		t.Errorf("expected %d remaining tokens (50 quota + 30 reserved), got %d", expectedRemaining, status.Remaining)
	}
}

func TestCreateReservationInvalid(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()

	tests := []struct {
		name      string
		policy    string
		caller    string
		tokens    int
		startAt   time.Time
		endAt     time.Time
		expectErr bool
	}{
		{
			name:      "end before start",
			policy:    "test-policy",
			caller:    "caller-1",
			tokens:    10,
			startAt:   now.Add(2 * time.Hour),
			endAt:     now.Add(1 * time.Hour),
			expectErr: true,
		},
		{
			name:      "negative tokens",
			policy:    "test-policy",
			caller:    "caller-1",
			tokens:    -5,
			startAt:   now.Add(1 * time.Hour),
			endAt:     now.Add(2 * time.Hour),
			expectErr: true,
		},
		{
			name:      "tokens exceeds policy max",
			policy:    "test-policy",
			caller:    "caller-1",
			tokens:    200,
			startAt:   now.Add(1 * time.Hour),
			endAt:     now.Add(2 * time.Hour),
			expectErr: true,
		},
		{
			name:      "nonexistent policy",
			policy:    "nonexistent",
			caller:    "caller-1",
			tokens:    10,
			startAt:   now.Add(1 * time.Hour),
			endAt:     now.Add(2 * time.Hour),
			expectErr: true,
		},
		{
			name:      "nonexistent caller",
			policy:    "test-policy",
			caller:    "nonexistent",
			tokens:    10,
			startAt:   now.Add(1 * time.Hour),
			endAt:     now.Add(2 * time.Hour),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := m.CreateReservation(tt.policy, tt.caller, tt.tokens, tt.startAt, tt.endAt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.expectErr && result.Success {
				t.Errorf("expected failure but got success")
			}
			if !tt.expectErr && !result.Success {
				t.Errorf("expected success but got failure: %s", result.Message)
			}
		})
	}
}

func TestCancelActiveReservation(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()
	startAt := now
	endAt := now.Add(1 * time.Hour)

	result, err := m.CreateReservation("test-policy", "caller-1", 30, startAt, endAt)
	if err != nil {
		t.Fatalf("failed to create reservation: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got: %s", result.Message)
	}

	resID := result.Reservation.ID

	statusBefore, err := m.GetCallerStatus("caller-1")
	if err != nil {
		t.Fatalf("failed to get caller status: %v", err)
	}
	if statusBefore.ReservedTokens != 30 {
		t.Errorf("expected 30 reserved tokens before cancel, got %d", statusBefore.ReservedTokens)
	}

	cancelResult, err := m.CancelReservation(resID)
	if err != nil {
		t.Fatalf("failed to cancel reservation: %v", err)
	}
	if !cancelResult.Success {
		t.Fatalf("expected cancel success, got: %s", cancelResult.Message)
	}

	statusAfter, err := m.GetCallerStatus("caller-1")
	if err != nil {
		t.Fatalf("failed to get caller status: %v", err)
	}
	if statusAfter.ReservedTokens != 0 {
		t.Errorf("expected 0 reserved tokens after cancel, got %d", statusAfter.ReservedTokens)
	}
}

func TestReservationPartialOverlap(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}
	_, err = m.BindCaller("caller-2", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()

	start1 := now.Add(1 * time.Hour)
	end1 := now.Add(3 * time.Hour)

	start2 := now.Add(2 * time.Hour)
	end2 := now.Add(4 * time.Hour)

	result1, err := m.CreateReservation("test-policy", "caller-1", 60, start1, end1)
	if err != nil {
		t.Fatalf("failed to create first reservation: %v", err)
	}
	if !result1.Success {
		t.Fatalf("expected first reservation success, got: %s", result1.Message)
	}

	result2, err := m.CreateReservation("test-policy", "caller-2", 50, start2, end2)
	if err != nil {
		t.Fatalf("failed to create second reservation: %v", err)
	}
	if result2.Success {
		t.Fatal("expected second reservation to fail due to partial overlap conflict")
	}
}

func TestReservationNoOverlap(t *testing.T) {
	m, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := m.CreatePolicy("test-policy", model.AlgoTokenBucket, 60, 100, 1.0, "second")
	if err != nil {
		t.Fatalf("failed to create policy: %v", err)
	}

	_, err = m.BindCaller("caller-1", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}
	_, err = m.BindCaller("caller-2", "test-policy", 50)
	if err != nil {
		t.Fatalf("failed to bind caller: %v", err)
	}

	now := time.Now()

	start1 := now.Add(1 * time.Hour)
	end1 := now.Add(2 * time.Hour)

	start2 := now.Add(3 * time.Hour)
	end2 := now.Add(4 * time.Hour)

	result1, err := m.CreateReservation("test-policy", "caller-1", 80, start1, end1)
	if err != nil {
		t.Fatalf("failed to create first reservation: %v", err)
	}
	if !result1.Success {
		t.Fatalf("expected first reservation success, got: %s", result1.Message)
	}

	result2, err := m.CreateReservation("test-policy", "caller-2", 80, start2, end2)
	if err != nil {
		t.Fatalf("failed to create second reservation: %v", err)
	}
	if !result2.Success {
		t.Fatalf("expected second reservation success (no overlap), got: %s", result2.Message)
	}
}
