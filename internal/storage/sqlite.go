package storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"task106/internal/model"
	"time"

	_ "modernc.org/sqlite"
)

type Storage struct {
	db *sql.DB
}

func New(path string) (*Storage, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)

	s := &Storage{db: db}
	if err := s.initSchema(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) DB() *sql.DB {
	return s.db
}

func (s *Storage) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS locks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		status TEXT NOT NULL DEFAULT 'free',
		holder TEXT DEFAULT '',
		reentrant INTEGER NOT NULL DEFAULT 0,
		count INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS leases (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		holder TEXT NOT NULL,
		lease_sec INTEGER NOT NULL,
		acquired_at DATETIME NOT NULL,
		expires_at DATETIME NOT NULL,
		active INTEGER NOT NULL DEFAULT 1
	);

	CREATE TABLE IF NOT EXISTS wait_queue (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		holder TEXT NOT NULL,
		reentrant INTEGER NOT NULL DEFAULT 0,
		lease_sec INTEGER NOT NULL,
		enqueued_at DATETIME NOT NULL,
		timeout_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS op_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		holder TEXT NOT NULL,
		operation TEXT NOT NULL,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_leases_active ON leases(active);
	CREATE INDEX IF NOT EXISTS idx_leases_lock_name ON leases(lock_name);
	CREATE INDEX IF NOT EXISTS idx_wait_queue_lock_name ON wait_queue(lock_name);
	CREATE INDEX IF NOT EXISTS idx_history_lock_name ON op_history(lock_name);

	CREATE TABLE IF NOT EXISTS rl_policies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		algorithm TEXT NOT NULL,
		window_sec INTEGER DEFAULT 0,
		max_tokens INTEGER NOT NULL,
		refill_rate REAL DEFAULT 0,
		refill_unit TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS rl_caller_bindings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		policy_name TEXT NOT NULL,
		quota_limit INTEGER NOT NULL,
		used_tokens INTEGER NOT NULL DEFAULT 0,
		borrowed_tokens INTEGER NOT NULL DEFAULT 0,
		lent_tokens INTEGER NOT NULL DEFAULT 0,
		reserved_tokens INTEGER NOT NULL DEFAULT 0,
		last_refill_at DATETIME,
		window_start_at DATETIME,
		prev_window_count INTEGER NOT NULL DEFAULT 0,
		curr_window_count INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS rl_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		policy_name TEXT NOT NULL,
		requested INTEGER NOT NULL,
		granted INTEGER NOT NULL,
		allowed INTEGER NOT NULL DEFAULT 1,
		reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS rl_borrow_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		from_caller TEXT NOT NULL,
		to_caller TEXT NOT NULL,
		amount INTEGER NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		returned_at DATETIME
	);

	CREATE TABLE IF NOT EXISTS rl_wait_queue (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		tokens INTEGER NOT NULL,
		enqueued_at DATETIME NOT NULL,
		timeout_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS rl_reservations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		policy_name TEXT NOT NULL,
		caller_id TEXT NOT NULL,
		tokens INTEGER NOT NULL,
		start_at DATETIME NOT NULL,
		end_at DATETIME NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_rl_events_caller ON rl_events(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rl_events_policy ON rl_events(policy_name);
	CREATE INDEX IF NOT EXISTS idx_rl_borrow_from ON rl_borrow_records(from_caller);
	CREATE INDEX IF NOT EXISTS idx_rl_borrow_to ON rl_borrow_records(to_caller);
	CREATE INDEX IF NOT EXISTS idx_rl_wait_caller ON rl_wait_queue(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_policy ON rl_reservations(policy_name);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_caller ON rl_reservations(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_status ON rl_reservations(status);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_start ON rl_reservations(start_at);
	CREATE INDEX IF NOT EXISTS idx_rl_reservations_end ON rl_reservations(end_at);

	CREATE TABLE IF NOT EXISTS orch_txs (
		id TEXT PRIMARY KEY,
		holder TEXT NOT NULL,
		status TEXT NOT NULL,
		timeout_sec INTEGER NOT NULL,
		fail_reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		expires_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS orch_tx_locks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tx_id TEXT NOT NULL,
		lock_name TEXT NOT NULL,
		lease_sec INTEGER NOT NULL,
		holder TEXT NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS orch_tx_tokens (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tx_id TEXT NOT NULL,
		caller_id TEXT NOT NULL,
		tokens INTEGER NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS orch_tx_state_changes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		tx_id TEXT NOT NULL,
		from_state TEXT NOT NULL,
		to_state TEXT NOT NULL,
		reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_orch_txs_status ON orch_txs(status);
	CREATE INDEX IF NOT EXISTS idx_orch_txs_expires ON orch_txs(expires_at);
	CREATE INDEX IF NOT EXISTS idx_orch_tx_locks_tx ON orch_tx_locks(tx_id);
	CREATE INDEX IF NOT EXISTS idx_orch_tx_tokens_tx ON orch_tx_tokens(tx_id);
	CREATE INDEX IF NOT EXISTS idx_orch_tx_state_changes_tx ON orch_tx_state_changes(tx_id);

	CREATE TABLE IF NOT EXISTS audit_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME NOT NULL,
		caller TEXT NOT NULL,
		operation TEXT NOT NULL,
		resource TEXT NOT NULL,
		success INTEGER NOT NULL DEFAULT 1,
		fail_reason TEXT DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS idx_audit_caller ON audit_logs(caller);
	CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_logs(resource);
	CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_audit_success ON audit_logs(success);

	CREATE TABLE IF NOT EXISTS cb_rules (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		window_sec INTEGER NOT NULL,
		failure_threshold INTEGER NOT NULL,
		cooldown_sec INTEGER NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS cb_status (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		state TEXT NOT NULL DEFAULT 'closed',
		triggered_at DATETIME,
		expires_at DATETIME,
		failures_in_window INTEGER NOT NULL DEFAULT 0,
		trigger_reason TEXT DEFAULT '',
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_cb_status_state ON cb_status(state);

	CREATE TABLE IF NOT EXISTS cb_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		state TEXT NOT NULL,
		triggered_at DATETIME NOT NULL,
		recovered_at DATETIME,
		trigger_reason TEXT DEFAULT '',
		recover_reason TEXT DEFAULT ''
	);

	CREATE INDEX IF NOT EXISTS idx_cb_history_caller ON cb_history(caller_id);

	CREATE TABLE IF NOT EXISTS topo_nodes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		lock_name TEXT NOT NULL,
		rate_policy TEXT DEFAULT '',
		token_cost INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS topo_edges (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		from_node TEXT NOT NULL,
		to_node TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(from_node, to_node)
	);

	CREATE INDEX IF NOT EXISTS idx_topo_edges_from ON topo_edges(from_node);
	CREATE INDEX IF NOT EXISTS idx_topo_edges_to ON topo_edges(to_node);

	CREATE TABLE IF NOT EXISTS topo_op_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		operation TEXT NOT NULL,
		target_node TEXT NOT NULL,
		holder TEXT NOT NULL,
		success INTEGER NOT NULL DEFAULT 1,
		rolled_back INTEGER NOT NULL DEFAULT 0,
		nodes_touched TEXT DEFAULT '',
		duration_ms INTEGER NOT NULL DEFAULT 0,
		message TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_topo_op_holder ON topo_op_history(holder);
	CREATE INDEX IF NOT EXISTS idx_topo_op_created ON topo_op_history(created_at);

	CREATE TABLE IF NOT EXISTS shadow_plans (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		description TEXT DEFAULT '',
		status TEXT NOT NULL DEFAULT 'draft',
		mode TEXT NOT NULL DEFAULT 'replay',
		audit_log_start_id INTEGER DEFAULT 0,
		audit_log_end_id INTEGER DEFAULT 0,
		mirror_until DATETIME,
		applied_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS shadow_config_overrides (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		plan_id INTEGER NOT NULL,
		category TEXT NOT NULL,
		target_key TEXT NOT NULL,
		field TEXT NOT NULL,
		orig_value TEXT NOT NULL DEFAULT '',
		new_value TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(plan_id) REFERENCES shadow_plans(id)
	);

	CREATE INDEX IF NOT EXISTS idx_shadow_overrides_plan ON shadow_config_overrides(plan_id);

	CREATE TABLE IF NOT EXISTS shadow_diff_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		plan_id INTEGER NOT NULL,
		audit_log_id INTEGER DEFAULT 0,
		request_caller TEXT NOT NULL,
		request_op TEXT NOT NULL,
		request_resource TEXT NOT NULL,
		live_decision TEXT NOT NULL,
		shadow_decision TEXT NOT NULL,
		rule_category TEXT NOT NULL,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(plan_id) REFERENCES shadow_plans(id)
	);

	CREATE INDEX IF NOT EXISTS idx_shadow_diff_plan ON shadow_diff_records(plan_id);
	CREATE INDEX IF NOT EXISTS idx_shadow_diff_caller ON shadow_diff_records(request_caller);
	CREATE INDEX IF NOT EXISTS idx_shadow_diff_resource ON shadow_diff_records(request_resource);
	CREATE INDEX IF NOT EXISTS idx_shadow_diff_category ON shadow_diff_records(rule_category);

	CREATE TABLE IF NOT EXISTS debt_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		debtor TEXT NOT NULL,
		creditor TEXT NOT NULL,
		amount INTEGER NOT NULL,
		resource_type TEXT NOT NULL,
		resource_key TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'active',
		due_at DATETIME NOT NULL,
		collected_at DATETIME,
		overdue_at DATETIME,
		write_off_at DATETIME,
		source_event_id INTEGER DEFAULT 0,
		collect_attempts INTEGER NOT NULL DEFAULT 0,
		last_collect_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_debt_records_debtor ON debt_records(debtor);
	CREATE INDEX IF NOT EXISTS idx_debt_records_creditor ON debt_records(creditor);
	CREATE INDEX IF NOT EXISTS idx_debt_records_status ON debt_records(status);
	CREATE INDEX IF NOT EXISTS idx_debt_records_due ON debt_records(due_at);

	CREATE TABLE IF NOT EXISTS debt_ledger_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		debt_id INTEGER DEFAULT 0,
		debtor TEXT NOT NULL,
		creditor TEXT NOT NULL,
		event_type TEXT NOT NULL,
		amount INTEGER NOT NULL,
		resource_type TEXT NOT NULL DEFAULT '',
		resource_key TEXT NOT NULL DEFAULT '',
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_debt_events_debtor ON debt_ledger_events(debtor);
	CREATE INDEX IF NOT EXISTS idx_debt_events_creditor ON debt_ledger_events(creditor);
	CREATE INDEX IF NOT EXISTS idx_debt_events_type ON debt_ledger_events(event_type);
	CREATE INDEX IF NOT EXISTS idx_debt_events_debt ON debt_ledger_events(debt_id);

	CREATE TABLE IF NOT EXISTS debt_restrictions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		restriction_type TEXT NOT NULL,
		scope TEXT NOT NULL,
		overdue_threshold INTEGER NOT NULL DEFAULT 1,
		reason TEXT DEFAULT '',
		active INTEGER NOT NULL DEFAULT 1,
		lifted_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_debt_restrictions_caller ON debt_restrictions(caller_id);
	CREATE INDEX IF NOT EXISTS idx_debt_restrictions_active ON debt_restrictions(active);

	CREATE TABLE IF NOT EXISTS liquidation_rules (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		grace_period_sec INTEGER NOT NULL DEFAULT 60,
		overdue_threshold INTEGER NOT NULL DEFAULT 2,
		restriction_type TEXT NOT NULL DEFAULT 'reject',
		restriction_scope TEXT NOT NULL DEFAULT 'all',
		max_collect_retries INTEGER NOT NULL DEFAULT 3,
		protection_after INTEGER NOT NULL DEFAULT 5,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS liquidation_audit (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		debt_id INTEGER NOT NULL,
		debtor TEXT NOT NULL,
		creditor TEXT NOT NULL,
		action TEXT NOT NULL,
		amount INTEGER NOT NULL,
		success INTEGER NOT NULL DEFAULT 1,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_liquidation_audit_debtor ON liquidation_audit(debtor);
	CREATE INDEX IF NOT EXISTS idx_liquidation_audit_debt ON liquidation_audit(debt_id);

	CREATE TABLE IF NOT EXISTS handovers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		from_caller TEXT NOT NULL,
		to_caller TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'created',
		initiator TEXT NOT NULL,
		description TEXT DEFAULT '',
		need_confirm INTEGER NOT NULL DEFAULT 1,
		confirm_timeout_sec INTEGER NOT NULL DEFAULT 3600,
		confirm_deadline DATETIME,
		confirmed_at DATETIME,
		cancelled_at DATETIME,
		completed_at DATETIME,
		cancel_reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_handovers_from ON handovers(from_caller);
	CREATE INDEX IF NOT EXISTS idx_handovers_to ON handovers(to_caller);
	CREATE INDEX IF NOT EXISTS idx_handovers_status ON handovers(status);
	CREATE INDEX IF NOT EXISTS idx_handovers_deadline ON handovers(confirm_deadline);

	CREATE TABLE IF NOT EXISTS handover_resources (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		handover_id INTEGER NOT NULL,
		resource_type TEXT NOT NULL,
		resource_key TEXT NOT NULL,
		resource_name TEXT DEFAULT '',
		precheck_status TEXT NOT NULL DEFAULT 'ok',
		precheck_detail TEXT DEFAULT '',
		current_holder TEXT DEFAULT '',
		snapshot TEXT DEFAULT '',
		executed INTEGER NOT NULL DEFAULT 0,
		rolled_back INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(handover_id) REFERENCES handovers(id)
	);

	CREATE INDEX IF NOT EXISTS idx_hr_handover ON handover_resources(handover_id);
	CREATE INDEX IF NOT EXISTS idx_hr_type_key ON handover_resources(resource_type, resource_key);

	CREATE TABLE IF NOT EXISTS handover_timeline (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		handover_id INTEGER NOT NULL,
		status TEXT NOT NULL,
		operator TEXT NOT NULL,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(handover_id) REFERENCES handovers(id)
	);

	CREATE INDEX IF NOT EXISTS idx_ht_handover ON handover_timeline(handover_id);

	CREATE TABLE IF NOT EXISTS heartbeat_registrations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		group_name TEXT DEFAULT '',
		interval_sec INTEGER NOT NULL,
		max_missed INTEGER NOT NULL,
		strategy TEXT NOT NULL,
		last_heartbeat_at DATETIME NOT NULL,
		next_expected_at DATETIME NOT NULL,
		missed_count INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'healthy',
		frozen_at DATETIME,
		lost_at DATETIME,
		recovered_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS heartbeat_groups (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		survival_threshold INTEGER NOT NULL DEFAULT 1,
		status TEXT NOT NULL DEFAULT 'healthy',
		alive_count INTEGER NOT NULL DEFAULT 0,
		total_count INTEGER NOT NULL DEFAULT 0,
		degraded INTEGER NOT NULL DEFAULT 0,
		degraded_reason TEXT DEFAULT '',
		degraded_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS heartbeat_group_dependencies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		group_name TEXT NOT NULL,
		depends_on TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(group_name, depends_on)
	);

	CREATE TABLE IF NOT EXISTS heartbeat_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		from_status TEXT,
		to_status TEXT,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS frozen_resources (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		resource_type TEXT NOT NULL,
		resource_key TEXT NOT NULL,
		frozen_at DATETIME NOT NULL,
		released_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_hb_reg_status ON heartbeat_registrations(status);
	CREATE INDEX IF NOT EXISTS idx_hb_reg_caller ON heartbeat_registrations(caller_id);
	CREATE INDEX IF NOT EXISTS idx_hb_reg_group ON heartbeat_registrations(group_name);
	CREATE INDEX IF NOT EXISTS idx_hb_groups_name ON heartbeat_groups(name);
	CREATE INDEX IF NOT EXISTS idx_hb_groups_status ON heartbeat_groups(status);
	CREATE INDEX IF NOT EXISTS idx_hb_groups_degraded ON heartbeat_groups(degraded);
	CREATE INDEX IF NOT EXISTS idx_hb_group_deps_group ON heartbeat_group_dependencies(group_name);
	CREATE INDEX IF NOT EXISTS idx_hb_group_deps_depends ON heartbeat_group_dependencies(depends_on);
	CREATE INDEX IF NOT EXISTS idx_hb_events_caller ON heartbeat_events(caller_id);
	CREATE INDEX IF NOT EXISTS idx_hb_events_type ON heartbeat_events(event_type);
	CREATE INDEX IF NOT EXISTS idx_frozen_caller ON frozen_resources(caller_id);
	CREATE INDEX IF NOT EXISTS idx_frozen_released ON frozen_resources(released_at);

	CREATE TABLE IF NOT EXISTS heatmap_lock_stats (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		minute_bucket DATETIME NOT NULL,
		request_count INTEGER NOT NULL DEFAULT 0,
		wait_count INTEGER NOT NULL DEFAULT 0,
		total_wait_ms INTEGER NOT NULL DEFAULT 0,
		max_wait_ms INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(lock_name, minute_bucket)
	);

	CREATE INDEX IF NOT EXISTS idx_heatmap_lock_name ON heatmap_lock_stats(lock_name);
	CREATE INDEX IF NOT EXISTS idx_heatmap_bucket ON heatmap_lock_stats(minute_bucket);
	CREATE INDEX IF NOT EXISTS idx_heatmap_name_bucket ON heatmap_lock_stats(lock_name, minute_bucket);

	CREATE TABLE IF NOT EXISTS heatmap_alerts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		avg_wait_ms REAL NOT NULL DEFAULT 0,
		threshold_ms REAL NOT NULL DEFAULT 0,
		request_count INTEGER NOT NULL DEFAULT 0,
		wait_count INTEGER NOT NULL DEFAULT 0,
		max_wait_ms INTEGER NOT NULL DEFAULT 0,
		current_queue_len INTEGER NOT NULL DEFAULT 0,
		window_minutes INTEGER NOT NULL DEFAULT 5,
		alert_type TEXT NOT NULL DEFAULT 'avg_wait_exceeded',
		detail TEXT DEFAULT '',
		acknowledged INTEGER NOT NULL DEFAULT 0,
		acknowledged_at DATETIME,
		acknowledged_by TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_heatmap_alerts_lock ON heatmap_alerts(lock_name);
	CREATE INDEX IF NOT EXISTS idx_heatmap_alerts_ack ON heatmap_alerts(acknowledged);
	CREATE INDEX IF NOT EXISTS idx_heatmap_alerts_created ON heatmap_alerts(created_at);

	CREATE TABLE IF NOT EXISTS heatmap_config (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		window_minutes INTEGER NOT NULL DEFAULT 5,
		alert_threshold_ms REAL NOT NULL DEFAULT 5000,
		alert_suppress_min INTEGER NOT NULL DEFAULT 10,
		top_n INTEGER NOT NULL DEFAULT 10,
		history_retention_min INTEGER NOT NULL DEFAULT 1440,
		cooldown_enabled INTEGER NOT NULL DEFAULT 1,
		cooldown_consecutive_hot_cycles INTEGER NOT NULL DEFAULT 3,
		cooldown_lease_sec INTEGER NOT NULL DEFAULT 30,
		cooldown_lease_min_pct REAL NOT NULL DEFAULT 10,
		cooldown_resolve_threshold_ms REAL NOT NULL DEFAULT 1000,
		cooldown_resolve_cycles INTEGER NOT NULL DEFAULT 2,
		cooldown_max_sec INTEGER NOT NULL DEFAULT 3600,
		cooldown_accelerated_grant INTEGER NOT NULL DEFAULT 1,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS lock_cooldown_states (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		trigger_type TEXT NOT NULL DEFAULT 'auto',
		original_lease_sec INTEGER NOT NULL DEFAULT 0,
		cooldown_lease_sec INTEGER NOT NULL DEFAULT 0,
		leases_shortened INTEGER NOT NULL DEFAULT 0,
		consecutive_hot_cycles INTEGER NOT NULL DEFAULT 0,
		avg_wait_ms_at_start REAL NOT NULL DEFAULT 0,
		threshold_ms_at_start REAL NOT NULL DEFAULT 0,
		started_at DATETIME NOT NULL,
		resolved_at DATETIME,
		resolve_reason TEXT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);

	CREATE UNIQUE INDEX IF NOT EXISTS idx_cooldown_states_lock_active ON lock_cooldown_states(lock_name) WHERE status = 'active';
	CREATE INDEX IF NOT EXISTS idx_cooldown_states_status ON lock_cooldown_states(status);
	CREATE INDEX IF NOT EXISTS idx_cooldown_states_started ON lock_cooldown_states(started_at);

	CREATE TABLE IF NOT EXISTS cooldown_history_records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		lock_name TEXT NOT NULL,
		trigger_type TEXT NOT NULL,
		original_lease_sec INTEGER NOT NULL DEFAULT 0,
		cooldown_lease_sec INTEGER NOT NULL DEFAULT 0,
		leases_shortened INTEGER NOT NULL DEFAULT 0,
		avg_wait_ms_at_start REAL NOT NULL DEFAULT 0,
		avg_wait_ms_at_end REAL NOT NULL DEFAULT 0,
		threshold_ms REAL NOT NULL DEFAULT 0,
		duration_sec REAL NOT NULL DEFAULT 0,
		started_at DATETIME NOT NULL,
		ended_at DATETIME NOT NULL,
		resolve_reason TEXT,
		created_at DATETIME NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_cooldown_history_lock ON cooldown_history_records(lock_name);
	CREATE INDEX IF NOT EXISTS idx_cooldown_history_created ON cooldown_history_records(created_at);

	CREATE TABLE IF NOT EXISTS lock_budget_configs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		budget_limit INTEGER NOT NULL,
		period_sec INTEGER NOT NULL,
		warning_pct INTEGER NOT NULL DEFAULT 80,
		overdraft_limit INTEGER NOT NULL DEFAULT 0,
		max_concurrent_locks INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS lock_budget_holdings (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		lock_name TEXT NOT NULL,
		acquired_at DATETIME NOT NULL,
		expires_at DATETIME NOT NULL,
		last_metered_at DATETIME NOT NULL,
		units_accrued INTEGER NOT NULL DEFAULT 0,
		UNIQUE(caller_id, lock_name)
	);

	CREATE INDEX IF NOT EXISTS idx_budget_holdings_caller ON lock_budget_holdings(caller_id);
	CREATE INDEX IF NOT EXISTS idx_budget_holdings_lock ON lock_budget_holdings(lock_name);
	CREATE INDEX IF NOT EXISTS idx_budget_holdings_expires ON lock_budget_holdings(expires_at);

	CREATE TABLE IF NOT EXISTS lock_budget_exhaust_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		consumed_units INTEGER NOT NULL,
		budget_limit INTEGER NOT NULL,
		period_start_at DATETIME NOT NULL,
		period_end_at DATETIME NOT NULL,
		attempted_lock TEXT DEFAULT '',
		units_requested INTEGER NOT NULL DEFAULT 0,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_budget_exhaust_caller ON lock_budget_exhaust_events(caller_id);
	CREATE INDEX IF NOT EXISTS idx_budget_exhaust_created ON lock_budget_exhaust_events(created_at);

	CREATE TABLE IF NOT EXISTS lock_budget_period_summaries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		period_start_at DATETIME NOT NULL,
		period_end_at DATETIME NOT NULL,
		budget_limit INTEGER NOT NULL,
		overdraft_limit INTEGER NOT NULL DEFAULT 0,
		total_consumed INTEGER NOT NULL,
		overdraft_used INTEGER NOT NULL DEFAULT 0,
		overdraft_penalty INTEGER NOT NULL DEFAULT 0,
		transferred_in INTEGER NOT NULL DEFAULT 0,
		transferred_out INTEGER NOT NULL DEFAULT 0,
		carry_over_deduction INTEGER NOT NULL DEFAULT 0,
		peak_concurrent INTEGER NOT NULL DEFAULT 0,
		lock_count INTEGER NOT NULL DEFAULT 0,
		exhaust_events INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(caller_id, period_start_at)
	);

	CREATE INDEX IF NOT EXISTS idx_budget_summary_caller ON lock_budget_period_summaries(caller_id);
	CREATE INDEX IF NOT EXISTS idx_budget_summary_period ON lock_budget_period_summaries(period_start_at);

	CREATE TABLE IF NOT EXISTS lock_budget_transfers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		from_caller TEXT NOT NULL,
		to_caller TEXT NOT NULL,
		amount INTEGER NOT NULL,
		reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_budget_transfers_from ON lock_budget_transfers(from_caller);
	CREATE INDEX IF NOT EXISTS idx_budget_transfers_to ON lock_budget_transfers(to_caller);
	CREATE INDEX IF NOT EXISTS idx_budget_transfers_created ON lock_budget_transfers(created_at);

	CREATE TABLE IF NOT EXISTS lock_budget_settlement_bills (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		period_start_at DATETIME NOT NULL,
		period_end_at DATETIME NOT NULL,
		budget_limit INTEGER NOT NULL,
		overdraft_limit INTEGER NOT NULL DEFAULT 0,
		normal_consumption INTEGER NOT NULL DEFAULT 0,
		overdraft_consumption INTEGER NOT NULL DEFAULT 0,
		overdraft_penalty INTEGER NOT NULL DEFAULT 0,
		total_consumption INTEGER NOT NULL DEFAULT 0,
		transferred_in INTEGER NOT NULL DEFAULT 0,
		transferred_out INTEGER NOT NULL DEFAULT 0,
		ending_balance INTEGER NOT NULL DEFAULT 0,
		had_overdraft INTEGER NOT NULL DEFAULT 0,
		peak_overdraft INTEGER NOT NULL DEFAULT 0,
		peak_concurrent INTEGER NOT NULL DEFAULT 0,
		exhaust_events INTEGER NOT NULL DEFAULT 0,
		carry_over_to_next_period INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'finalized',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(caller_id, period_start_at)
	);

	CREATE INDEX IF NOT EXISTS idx_budget_bills_caller ON lock_budget_settlement_bills(caller_id);
	CREATE INDEX IF NOT EXISTS idx_budget_bills_period ON lock_budget_settlement_bills(period_start_at);
	CREATE INDEX IF NOT EXISTS idx_budget_bills_status ON lock_budget_settlement_bills(status);

	CREATE TABLE IF NOT EXISTS lock_budget_bill_transfer_details (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bill_id INTEGER NOT NULL,
		caller_id TEXT NOT NULL,
		direction TEXT NOT NULL,
		peer_caller TEXT NOT NULL,
		amount INTEGER NOT NULL,
		reason TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(bill_id) REFERENCES lock_budget_settlement_bills(id)
	);

	CREATE INDEX IF NOT EXISTS idx_budget_bill_details_bill ON lock_budget_bill_transfer_details(bill_id);
	CREATE INDEX IF NOT EXISTS idx_budget_bill_details_caller ON lock_budget_bill_transfer_details(caller_id);

	CREATE TABLE IF NOT EXISTS lock_budget_caller_arrears (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		arrears_amount INTEGER NOT NULL,
		original_bill_id INTEGER NOT NULL,
		original_period_start_at DATETIME NOT NULL,
		original_period_end_at DATETIME NOT NULL,
		status TEXT NOT NULL DEFAULT 'active',
		cleared_at DATETIME,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(original_bill_id) REFERENCES lock_budget_settlement_bills(id)
	);

	CREATE INDEX IF NOT EXISTS idx_budget_arrears_caller ON lock_budget_caller_arrears(caller_id);
	CREATE INDEX IF NOT EXISTS idx_budget_arrears_status ON lock_budget_caller_arrears(status);

	CREATE TABLE IF NOT EXISTS lock_budget_rate_alert_rules (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		window_sec INTEGER NOT NULL,
		max_units_in_window INTEGER NOT NULL,
		freeze_trigger_n INTEGER NOT NULL DEFAULT 3,
		enabled INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS lock_budget_rate_alert_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		window_sec INTEGER NOT NULL,
		max_units_in_window INTEGER NOT NULL,
		actual_rate REAL NOT NULL,
		consumed_in_window INTEGER NOT NULL,
		detail TEXT DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_rate_alert_events_caller ON lock_budget_rate_alert_events(caller_id);
	CREATE INDEX IF NOT EXISTS idx_rate_alert_events_created ON lock_budget_rate_alert_events(created_at);

	CREATE TABLE IF NOT EXISTS lock_budget_caller_freezes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		frozen_at DATETIME NOT NULL,
		frozen_by TEXT NOT NULL DEFAULT 'system',
		reason TEXT NOT NULL DEFAULT '',
		alert_count_before INTEGER NOT NULL DEFAULT 0,
		unfrozen_at DATETIME,
		unfrozen_by TEXT DEFAULT '',
		unfreeze_reason TEXT DEFAULT '',
		active INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_caller_freezes_active ON lock_budget_caller_freezes(active);
	CREATE INDEX IF NOT EXISTS idx_caller_freezes_caller ON lock_budget_caller_freezes(caller_id);

	CREATE TABLE IF NOT EXISTS caller_reputations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL UNIQUE,
		score REAL NOT NULL DEFAULT 0,
		tier TEXT NOT NULL DEFAULT 'silver',
		on_time_release_score REAL NOT NULL DEFAULT 0,
		overdraft_reverse_score REAL NOT NULL DEFAULT 0,
		circuit_breaker_score REAL NOT NULL DEFAULT 0,
		arrear_score REAL NOT NULL DEFAULT 0,
		rate_alert_score REAL NOT NULL DEFAULT 0,
		calculated_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_reputation_caller ON caller_reputations(caller_id);
	CREATE INDEX IF NOT EXISTS idx_reputation_tier ON caller_reputations(tier);
	CREATE INDEX IF NOT EXISTS idx_reputation_score ON caller_reputations(score DESC);

	CREATE TABLE IF NOT EXISTS tier_change_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		caller_id TEXT NOT NULL,
		old_tier TEXT NOT NULL,
		new_tier TEXT NOT NULL,
		score REAL NOT NULL,
		changed_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_tier_change_caller ON tier_change_events(caller_id);
	CREATE INDEX IF NOT EXISTS idx_tier_change_changed ON tier_change_events(changed_at);
	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return err
	}

	if err := s.migrateSchema(); err != nil {
		return err
	}
	if err := s.initCoordinationSchema(); err != nil {
		return err
	}

	return nil
}

func (s *Storage) migrateSchema() error {
	columns := []string{"reserved_tokens"}
	for _, col := range columns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('rl_caller_bindings') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to rl_caller_bindings", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE rl_caller_bindings ADD COLUMN %s INTEGER NOT NULL DEFAULT 0", col))
			if err != nil {
				return err
			}
		}
	}

	hbColumns := []string{"group_name"}
	for _, col := range hbColumns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('heartbeat_registrations') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to heartbeat_registrations", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE heartbeat_registrations ADD COLUMN %s TEXT NOT NULL DEFAULT ''", col))
			if err != nil {
				return err
			}
		}
	}

	hmCfgColumns := []string{"alert_suppress_min"}
	for _, col := range hmCfgColumns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('heatmap_config') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to heatmap_config", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE heatmap_config ADD COLUMN %s INTEGER NOT NULL DEFAULT 10", col))
			if err != nil {
				return err
			}
		}
	}

	cooldownIntColumns := map[string]int{
		"cooldown_enabled":                1,
		"cooldown_consecutive_hot_cycles": 3,
		"cooldown_lease_sec":              30,
		"cooldown_resolve_cycles":         2,
		"cooldown_max_sec":                3600,
		"cooldown_accelerated_grant":      1,
	}
	for col, defaultValue := range cooldownIntColumns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('heatmap_config') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to heatmap_config", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE heatmap_config ADD COLUMN %s INTEGER NOT NULL DEFAULT %d", col, defaultValue))
			if err != nil {
				return err
			}
		}
	}

	cooldownRealColumns := map[string]float64{
		"cooldown_lease_min_pct":        10.0,
		"cooldown_resolve_threshold_ms": 1000.0,
	}
	for col, defaultValue := range cooldownRealColumns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('heatmap_config') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to heatmap_config", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE heatmap_config ADD COLUMN %s REAL NOT NULL DEFAULT %f", col, defaultValue))
			if err != nil {
				return err
			}
		}
	}

	lbCfgColumns := map[string]int{
		"overdraft_limit":      0,
		"max_concurrent_locks": 0,
	}
	for col, defaultValue := range lbCfgColumns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('lock_budget_configs') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to lock_budget_configs", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE lock_budget_configs ADD COLUMN %s INTEGER NOT NULL DEFAULT %d", col, defaultValue))
			if err != nil {
				return err
			}
		}
	}

	lbSummaryIntColumns := map[string]int{
		"overdraft_limit":      0,
		"overdraft_used":       0,
		"overdraft_penalty":    0,
		"transferred_in":       0,
		"transferred_out":      0,
		"carry_over_deduction": 0,
	}
	for col, defaultValue := range lbSummaryIntColumns {
		row := s.db.QueryRow(`
			SELECT COUNT(*) FROM pragma_table_info('lock_budget_period_summaries') WHERE name = ?
		`, col)
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			log.Printf("[storage-migration] adding column %s to lock_budget_period_summaries", col)
			_, err := s.db.Exec(fmt.Sprintf(
				"ALTER TABLE lock_budget_period_summaries ADD COLUMN %s INTEGER NOT NULL DEFAULT %d", col, defaultValue))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Storage) GetLock(name string) (*model.Lock, error) {
	row := s.db.QueryRow(`
		SELECT id, name, status, holder, reentrant, count, created_at, updated_at
		FROM locks WHERE name = ?
	`, name)

	var l model.Lock
	var reentrantInt int
	err := row.Scan(&l.ID, &l.Name, &l.Status, &l.Holder, &reentrantInt, &l.Count, &l.CreatedAt, &l.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.Reentrant = reentrantInt != 0
	return &l, nil
}

func (s *Storage) UpsertLock(l *model.Lock) error {
	reentrantInt := 0
	if l.Reentrant {
		reentrantInt = 1
	}
	_, err := s.db.Exec(`
		INSERT INTO locks (name, status, holder, reentrant, count, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			status = excluded.status,
			holder = excluded.holder,
			reentrant = excluded.reentrant,
			count = excluded.count,
			updated_at = excluded.updated_at
	`, l.Name, l.Status, l.Holder, reentrantInt, l.Count, l.CreatedAt, l.UpdatedAt)
	return err
}

func (s *Storage) ListLocks() ([]model.Lock, error) {
	rows, err := s.db.Query(`
		SELECT id, name, status, holder, reentrant, count, created_at, updated_at
		FROM locks ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	locks := make([]model.Lock, 0)
	for rows.Next() {
		var l model.Lock
		var reentrantInt int
		if err := rows.Scan(&l.ID, &l.Name, &l.Status, &l.Holder, &reentrantInt, &l.Count, &l.CreatedAt, &l.UpdatedAt); err != nil {
			return nil, err
		}
		l.Reentrant = reentrantInt != 0
		locks = append(locks, l)
	}
	return locks, nil
}

func (s *Storage) GetActiveLease(lockName string) (*model.Lease, error) {
	row := s.db.QueryRow(`
		SELECT id, lock_name, holder, lease_sec, acquired_at, expires_at, active, fencing_token
		FROM leases WHERE lock_name = ? AND active = 1 ORDER BY id DESC LIMIT 1
	`, lockName)

	var l model.Lease
	var activeInt int
	err := row.Scan(&l.ID, &l.LockName, &l.Holder, &l.LeaseSec, &l.AcquiredAt, &l.ExpiresAt, &activeInt, &l.FencingToken)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.Active = activeInt != 0
	return &l, nil
}

func (s *Storage) CreateLease(l *model.Lease) error {
	activeInt := 0
	if l.Active {
		activeInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO leases (lock_name, holder, lease_sec, acquired_at, expires_at, active, fencing_token)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, l.LockName, l.Holder, l.LeaseSec, l.AcquiredAt, l.ExpiresAt, activeInt, l.FencingToken)
	if err != nil {
		return err
	}
	l.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) DeactivateLease(lockName string) error {
	_, err := s.db.Exec(`UPDATE leases SET active = 0 WHERE lock_name = ? AND active = 1`, lockName)
	return err
}

func (s *Storage) UpdateLeaseExpiry(lockName string, newExpiresAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE leases SET expires_at = ? WHERE lock_name = ? AND active = 1
	`, newExpiresAt, lockName)
	return err
}

func (s *Storage) ListActiveLeases() ([]model.Lease, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, lease_sec, acquired_at, expires_at, active, fencing_token
		FROM leases WHERE active = 1 ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var leases []model.Lease
	for rows.Next() {
		var l model.Lease
		var activeInt int
		if err := rows.Scan(&l.ID, &l.LockName, &l.Holder, &l.LeaseSec, &l.AcquiredAt, &l.ExpiresAt, &activeInt, &l.FencingToken); err != nil {
			return nil, err
		}
		l.Active = activeInt != 0
		leases = append(leases, l)
	}
	return leases, nil
}

func (s *Storage) Enqueue(item *model.WaitQueueItem) error {
	result, err := s.db.Exec(`
		INSERT INTO wait_queue (lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, item.LockName, item.Holder, boolToInt(item.Reentrant), item.LeaseSec, item.EnqueuedAt, item.TimeoutAt)
	if err != nil {
		return err
	}
	item.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) Dequeue(lockName string) (*model.WaitQueueItem, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at
		FROM wait_queue WHERE lock_name = ? ORDER BY id LIMIT 1
	`, lockName)

	var item model.WaitQueueItem
	var reentrantInt int
	err = row.Scan(&item.ID, &item.LockName, &item.Holder, &reentrantInt, &item.LeaseSec, &item.EnqueuedAt, &item.TimeoutAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.Reentrant = reentrantInt != 0

	_, err = tx.Exec(`DELETE FROM wait_queue WHERE id = ?`, item.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *Storage) RemoveFromQueue(lockName, holder string) error {
	_, err := s.db.Exec(`DELETE FROM wait_queue WHERE lock_name = ? AND holder = ?`, lockName, holder)
	return err
}

func (s *Storage) RemoveFromQueueByID(id int64) error {
	_, err := s.db.Exec(`DELETE FROM wait_queue WHERE id = ?`, id)
	return err
}

func (s *Storage) ListWaitQueue(lockName string) ([]model.WaitQueueItem, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at
		FROM wait_queue WHERE lock_name = ? ORDER BY id
	`, lockName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.WaitQueueItem
	for rows.Next() {
		var item model.WaitQueueItem
		var reentrantInt int
		if err := rows.Scan(&item.ID, &item.LockName, &item.Holder, &reentrantInt, &item.LeaseSec, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		item.Reentrant = reentrantInt != 0
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) ListAllWaitQueue() ([]model.WaitQueueItem, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, reentrant, lease_sec, enqueued_at, timeout_at
		FROM wait_queue ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.WaitQueueItem
	for rows.Next() {
		var item model.WaitQueueItem
		var reentrantInt int
		if err := rows.Scan(&item.ID, &item.LockName, &item.Holder, &reentrantInt, &item.LeaseSec, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		item.Reentrant = reentrantInt != 0
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) WaitQueueLen(lockName string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM wait_queue WHERE lock_name = ?`, lockName).Scan(&count)
	return count, err
}

func (s *Storage) AddHistory(h *model.OperationHistory) error {
	result, err := s.db.Exec(`
		INSERT INTO op_history (lock_name, holder, operation, detail, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, h.LockName, h.Holder, h.Operation, h.Detail, h.CreatedAt)
	if err != nil {
		return err
	}
	h.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListHistory(lockName string, limit int) ([]model.OperationHistory, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, lock_name, holder, operation, detail, created_at
		FROM op_history WHERE lock_name = ? ORDER BY id DESC LIMIT ?
	`, lockName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []model.OperationHistory
	for rows.Next() {
		var h model.OperationHistory
		if err := rows.Scan(&h.ID, &h.LockName, &h.Holder, &h.Operation, &h.Detail, &h.CreatedAt); err != nil {
			return nil, err
		}
		history = append(history, h)
	}
	return history, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *Storage) LogInfo(format string, v ...interface{}) {
	log.Printf("[storage] "+format, v...)
}

func (s *Storage) CreatePolicy(p *model.RateLimitPolicy) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_policies (name, algorithm, window_sec, max_tokens, refill_rate, refill_unit, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, p.Name, p.Algorithm, p.WindowSec, p.MaxTokens, p.RefillRate, p.RefillUnit, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return err
	}
	p.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetPolicy(name string) (*model.RateLimitPolicy, error) {
	row := s.db.QueryRow(`
		SELECT id, name, algorithm, window_sec, max_tokens, refill_rate, refill_unit, created_at, updated_at
		FROM rl_policies WHERE name = ?
	`, name)

	var p model.RateLimitPolicy
	err := row.Scan(&p.ID, &p.Name, &p.Algorithm, &p.WindowSec, &p.MaxTokens, &p.RefillRate, &p.RefillUnit, &p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Storage) ListPolicies() ([]model.RateLimitPolicy, error) {
	rows, err := s.db.Query(`
		SELECT id, name, algorithm, window_sec, max_tokens, refill_rate, refill_unit, created_at, updated_at
		FROM rl_policies ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []model.RateLimitPolicy
	for rows.Next() {
		var p model.RateLimitPolicy
		if err := rows.Scan(&p.ID, &p.Name, &p.Algorithm, &p.WindowSec, &p.MaxTokens, &p.RefillRate, &p.RefillUnit, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		policies = append(policies, p)
	}
	return policies, nil
}

func (s *Storage) UpsertCallerBinding(b *model.CallerBinding) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_caller_bindings (caller_id, policy_name, quota_limit, used_tokens, borrowed_tokens, lent_tokens, reserved_tokens, last_refill_at, window_start_at, prev_window_count, curr_window_count, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			policy_name = excluded.policy_name,
			quota_limit = excluded.quota_limit,
			used_tokens = excluded.used_tokens,
			borrowed_tokens = excluded.borrowed_tokens,
			lent_tokens = excluded.lent_tokens,
			reserved_tokens = excluded.reserved_tokens,
			last_refill_at = excluded.last_refill_at,
			window_start_at = excluded.window_start_at,
			prev_window_count = excluded.prev_window_count,
			curr_window_count = excluded.curr_window_count,
			updated_at = excluded.updated_at
	`, b.CallerID, b.PolicyName, b.QuotaLimit, b.UsedTokens, b.BorrowedTokens, b.LentTokens, b.ReservedTokens,
		nullTime(b.LastRefillAt), nullTime(b.WindowStartAt), b.PrevWindowCount, b.CurrWindowCount, b.CreatedAt, b.UpdatedAt)
	if err != nil {
		return err
	}
	if b.ID == 0 {
		b.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetCallerBinding(callerID string) (*model.CallerBinding, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, policy_name, quota_limit, used_tokens, borrowed_tokens, lent_tokens, reserved_tokens, last_refill_at, window_start_at, prev_window_count, curr_window_count, created_at, updated_at
		FROM rl_caller_bindings WHERE caller_id = ?
	`, callerID)

	var b model.CallerBinding
	var lastRefillAt, windowStartAt sql.NullTime
	err := row.Scan(&b.ID, &b.CallerID, &b.PolicyName, &b.QuotaLimit, &b.UsedTokens, &b.BorrowedTokens, &b.LentTokens, &b.ReservedTokens,
		&lastRefillAt, &windowStartAt, &b.PrevWindowCount, &b.CurrWindowCount, &b.CreatedAt, &b.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if lastRefillAt.Valid {
		b.LastRefillAt = lastRefillAt.Time
	}
	if windowStartAt.Valid {
		b.WindowStartAt = windowStartAt.Time
	}
	return &b, nil
}

func (s *Storage) ListCallerBindings() ([]model.CallerBinding, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, policy_name, quota_limit, used_tokens, borrowed_tokens, lent_tokens, reserved_tokens, last_refill_at, window_start_at, prev_window_count, curr_window_count, created_at, updated_at
		FROM rl_caller_bindings ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bindings []model.CallerBinding
	for rows.Next() {
		var b model.CallerBinding
		var lastRefillAt, windowStartAt sql.NullTime
		if err := rows.Scan(&b.ID, &b.CallerID, &b.PolicyName, &b.QuotaLimit, &b.UsedTokens, &b.BorrowedTokens, &b.LentTokens, &b.ReservedTokens,
			&lastRefillAt, &windowStartAt, &b.PrevWindowCount, &b.CurrWindowCount, &b.CreatedAt, &b.UpdatedAt); err != nil {
			return nil, err
		}
		if lastRefillAt.Valid {
			b.LastRefillAt = lastRefillAt.Time
		}
		if windowStartAt.Valid {
			b.WindowStartAt = windowStartAt.Time
		}
		bindings = append(bindings, b)
	}
	return bindings, nil
}

func (s *Storage) UpdateCallerBinding(b *model.CallerBinding) error {
	_, err := s.db.Exec(`
		UPDATE rl_caller_bindings SET
			policy_name = ?,
			quota_limit = ?,
			used_tokens = ?,
			borrowed_tokens = ?,
			lent_tokens = ?,
			reserved_tokens = ?,
			last_refill_at = ?,
			window_start_at = ?,
			prev_window_count = ?,
			curr_window_count = ?,
			updated_at = ?
		WHERE caller_id = ?
	`, b.PolicyName, b.QuotaLimit, b.UsedTokens, b.BorrowedTokens, b.LentTokens, b.ReservedTokens,
		nullTime(b.LastRefillAt), nullTime(b.WindowStartAt), b.PrevWindowCount, b.CurrWindowCount, b.UpdatedAt, b.CallerID)
	return err
}

func (s *Storage) AddRateLimitEvent(e *model.RateLimitEvent) error {
	allowedInt := 0
	if e.Allowed {
		allowedInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO rl_events (caller_id, policy_name, requested, granted, allowed, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, e.CallerID, e.PolicyName, e.Requested, e.Granted, allowedInt, e.Reason, e.CreatedAt)
	if err != nil {
		return err
	}
	e.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListRateLimitEvents(callerID string, limit int) ([]model.RateLimitEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, caller_id, policy_name, requested, granted, allowed, reason, created_at
		FROM rl_events WHERE caller_id = ? ORDER BY id DESC LIMIT ?
	`, callerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.RateLimitEvent
	for rows.Next() {
		var e model.RateLimitEvent
		var allowedInt int
		if err := rows.Scan(&e.ID, &e.CallerID, &e.PolicyName, &e.Requested, &e.Granted, &allowedInt, &e.Reason, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Allowed = allowedInt != 0
		events = append(events, e)
	}
	return events, nil
}

func (s *Storage) CountRateLimited(callerID string) (int64, error) {
	var count int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM rl_events WHERE caller_id = ? AND allowed = 0`, callerID).Scan(&count)
	return count, err
}

func (s *Storage) CountAllEvents() (int64, int64, error) {
	var total, allowed int64
	row := s.db.QueryRow(`SELECT COUNT(*), SUM(CASE WHEN allowed = 1 THEN 1 ELSE 0 END) FROM rl_events`)
	err := row.Scan(&total, &allowed)
	return total, allowed, err
}

func (s *Storage) CreateBorrowRecord(r *model.QuotaBorrowRecord) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_borrow_records (from_caller, to_caller, amount, status, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, r.FromCaller, r.ToCaller, r.Amount, r.Status, r.CreatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListActiveBorrows() ([]model.QuotaBorrowRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, from_caller, to_caller, amount, status, created_at, returned_at
		FROM rl_borrow_records WHERE status = 'active' ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []model.QuotaBorrowRecord
	for rows.Next() {
		var r model.QuotaBorrowRecord
		var returnedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.FromCaller, &r.ToCaller, &r.Amount, &r.Status, &r.CreatedAt, &returnedAt); err != nil {
			return nil, err
		}
		if returnedAt.Valid {
			r.ReturnedAt = returnedAt.Time
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) ReturnBorrow(fromCaller, toCaller string, amount int, returnedAt time.Time) error {
	row := s.db.QueryRow(`
		SELECT id FROM rl_borrow_records
		WHERE from_caller = ? AND to_caller = ? AND status = 'active' AND amount = ?
		ORDER BY id LIMIT 1
	`, fromCaller, toCaller, amount)

	var id int64
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
		UPDATE rl_borrow_records SET status = 'returned', returned_at = ?
		WHERE id = ?
	`, returnedAt, id)
	return err
}

func (s *Storage) AddWaitItem(item *model.RateLimitWaitItem) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_wait_queue (caller_id, tokens, enqueued_at, timeout_at)
		VALUES (?, ?, ?, ?)
	`, item.CallerID, item.Tokens, item.EnqueuedAt, item.TimeoutAt)
	if err != nil {
		return err
	}
	item.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListWaitItemsByCaller(callerID string) ([]model.RateLimitWaitItem, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, tokens, enqueued_at, timeout_at
		FROM rl_wait_queue WHERE caller_id = ? ORDER BY id
	`, callerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.RateLimitWaitItem
	for rows.Next() {
		var item model.RateLimitWaitItem
		if err := rows.Scan(&item.ID, &item.CallerID, &item.Tokens, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) ListAllWaitItems() ([]model.RateLimitWaitItem, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, tokens, enqueued_at, timeout_at
		FROM rl_wait_queue ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.RateLimitWaitItem
	for rows.Next() {
		var item model.RateLimitWaitItem
		if err := rows.Scan(&item.ID, &item.CallerID, &item.Tokens, &item.EnqueuedAt, &item.TimeoutAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Storage) RemoveWaitItem(id int64) error {
	_, err := s.db.Exec(`DELETE FROM rl_wait_queue WHERE id = ?`, id)
	return err
}

func (s *Storage) RemoveExpiredWaitItems(now time.Time) (int64, error) {
	result, err := s.db.Exec(`DELETE FROM rl_wait_queue WHERE timeout_at <= ?`, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Storage) CountWaitItems(callerID string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM rl_wait_queue WHERE caller_id = ?`, callerID).Scan(&count)
	return count, err
}

func (s *Storage) CreateReservation(r *model.QuotaReservation) error {
	result, err := s.db.Exec(`
		INSERT INTO rl_reservations (policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, r.PolicyName, r.CallerID, r.Tokens, r.StartAt, r.EndAt, r.Status, r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetReservation(id int64) (*model.QuotaReservation, error) {
	row := s.db.QueryRow(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations WHERE id = ?
	`, id)

	var r model.QuotaReservation
	err := row.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Storage) UpdateReservationStatus(id int64, status model.ReservationStatus, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE rl_reservations SET status = ?, updated_at = ? WHERE id = ?
	`, status, updatedAt, id)
	return err
}

func (s *Storage) CancelReservation(id int64, updatedAt time.Time) error {
	return s.UpdateReservationStatus(id, model.ReservationStatusCancelled, updatedAt)
}

func (s *Storage) ListReservationsByPolicy(policyName string, status string) ([]model.QuotaReservation, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE policy_name = ? AND status = ? ORDER BY start_at
		`, policyName, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE policy_name = ? ORDER BY start_at
		`, policyName)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListReservationsByCaller(callerID string, status string) ([]model.QuotaReservation, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE caller_id = ? AND status = ? ORDER BY start_at
		`, callerID, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE caller_id = ? ORDER BY start_at
		`, callerID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListAllReservations(status string) ([]model.QuotaReservation, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations WHERE status = ? ORDER BY start_at
		`, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
			FROM rl_reservations ORDER BY start_at
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListReservationsInTimeRange(policyName string, startAt time.Time, endAt time.Time) ([]model.QuotaReservation, error) {
	rows, err := s.db.Query(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations
		WHERE policy_name = ?
		  AND status IN ('pending', 'active')
		  AND start_at < ?
		  AND end_at > ?
		ORDER BY start_at
	`, policyName, endAt, startAt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListPendingReservationsToStart(now time.Time) ([]model.QuotaReservation, error) {
	rows, err := s.db.Query(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations
		WHERE status = 'pending' AND start_at <= ?
		ORDER BY start_at
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func (s *Storage) ListActiveReservationsToEnd(now time.Time) ([]model.QuotaReservation, error) {
	rows, err := s.db.Query(`
		SELECT id, policy_name, caller_id, tokens, start_at, end_at, status, created_at, updated_at
		FROM rl_reservations
		WHERE status = 'active' AND end_at <= ?
		ORDER BY end_at
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []model.QuotaReservation
	for rows.Next() {
		var r model.QuotaReservation
		if err := rows.Scan(&r.ID, &r.PolicyName, &r.CallerID, &r.Tokens, &r.StartAt, &r.EndAt, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, r)
	}
	return reservations, nil
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

func (s *Storage) CreateOrchTx(tx *model.OrchestrationTx) error {
	_, err := s.db.Exec(`
		INSERT INTO orch_txs (id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, tx.ID, tx.Holder, tx.Status, tx.TimeoutSec, tx.FailReason, tx.CreatedAt, tx.UpdatedAt, tx.ExpiresAt)
	return err
}

func (s *Storage) UpdateOrchTxStatus(txID string, status model.TxStatus, failReason string, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE orch_txs SET status = ?, fail_reason = ?, updated_at = ? WHERE id = ?
	`, status, failReason, updatedAt, txID)
	return err
}

func (s *Storage) GetOrchTx(txID string) (*model.OrchestrationTx, error) {
	row := s.db.QueryRow(`
		SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
		FROM orch_txs WHERE id = ?
	`, txID)

	var tx model.OrchestrationTx
	err := row.Scan(&tx.ID, &tx.Holder, &tx.Status, &tx.TimeoutSec, &tx.FailReason,
		&tx.CreatedAt, &tx.UpdatedAt, &tx.ExpiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

func (s *Storage) ListOrchTxs(status string) ([]model.OrchestrationTx, error) {
	var rows *sql.Rows
	var err error

	if status != "" {
		rows, err = s.db.Query(`
			SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
			FROM orch_txs WHERE status = ? ORDER BY created_at DESC
		`, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
			FROM orch_txs ORDER BY created_at DESC
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []model.OrchestrationTx
	for rows.Next() {
		var tx model.OrchestrationTx
		if err := rows.Scan(&tx.ID, &tx.Holder, &tx.Status, &tx.TimeoutSec, &tx.FailReason,
			&tx.CreatedAt, &tx.UpdatedAt, &tx.ExpiresAt); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (s *Storage) ListActiveOrchTxs(now time.Time) ([]model.OrchestrationTx, error) {
	rows, err := s.db.Query(`
		SELECT id, holder, status, timeout_sec, fail_reason, created_at, updated_at, expires_at
		FROM orch_txs WHERE status = ? ORDER BY created_at DESC
	`, model.TxStatusCommitted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txs []model.OrchestrationTx
	for rows.Next() {
		var tx model.OrchestrationTx
		if err := rows.Scan(&tx.ID, &tx.Holder, &tx.Status, &tx.TimeoutSec, &tx.FailReason,
			&tx.CreatedAt, &tx.UpdatedAt, &tx.ExpiresAt); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (s *Storage) AddTxLock(txLock *model.TxLock) error {
	result, err := s.db.Exec(`
		INSERT INTO orch_tx_locks (tx_id, lock_name, lease_sec, holder, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, txLock.TxID, txLock.LockName, txLock.LeaseSec, txLock.Holder, txLock.CreatedAt)
	if err != nil {
		return err
	}
	txLock.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTxLocks(txID string) ([]model.TxLock, error) {
	rows, err := s.db.Query(`
		SELECT id, tx_id, lock_name, lease_sec, holder, created_at
		FROM orch_tx_locks WHERE tx_id = ? ORDER BY id
	`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locks []model.TxLock
	for rows.Next() {
		var l model.TxLock
		if err := rows.Scan(&l.ID, &l.TxID, &l.LockName, &l.LeaseSec, &l.Holder, &l.CreatedAt); err != nil {
			return nil, err
		}
		locks = append(locks, l)
	}
	return locks, nil
}

func (s *Storage) AddTxToken(txToken *model.TxToken) error {
	result, err := s.db.Exec(`
		INSERT INTO orch_tx_tokens (tx_id, caller_id, tokens, created_at)
		VALUES (?, ?, ?, ?)
	`, txToken.TxID, txToken.CallerID, txToken.Tokens, txToken.CreatedAt)
	if err != nil {
		return err
	}
	txToken.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTxTokens(txID string) ([]model.TxToken, error) {
	rows, err := s.db.Query(`
		SELECT id, tx_id, caller_id, tokens, created_at
		FROM orch_tx_tokens WHERE tx_id = ? ORDER BY id
	`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []model.TxToken
	for rows.Next() {
		var t model.TxToken
		if err := rows.Scan(&t.ID, &t.TxID, &t.CallerID, &t.Tokens, &t.CreatedAt); err != nil {
			return nil, err
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}

func (s *Storage) AddTxStateChange(sc *model.TxStateChange) error {
	result, err := s.db.Exec(`
		INSERT INTO orch_tx_state_changes (tx_id, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, sc.TxID, sc.FromState, sc.ToState, sc.Reason, sc.CreatedAt)
	if err != nil {
		return err
	}
	sc.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTxStateChanges(txID string) ([]model.TxStateChange, error) {
	rows, err := s.db.Query(`
		SELECT id, tx_id, from_state, to_state, reason, created_at
		FROM orch_tx_state_changes WHERE tx_id = ? ORDER BY id
	`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []model.TxStateChange
	for rows.Next() {
		var sc model.TxStateChange
		if err := rows.Scan(&sc.ID, &sc.TxID, &sc.FromState, &sc.ToState, &sc.Reason, &sc.CreatedAt); err != nil {
			return nil, err
		}
		changes = append(changes, sc)
	}
	return changes, nil
}

func (s *Storage) AddAuditLog(log *model.AuditLog) error {
	successInt := 0
	if log.Success {
		successInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO audit_logs (timestamp, caller, operation, resource, success, fail_reason)
		VALUES (?, ?, ?, ?, ?, ?)
	`, log.Timestamp, log.Caller, log.Operation, log.Resource, successInt, log.FailReason)
	if err != nil {
		return err
	}
	log.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) QueryAuditLogs(caller, resource string, success *bool, startTime, endTime time.Time, page, pageSize int) ([]model.AuditLog, int64, error) {
	where := []string{"1=1"}
	args := []interface{}{}

	if caller != "" {
		where = append(where, "caller = ?")
		args = append(args, caller)
	}
	if resource != "" {
		where = append(where, "resource = ?")
		args = append(args, resource)
	}
	if success != nil {
		where = append(where, "success = ?")
		si := 0
		if *success {
			si = 1
		}
		args = append(args, si)
	}
	if !startTime.IsZero() {
		where = append(where, "timestamp >= ?")
		args = append(args, startTime)
	}
	if !endTime.IsZero() {
		where = append(where, "timestamp <= ?")
		args = append(args, endTime)
	}

	whereClause := ""
	for _, w := range where {
		if whereClause != "" {
			whereClause += " AND "
		}
		whereClause += w
	}

	var total int64
	countQuery := "SELECT COUNT(*) FROM audit_logs WHERE " + whereClause
	if err := s.db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	query := `SELECT id, timestamp, caller, operation, resource, success, fail_reason
		FROM audit_logs WHERE ` + whereClause + ` ORDER BY timestamp DESC LIMIT ? OFFSET ?`
	args = append(args, pageSize, offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var logs []model.AuditLog
	for rows.Next() {
		var l model.AuditLog
		var successInt int
		if err := rows.Scan(&l.ID, &l.Timestamp, &l.Caller, &l.Operation, &l.Resource, &successInt, &l.FailReason); err != nil {
			return nil, 0, err
		}
		l.Success = successInt != 0
		logs = append(logs, l)
	}
	return logs, total, nil
}

func (s *Storage) CountFailuresInWindow(caller string, windowSec int, now time.Time) (int, error) {
	startTime := now.Add(-time.Duration(windowSec) * time.Second)
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM audit_logs
		WHERE caller = ? AND success = 0 AND timestamp >= ?
	`, caller, startTime).Scan(&count)
	return count, err
}

func (s *Storage) CreateCircuitBreakerRule(rule *model.CircuitBreakerRule) error {
	result, err := s.db.Exec(`
		INSERT INTO cb_rules (caller_id, window_sec, failure_threshold, cooldown_sec, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			window_sec = excluded.window_sec,
			failure_threshold = excluded.failure_threshold,
			cooldown_sec = excluded.cooldown_sec,
			updated_at = excluded.updated_at
	`, rule.CallerID, rule.WindowSec, rule.FailureThreshold, rule.CooldownSec, rule.CreatedAt, rule.UpdatedAt)
	if err != nil {
		return err
	}
	if rule.ID == 0 {
		rule.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetCircuitBreakerRule(callerID string) (*model.CircuitBreakerRule, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, window_sec, failure_threshold, cooldown_sec, created_at, updated_at
		FROM cb_rules WHERE caller_id = ?
	`, callerID)

	var r model.CircuitBreakerRule
	err := row.Scan(&r.ID, &r.CallerID, &r.WindowSec, &r.FailureThreshold, &r.CooldownSec, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Storage) ListCircuitBreakerRules() ([]model.CircuitBreakerRule, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, window_sec, failure_threshold, cooldown_sec, created_at, updated_at
		FROM cb_rules ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []model.CircuitBreakerRule
	for rows.Next() {
		var r model.CircuitBreakerRule
		if err := rows.Scan(&r.ID, &r.CallerID, &r.WindowSec, &r.FailureThreshold, &r.CooldownSec, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func (s *Storage) DeleteCircuitBreakerRule(callerID string) error {
	_, err := s.db.Exec("DELETE FROM cb_rules WHERE caller_id = ?", callerID)
	return err
}

func (s *Storage) UpsertCircuitBreakerStatus(status *model.CircuitBreakerStatus) error {
	result, err := s.db.Exec(`
		INSERT INTO cb_status (caller_id, state, triggered_at, expires_at, failures_in_window, trigger_reason, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			state = excluded.state,
			triggered_at = excluded.triggered_at,
			expires_at = excluded.expires_at,
			failures_in_window = excluded.failures_in_window,
			trigger_reason = excluded.trigger_reason,
			updated_at = excluded.updated_at
	`, status.CallerID, status.State, nullTime(status.TriggeredAt), nullTime(status.ExpiresAt),
		status.FailuresInWindow, status.TriggerReason, status.UpdatedAt)
	if err != nil {
		return err
	}
	if status.ID == 0 {
		status.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetCircuitBreakerStatus(callerID string) (*model.CircuitBreakerStatus, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, state, triggered_at, expires_at, failures_in_window, trigger_reason, updated_at
		FROM cb_status WHERE caller_id = ?
	`, callerID)

	var st model.CircuitBreakerStatus
	var triggeredAt, expiresAt sql.NullTime
	err := row.Scan(&st.ID, &st.CallerID, &st.State, &triggeredAt, &expiresAt,
		&st.FailuresInWindow, &st.TriggerReason, &st.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if triggeredAt.Valid {
		st.TriggeredAt = triggeredAt.Time
	}
	if expiresAt.Valid {
		st.ExpiresAt = expiresAt.Time
	}
	return &st, nil
}

func (s *Storage) ListCircuitBreakerStatuses(state string) ([]model.CircuitBreakerStatus, error) {
	var rows *sql.Rows
	var err error

	if state != "" {
		rows, err = s.db.Query(`
			SELECT id, caller_id, state, triggered_at, expires_at, failures_in_window, trigger_reason, updated_at
			FROM cb_status WHERE state = ? ORDER BY updated_at DESC
		`, state)
	} else {
		rows, err = s.db.Query(`
			SELECT id, caller_id, state, triggered_at, expires_at, failures_in_window, trigger_reason, updated_at
			FROM cb_status ORDER BY updated_at DESC
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []model.CircuitBreakerStatus
	for rows.Next() {
		var st model.CircuitBreakerStatus
		var triggeredAt, expiresAt sql.NullTime
		if err := rows.Scan(&st.ID, &st.CallerID, &st.State, &triggeredAt, &expiresAt,
			&st.FailuresInWindow, &st.TriggerReason, &st.UpdatedAt); err != nil {
			return nil, err
		}
		if triggeredAt.Valid {
			st.TriggeredAt = triggeredAt.Time
		}
		if expiresAt.Valid {
			st.ExpiresAt = expiresAt.Time
		}
		statuses = append(statuses, st)
	}
	return statuses, nil
}

func (s *Storage) AddCircuitBreakerHistory(h *model.CircuitBreakerHistory) error {
	result, err := s.db.Exec(`
		INSERT INTO cb_history (caller_id, state, triggered_at, recovered_at, trigger_reason, recover_reason)
		VALUES (?, ?, ?, ?, ?, ?)
	`, h.CallerID, h.State, h.TriggeredAt, nullTime(h.RecoveredAt), h.TriggerReason, h.RecoverReason)
	if err != nil {
		return err
	}
	h.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) UpdateCircuitBreakerHistoryRecover(id int64, recoveredAt time.Time, recoverReason string) error {
	_, err := s.db.Exec(`
		UPDATE cb_history SET recovered_at = ?, recover_reason = ? WHERE id = ?
	`, recoveredAt, recoverReason, id)
	return err
}

func (s *Storage) ListCircuitBreakerHistory(callerID string, limit int) ([]model.CircuitBreakerHistory, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows *sql.Rows
	var err error

	if callerID != "" {
		rows, err = s.db.Query(`
			SELECT id, caller_id, state, triggered_at, recovered_at, trigger_reason, recover_reason
			FROM cb_history WHERE caller_id = ? ORDER BY triggered_at DESC LIMIT ?
		`, callerID, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT id, caller_id, state, triggered_at, recovered_at, trigger_reason, recover_reason
			FROM cb_history ORDER BY triggered_at DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []model.CircuitBreakerHistory
	for rows.Next() {
		var h model.CircuitBreakerHistory
		var recoveredAt sql.NullTime
		if err := rows.Scan(&h.ID, &h.CallerID, &h.State, &h.TriggeredAt, &recoveredAt,
			&h.TriggerReason, &h.RecoverReason); err != nil {
			return nil, err
		}
		if recoveredAt.Valid {
			h.RecoveredAt = recoveredAt.Time
		}
		history = append(history, h)
	}
	return history, nil
}

func (s *Storage) GetLatestOpenHistory(callerID string) (*model.CircuitBreakerHistory, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, state, triggered_at, recovered_at, trigger_reason, recover_reason
		FROM cb_history WHERE caller_id = ? AND state = 'open' AND recovered_at IS NULL
		ORDER BY triggered_at DESC LIMIT 1
	`, callerID)

	var h model.CircuitBreakerHistory
	var recoveredAt sql.NullTime
	err := row.Scan(&h.ID, &h.CallerID, &h.State, &h.TriggeredAt, &recoveredAt,
		&h.TriggerReason, &h.RecoverReason)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if recoveredAt.Valid {
		h.RecoveredAt = recoveredAt.Time
	}
	return &h, nil
}

func (s *Storage) GetCallerStats(callerID string, now time.Time) (*model.CallerStats, error) {
	var total, success, failure int64
	row := s.db.QueryRow(`
		SELECT COUNT(*), SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END)
		FROM audit_logs WHERE caller = ?
	`, callerID)
	if err := row.Scan(&total, &success, &failure); err != nil {
		return nil, err
	}

	req1Min, _ := s.countRequestsSince(callerID, now.Add(-1*time.Minute))
	req5Min, _ := s.countRequestsSince(callerID, now.Add(-5*time.Minute))
	req15Min, _ := s.countRequestsSince(callerID, now.Add(-15*time.Minute))

	stats := &model.CallerStats{
		CallerID:      callerID,
		TotalRequests: total,
		SuccessCount:  success,
		FailureCount:  failure,
		Requests1Min:  req1Min,
		Requests5Min:  req5Min,
		Requests15Min: req15Min,
	}
	if total > 0 {
		stats.SuccessRate = float64(success) / float64(total)
		stats.FailureRate = float64(failure) / float64(total)
	}
	return stats, nil
}

func (s *Storage) countRequestsSince(callerID string, since time.Time) (int64, error) {
	var count int64
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM audit_logs WHERE caller = ? AND timestamp >= ?
	`, callerID, since).Scan(&count)
	return count, err
}

func (s *Storage) GetAllCallerStats(now time.Time) ([]model.CallerStats, error) {
	rows, err := s.db.Query(`
		SELECT DISTINCT caller FROM audit_logs ORDER BY caller
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var callers []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		callers = append(callers, c)
	}

	var stats []model.CallerStats
	for _, c := range callers {
		s, err := s.GetCallerStats(c, now)
		if err != nil {
			return nil, err
		}
		stats = append(stats, *s)
	}
	return stats, nil
}

func (s *Storage) GetGlobalAuditStats(now time.Time) (*model.GlobalAuditStats, error) {
	var total, success, failure int64
	row := s.db.QueryRow(`
		SELECT COUNT(*), SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END)
		FROM audit_logs
	`)
	if err := row.Scan(&total, &success, &failure); err != nil {
		return nil, err
	}

	var req1Min, req5Min, req15Min int64
	s.db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE timestamp >= ?`, now.Add(-1*time.Minute)).Scan(&req1Min)
	s.db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE timestamp >= ?`, now.Add(-5*time.Minute)).Scan(&req5Min)
	s.db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE timestamp >= ?`, now.Add(-15*time.Minute)).Scan(&req15Min)

	var activeBreakers int
	s.db.QueryRow(`SELECT COUNT(*) FROM cb_status WHERE state = 'open'`).Scan(&activeBreakers)

	stats := &model.GlobalAuditStats{
		TotalRequests:  total,
		SuccessCount:   success,
		FailureCount:   failure,
		Requests1Min:   req1Min,
		Requests5Min:   req5Min,
		Requests15Min:  req15Min,
		ActiveBreakers: activeBreakers,
	}
	if total > 0 {
		stats.SuccessRate = float64(success) / float64(total)
		stats.FailureRate = float64(failure) / float64(total)
	}
	return stats, nil
}

func (s *Storage) CreateTopoNode(node *model.TopologyNode) error {
	result, err := s.db.Exec(`
		INSERT INTO topo_nodes (name, lock_name, rate_policy, token_cost, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, node.Name, node.LockName, node.RatePolicy, node.TokenCost, node.CreatedAt, node.UpdatedAt)
	if err != nil {
		return err
	}
	node.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetTopoNode(name string) (*model.TopologyNode, error) {
	row := s.db.QueryRow(`
		SELECT id, name, lock_name, rate_policy, token_cost, created_at, updated_at
		FROM topo_nodes WHERE name = ?
	`, name)

	var node model.TopologyNode
	err := row.Scan(&node.ID, &node.Name, &node.LockName, &node.RatePolicy, &node.TokenCost, &node.CreatedAt, &node.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &node, nil
}

func (s *Storage) ListTopoNodes() ([]model.TopologyNode, error) {
	rows, err := s.db.Query(`
		SELECT id, name, lock_name, rate_policy, token_cost, created_at, updated_at
		FROM topo_nodes ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []model.TopologyNode
	for rows.Next() {
		var node model.TopologyNode
		if err := rows.Scan(&node.ID, &node.Name, &node.LockName, &node.RatePolicy, &node.TokenCost, &node.CreatedAt, &node.UpdatedAt); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (s *Storage) UpdateTopoNode(node *model.TopologyNode) error {
	_, err := s.db.Exec(`
		UPDATE topo_nodes SET
			lock_name = ?,
			rate_policy = ?,
			token_cost = ?,
			updated_at = ?
		WHERE name = ?
	`, node.LockName, node.RatePolicy, node.TokenCost, node.UpdatedAt, node.Name)
	return err
}

func (s *Storage) DeleteTopoNode(name string) error {
	_, err := s.db.Exec(`DELETE FROM topo_nodes WHERE name = ?`, name)
	return err
}

func (s *Storage) CreateTopoEdge(edge *model.TopologyEdge) error {
	result, err := s.db.Exec(`
		INSERT INTO topo_edges (from_node, to_node, created_at)
		VALUES (?, ?, ?)
	`, edge.FromNode, edge.ToNode, edge.CreatedAt)
	if err != nil {
		return err
	}
	edge.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTopoEdges() ([]model.TopologyEdge, error) {
	rows, err := s.db.Query(`
		SELECT id, from_node, to_node, created_at
		FROM topo_edges ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var edges []model.TopologyEdge
	for rows.Next() {
		var edge model.TopologyEdge
		if err := rows.Scan(&edge.ID, &edge.FromNode, &edge.ToNode, &edge.CreatedAt); err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func (s *Storage) DeleteTopoEdge(fromNode, toNode string) error {
	_, err := s.db.Exec(`
		DELETE FROM topo_edges WHERE from_node = ? AND to_node = ?
	`, fromNode, toNode)
	return err
}

func (s *Storage) AddTopoOpHistory(h *model.TopologyOperationHistory) error {
	successInt := 0
	if h.Success {
		successInt = 1
	}
	rolledBackInt := 0
	if h.RolledBack {
		rolledBackInt = 1
	}
	nodesStr := ""
	for i, n := range h.NodesTouched {
		if i > 0 {
			nodesStr += ","
		}
		nodesStr += n
	}
	result, err := s.db.Exec(`
		INSERT INTO topo_op_history (operation, target_node, holder, success, rolled_back, nodes_touched, duration_ms, message, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, string(h.Operation), h.TargetNode, h.Holder, successInt, rolledBackInt, nodesStr, h.DurationMs, h.Message, h.CreatedAt)
	if err != nil {
		return err
	}
	h.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTopoOpHistory(holder string, limit int) ([]model.TopologyOperationHistory, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows *sql.Rows
	var err error
	if holder != "" {
		rows, err = s.db.Query(`
			SELECT id, operation, target_node, holder, success, rolled_back, nodes_touched, duration_ms, message, created_at
			FROM topo_op_history WHERE holder = ? ORDER BY id DESC LIMIT ?
		`, holder, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT id, operation, target_node, holder, success, rolled_back, nodes_touched, duration_ms, message, created_at
			FROM topo_op_history ORDER BY id DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []model.TopologyOperationHistory
	for rows.Next() {
		var h model.TopologyOperationHistory
		var successInt, rolledBackInt int
		var nodesStr string
		if err := rows.Scan(&h.ID, &h.Operation, &h.TargetNode, &h.Holder, &successInt, &rolledBackInt, &nodesStr, &h.DurationMs, &h.Message, &h.CreatedAt); err != nil {
			return nil, err
		}
		h.Success = successInt != 0
		h.RolledBack = rolledBackInt != 0
		if nodesStr != "" {
			h.NodesTouched = splitAndTrim(nodesStr, ",")
		}
		history = append(history, h)
	}
	return history, nil
}

func (s *Storage) CountTopoOps() (int, int, int, error) {
	var total, acquire, release int
	row := s.db.QueryRow(`
		SELECT 
			COUNT(*),
			SUM(CASE WHEN operation = ? THEN 1 ELSE 0 END),
			SUM(CASE WHEN operation = ? THEN 1 ELSE 0 END)
		FROM topo_op_history
	`, string(model.TopologyOpAcquire), string(model.TopologyOpRelease))
	err := row.Scan(&total, &acquire, &release)
	return total, acquire, release, err
}

func splitAndTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	parts := make([]string, 0)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep[0] {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func (s *Storage) CreateShadowPlan(plan *model.ShadowPlan) error {
	result, err := s.db.Exec(`
		INSERT INTO shadow_plans (name, description, status, mode, audit_log_start_id, audit_log_end_id, mirror_until, applied_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, plan.Name, plan.Description, plan.Status, plan.Mode, plan.AuditLogStartID, plan.AuditLogEndID,
		nullTime(plan.MirrorUntil), nullTime(plan.AppliedAt), plan.CreatedAt, plan.UpdatedAt)
	if err != nil {
		return err
	}
	plan.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetShadowPlan(id int64) (*model.ShadowPlan, error) {
	row := s.db.QueryRow(`
		SELECT id, name, description, status, mode, audit_log_start_id, audit_log_end_id, mirror_until, applied_at, created_at, updated_at
		FROM shadow_plans WHERE id = ?
	`, id)
	var p model.ShadowPlan
	var mirrorUntil, appliedAt sql.NullTime
	err := row.Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.Mode,
		&p.AuditLogStartID, &p.AuditLogEndID, &mirrorUntil, &appliedAt,
		&p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if mirrorUntil.Valid {
		p.MirrorUntil = mirrorUntil.Time
	}
	if appliedAt.Valid {
		p.AppliedAt = appliedAt.Time
	}
	return &p, nil
}

func (s *Storage) GetShadowPlanByName(name string) (*model.ShadowPlan, error) {
	row := s.db.QueryRow(`
		SELECT id, name, description, status, mode, audit_log_start_id, audit_log_end_id, mirror_until, applied_at, created_at, updated_at
		FROM shadow_plans WHERE name = ?
	`, name)
	var p model.ShadowPlan
	var mirrorUntil, appliedAt sql.NullTime
	err := row.Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.Mode,
		&p.AuditLogStartID, &p.AuditLogEndID, &mirrorUntil, &appliedAt,
		&p.CreatedAt, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if mirrorUntil.Valid {
		p.MirrorUntil = mirrorUntil.Time
	}
	if appliedAt.Valid {
		p.AppliedAt = appliedAt.Time
	}
	return &p, nil
}

func (s *Storage) ListShadowPlans() ([]model.ShadowPlan, error) {
	rows, err := s.db.Query(`
		SELECT id, name, description, status, mode, audit_log_start_id, audit_log_end_id, mirror_until, applied_at, created_at, updated_at
		FROM shadow_plans ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var plans []model.ShadowPlan
	for rows.Next() {
		var p model.ShadowPlan
		var mirrorUntil, appliedAt sql.NullTime
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Status, &p.Mode,
			&p.AuditLogStartID, &p.AuditLogEndID, &mirrorUntil, &appliedAt,
			&p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		if mirrorUntil.Valid {
			p.MirrorUntil = mirrorUntil.Time
		}
		if appliedAt.Valid {
			p.AppliedAt = appliedAt.Time
		}
		plans = append(plans, p)
	}
	return plans, nil
}

func (s *Storage) UpdateShadowPlanStatus(id int64, status model.ShadowPlanStatus, updatedAt time.Time) error {
	_, err := s.db.Exec(`UPDATE shadow_plans SET status = ?, updated_at = ? WHERE id = ?`, status, updatedAt, id)
	return err
}

func (s *Storage) UpdateShadowPlanMirror(id int64, mirrorUntil time.Time, updatedAt time.Time) error {
	_, err := s.db.Exec(`UPDATE shadow_plans SET mirror_until = ?, updated_at = ? WHERE id = ?`, mirrorUntil, updatedAt, id)
	return err
}

func (s *Storage) UpdateShadowPlanApplied(id int64, appliedAt time.Time, updatedAt time.Time) error {
	_, err := s.db.Exec(`UPDATE shadow_plans SET status = ?, applied_at = ?, updated_at = ? WHERE id = ?`,
		model.ShadowPlanStatusApplied, appliedAt, updatedAt, id)
	return err
}

func (s *Storage) CreateShadowConfigOverride(ov *model.ShadowConfigOverride) error {
	result, err := s.db.Exec(`
		INSERT INTO shadow_config_overrides (plan_id, category, target_key, field, orig_value, new_value, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, ov.PlanID, ov.Category, ov.TargetKey, ov.Field, ov.OrigValue, ov.NewValue, ov.CreatedAt)
	if err != nil {
		return err
	}
	ov.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListShadowConfigOverrides(planID int64) ([]model.ShadowConfigOverride, error) {
	rows, err := s.db.Query(`
		SELECT id, plan_id, category, target_key, field, orig_value, new_value, created_at
		FROM shadow_config_overrides WHERE plan_id = ? ORDER BY id
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var overrides []model.ShadowConfigOverride
	for rows.Next() {
		var ov model.ShadowConfigOverride
		if err := rows.Scan(&ov.ID, &ov.PlanID, &ov.Category, &ov.TargetKey, &ov.Field, &ov.OrigValue, &ov.NewValue, &ov.CreatedAt); err != nil {
			return nil, err
		}
		overrides = append(overrides, ov)
	}
	return overrides, nil
}

func (s *Storage) DeleteShadowConfigOverride(id int64) error {
	_, err := s.db.Exec(`DELETE FROM shadow_config_overrides WHERE id = ?`, id)
	return err
}

func (s *Storage) CreateShadowDiffRecord(r *model.ShadowDiffRecord) error {
	result, err := s.db.Exec(`
		INSERT INTO shadow_diff_records (plan_id, audit_log_id, request_caller, request_op, request_resource, live_decision, shadow_decision, rule_category, detail, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.PlanID, r.AuditLogID, r.RequestCaller, r.RequestOp, r.RequestResource,
		r.LiveDecision, r.ShadowDecision, r.RuleCategory, r.Detail, r.CreatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListShadowDiffRecords(planID int64, limit int) ([]model.ShadowDiffRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(`
		SELECT id, plan_id, audit_log_id, request_caller, request_op, request_resource, live_decision, shadow_decision, rule_category, detail, created_at
		FROM shadow_diff_records WHERE plan_id = ? ORDER BY id LIMIT ?
	`, planID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []model.ShadowDiffRecord
	for rows.Next() {
		var r model.ShadowDiffRecord
		if err := rows.Scan(&r.ID, &r.PlanID, &r.AuditLogID, &r.RequestCaller, &r.RequestOp, &r.RequestResource,
			&r.LiveDecision, &r.ShadowDecision, &r.RuleCategory, &r.Detail, &r.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) CountShadowDiffs(planID int64) (int64, error) {
	var count int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM shadow_diff_records WHERE plan_id = ?`, planID).Scan(&count)
	return count, err
}

func (s *Storage) GetShadowDiffStats(planID int64) (*model.ShadowDiffStats, error) {
	stats := &model.ShadowDiffStats{
		PlanID:         planID,
		ByCategory:     make(map[model.ShadowRuleCategory]int64),
		ByDecisionPair: make(map[string]int64),
	}
	var total int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM shadow_diff_records WHERE plan_id = ?`, planID).Scan(&total)
	if err != nil {
		return nil, err
	}
	stats.TotalDiffs = total

	rows, err := s.db.Query(`
		SELECT rule_category, COUNT(*) FROM shadow_diff_records WHERE plan_id = ? GROUP BY rule_category
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var cat string
		var cnt int64
		if err := rows.Scan(&cat, &cnt); err != nil {
			return nil, err
		}
		stats.ByCategory[model.ShadowRuleCategory(cat)] = cnt
	}

	rows2, err := s.db.Query(`
		SELECT live_decision || '|' || shadow_decision AS pair, COUNT(*) FROM shadow_diff_records WHERE plan_id = ? GROUP BY pair
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var pair string
		var cnt int64
		if err := rows2.Scan(&pair, &cnt); err != nil {
			return nil, err
		}
		stats.ByDecisionPair[pair] = cnt
	}

	rows3, err := s.db.Query(`
		SELECT request_caller, COUNT(*) AS cnt FROM shadow_diff_records WHERE plan_id = ? GROUP BY request_caller ORDER BY cnt DESC LIMIT 5
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows3.Close()
	for rows3.Next() {
		var ci model.ShadowCallerImpact
		if err := rows3.Scan(&ci.CallerID, &ci.DiffCount); err != nil {
			return nil, err
		}
		stats.TopCallers = append(stats.TopCallers, ci)
	}

	rows4, err := s.db.Query(`
		SELECT request_resource, COUNT(*) AS cnt FROM shadow_diff_records WHERE plan_id = ? GROUP BY request_resource ORDER BY cnt DESC LIMIT 5
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows4.Close()
	for rows4.Next() {
		var ri model.ShadowResourceImpact
		if err := rows4.Scan(&ri.Resource, &ri.DiffCount); err != nil {
			return nil, err
		}
		stats.TopResources = append(stats.TopResources, ri)
	}

	rows5, err := s.db.Query(`
		SELECT detail, rule_category, COUNT(*) AS cnt FROM shadow_diff_records WHERE plan_id = ? AND detail != '' GROUP BY detail, rule_category ORDER BY cnt DESC LIMIT 5
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows5.Close()
	for rows5.Next() {
		var cr model.ShadowConflictReason
		if err := rows5.Scan(&cr.Reason, &cr.Category, &cr.Count); err != nil {
			return nil, err
		}
		stats.TopConflictReasons = append(stats.TopConflictReasons, cr)
	}

	return stats, nil
}

func (s *Storage) ListAuditLogsInRange(startID, endID int64) ([]model.AuditLog, error) {
	rows, err := s.db.Query(`
		SELECT id, timestamp, caller, operation, resource, success, fail_reason
		FROM audit_logs WHERE id >= ? AND id <= ? ORDER BY id ASC
	`, startID, endID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var logs []model.AuditLog
	for rows.Next() {
		var l model.AuditLog
		var successInt int
		if err := rows.Scan(&l.ID, &l.Timestamp, &l.Caller, &l.Operation, &l.Resource, &successInt, &l.FailReason); err != nil {
			return nil, err
		}
		l.Success = successInt != 0
		logs = append(logs, l)
	}
	return logs, nil
}

func (s *Storage) GetAuditLogRange() (int64, int64, error) {
	var minID, maxID int64
	err := s.db.QueryRow(`SELECT COALESCE(MIN(id),0), COALESCE(MAX(id),0) FROM audit_logs`).Scan(&minID, &maxID)
	return minID, maxID, err
}

func (s *Storage) CreateDebtRecord(r *model.DebtRecord) error {
	result, err := s.db.Exec(`
		INSERT INTO debt_records (debtor, creditor, amount, resource_type, resource_key, status, due_at, source_event_id, collect_attempts, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.Debtor, r.Creditor, r.Amount, r.ResourceType, r.ResourceKey, r.Status, r.DueAt, r.SourceEventID, r.CollectAttempts, r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) UpdateDebtRecord(r *model.DebtRecord) error {
	_, err := s.db.Exec(`
		UPDATE debt_records SET status = ?, collected_at = ?, overdue_at = ?, write_off_at = ?,
			collect_attempts = ?, last_collect_at = ?, updated_at = ?
		WHERE id = ?
	`, r.Status, nullTime(r.CollectedAt), nullTime(r.OverdueAt), nullTime(r.WriteOffAt),
		r.CollectAttempts, nullTime(r.LastCollectAt), r.UpdatedAt, r.ID)
	return err
}

func (s *Storage) GetDebtRecord(id int64) (*model.DebtRecord, error) {
	row := s.db.QueryRow(`
		SELECT id, debtor, creditor, amount, resource_type, resource_key, status, due_at,
			collected_at, overdue_at, write_off_at, source_event_id, collect_attempts, last_collect_at, created_at, updated_at
		FROM debt_records WHERE id = ?
	`, id)
	var r model.DebtRecord
	var collectedAt, overdueAt, writeOffAt, lastCollectAt sql.NullTime
	err := row.Scan(&r.ID, &r.Debtor, &r.Creditor, &r.Amount, &r.ResourceType, &r.ResourceKey,
		&r.Status, &r.DueAt, &collectedAt, &overdueAt, &writeOffAt,
		&r.SourceEventID, &r.CollectAttempts, &lastCollectAt, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if collectedAt.Valid {
		r.CollectedAt = collectedAt.Time
	}
	if overdueAt.Valid {
		r.OverdueAt = overdueAt.Time
	}
	if writeOffAt.Valid {
		r.WriteOffAt = writeOffAt.Time
	}
	if lastCollectAt.Valid {
		r.LastCollectAt = lastCollectAt.Time
	}
	return &r, nil
}

func (s *Storage) ListDebtRecords(debtor, status string) ([]model.DebtRecord, error) {
	var rows *sql.Rows
	var err error
	if debtor != "" && status != "" {
		rows, err = s.db.Query(`
			SELECT id, debtor, creditor, amount, resource_type, resource_key, status, due_at,
				collected_at, overdue_at, write_off_at, source_event_id, collect_attempts, last_collect_at, created_at, updated_at
			FROM debt_records WHERE debtor = ? AND status = ? ORDER BY id DESC
		`, debtor, status)
	} else if debtor != "" {
		rows, err = s.db.Query(`
			SELECT id, debtor, creditor, amount, resource_type, resource_key, status, due_at,
				collected_at, overdue_at, write_off_at, source_event_id, collect_attempts, last_collect_at, created_at, updated_at
			FROM debt_records WHERE debtor = ? ORDER BY id DESC
		`, debtor)
	} else if status != "" {
		rows, err = s.db.Query(`
			SELECT id, debtor, creditor, amount, resource_type, resource_key, status, due_at,
				collected_at, overdue_at, write_off_at, source_event_id, collect_attempts, last_collect_at, created_at, updated_at
			FROM debt_records WHERE status = ? ORDER BY id DESC
		`, status)
	} else {
		rows, err = s.db.Query(`
			SELECT id, debtor, creditor, amount, resource_type, resource_key, status, due_at,
				collected_at, overdue_at, write_off_at, source_event_id, collect_attempts, last_collect_at, created_at, updated_at
			FROM debt_records ORDER BY id DESC
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []model.DebtRecord
	for rows.Next() {
		var r model.DebtRecord
		var collectedAt, overdueAt, writeOffAt, lastCollectAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.Debtor, &r.Creditor, &r.Amount, &r.ResourceType, &r.ResourceKey,
			&r.Status, &r.DueAt, &collectedAt, &overdueAt, &writeOffAt,
			&r.SourceEventID, &r.CollectAttempts, &lastCollectAt, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		if collectedAt.Valid {
			r.CollectedAt = collectedAt.Time
		}
		if overdueAt.Valid {
			r.OverdueAt = overdueAt.Time
		}
		if writeOffAt.Valid {
			r.WriteOffAt = writeOffAt.Time
		}
		if lastCollectAt.Valid {
			r.LastCollectAt = lastCollectAt.Time
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) ListOverdueDebts(now time.Time) ([]model.DebtRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, debtor, creditor, amount, resource_type, resource_key, status, due_at,
			collected_at, overdue_at, write_off_at, source_event_id, collect_attempts, last_collect_at, created_at, updated_at
		FROM debt_records WHERE status IN ('active', 'overdue') AND due_at <= ? ORDER BY due_at
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []model.DebtRecord
	for rows.Next() {
		var r model.DebtRecord
		var collectedAt, overdueAt, writeOffAt, lastCollectAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.Debtor, &r.Creditor, &r.Amount, &r.ResourceType, &r.ResourceKey,
			&r.Status, &r.DueAt, &collectedAt, &overdueAt, &writeOffAt,
			&r.SourceEventID, &r.CollectAttempts, &lastCollectAt, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		if collectedAt.Valid {
			r.CollectedAt = collectedAt.Time
		}
		if overdueAt.Valid {
			r.OverdueAt = overdueAt.Time
		}
		if writeOffAt.Valid {
			r.WriteOffAt = writeOffAt.Time
		}
		if lastCollectAt.Valid {
			r.LastCollectAt = lastCollectAt.Time
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) CountOverdueDebts(debtor string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM debt_records WHERE debtor = ? AND status = 'overdue'`, debtor).Scan(&count)
	return count, err
}

func (s *Storage) SumActiveDebt(debtor string) (int, error) {
	var total int
	err := s.db.QueryRow(`SELECT COALESCE(SUM(amount),0) FROM debt_records WHERE debtor = ? AND status IN ('active','overdue')`, debtor).Scan(&total)
	return total, err
}

func (s *Storage) SumActiveCredit(creditor string) (int, error) {
	var total int
	err := s.db.QueryRow(`SELECT COALESCE(SUM(amount),0) FROM debt_records WHERE creditor = ? AND status IN ('active','overdue')`, creditor).Scan(&total)
	return total, err
}

func (s *Storage) CreateDebtLedgerEvent(e *model.DebtLedgerEvent) error {
	result, err := s.db.Exec(`
		INSERT INTO debt_ledger_events (debt_id, debtor, creditor, event_type, amount, resource_type, resource_key, detail, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.DebtID, e.Debtor, e.Creditor, e.EventType, e.Amount, e.ResourceType, e.ResourceKey, e.Detail, e.CreatedAt)
	if err != nil {
		return err
	}
	e.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListDebtLedgerEvents(debtor string, debtID int64, limit int) ([]model.DebtLedgerEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows *sql.Rows
	var err error
	if debtID > 0 {
		rows, err = s.db.Query(`
			SELECT id, debt_id, debtor, creditor, event_type, amount, resource_type, resource_key, detail, created_at
			FROM debt_ledger_events WHERE debt_id = ? ORDER BY id DESC LIMIT ?
		`, debtID, limit)
	} else if debtor != "" {
		rows, err = s.db.Query(`
			SELECT id, debt_id, debtor, creditor, event_type, amount, resource_type, resource_key, detail, created_at
			FROM debt_ledger_events WHERE debtor = ? ORDER BY id DESC LIMIT ?
		`, debtor, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT id, debt_id, debtor, creditor, event_type, amount, resource_type, resource_key, detail, created_at
			FROM debt_ledger_events ORDER BY id DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []model.DebtLedgerEvent
	for rows.Next() {
		var e model.DebtLedgerEvent
		if err := rows.Scan(&e.ID, &e.DebtID, &e.Debtor, &e.Creditor, &e.EventType, &e.Amount, &e.ResourceType, &e.ResourceKey, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

func (s *Storage) CreateDebtRestriction(r *model.DebtRestriction) error {
	result, err := s.db.Exec(`
		INSERT INTO debt_restrictions (caller_id, restriction_type, scope, overdue_threshold, reason, active, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, r.CallerID, r.RestrictionType, r.Scope, r.OverdueThreshold, r.Reason, boolToInt(r.Active), r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListActiveDebtRestrictions(callerID string) ([]model.DebtRestriction, error) {
	var rows *sql.Rows
	var err error
	if callerID != "" {
		rows, err = s.db.Query(`
			SELECT id, caller_id, restriction_type, scope, overdue_threshold, reason, active, lifted_at, created_at, updated_at
			FROM debt_restrictions WHERE caller_id = ? AND active = 1 ORDER BY id DESC
		`, callerID)
	} else {
		rows, err = s.db.Query(`
			SELECT id, caller_id, restriction_type, scope, overdue_threshold, reason, active, lifted_at, created_at, updated_at
			FROM debt_restrictions WHERE active = 1 ORDER BY id DESC
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var restrictions []model.DebtRestriction
	for rows.Next() {
		var r model.DebtRestriction
		var activeInt int
		var liftedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.CallerID, &r.RestrictionType, &r.Scope, &r.OverdueThreshold, &r.Reason, &activeInt, &liftedAt, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.Active = activeInt != 0
		if liftedAt.Valid {
			r.LiftedAt = liftedAt.Time
		}
		restrictions = append(restrictions, r)
	}
	return restrictions, nil
}

func (s *Storage) LiftDebtRestriction(id int64, liftedAt time.Time) error {
	_, err := s.db.Exec(`UPDATE debt_restrictions SET active = 0, lifted_at = ?, updated_at = ? WHERE id = ?`, liftedAt, liftedAt, id)
	return err
}

func (s *Storage) CreateLiquidationRule(r *model.LiquidationRule) error {
	result, err := s.db.Exec(`
		INSERT INTO liquidation_rules (caller_id, grace_period_sec, overdue_threshold, restriction_type, restriction_scope, max_collect_retries, protection_after, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			grace_period_sec = excluded.grace_period_sec,
			overdue_threshold = excluded.overdue_threshold,
			restriction_type = excluded.restriction_type,
			restriction_scope = excluded.restriction_scope,
			max_collect_retries = excluded.max_collect_retries,
			protection_after = excluded.protection_after,
			updated_at = excluded.updated_at
	`, r.CallerID, r.GracePeriodSec, r.OverdueThreshold, r.RestrictionType, r.RestrictionScope, r.MaxCollectRetries, r.ProtectionAfter, r.CreatedAt, r.UpdatedAt)
	if err != nil {
		return err
	}
	if r.ID == 0 {
		r.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetLiquidationRule(callerID string) (*model.LiquidationRule, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, grace_period_sec, overdue_threshold, restriction_type, restriction_scope, max_collect_retries, protection_after, created_at, updated_at
		FROM liquidation_rules WHERE caller_id = ?
	`, callerID)
	var r model.LiquidationRule
	err := row.Scan(&r.ID, &r.CallerID, &r.GracePeriodSec, &r.OverdueThreshold, &r.RestrictionType, &r.RestrictionScope, &r.MaxCollectRetries, &r.ProtectionAfter, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Storage) ListLiquidationRules() ([]model.LiquidationRule, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, grace_period_sec, overdue_threshold, restriction_type, restriction_scope, max_collect_retries, protection_after, created_at, updated_at
		FROM liquidation_rules ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rules []model.LiquidationRule
	for rows.Next() {
		var r model.LiquidationRule
		if err := rows.Scan(&r.ID, &r.CallerID, &r.GracePeriodSec, &r.OverdueThreshold, &r.RestrictionType, &r.RestrictionScope, &r.MaxCollectRetries, &r.ProtectionAfter, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		rules = append(rules, r)
	}
	return rules, nil
}

func (s *Storage) DeleteLiquidationRule(callerID string) error {
	_, err := s.db.Exec(`DELETE FROM liquidation_rules WHERE caller_id = ?`, callerID)
	return err
}

func (s *Storage) AddLiquidationAudit(a *model.LiquidationAuditEntry) error {
	result, err := s.db.Exec(`
		INSERT INTO liquidation_audit (debt_id, debtor, creditor, action, amount, success, detail, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, a.DebtID, a.Debtor, a.Creditor, a.Action, a.Amount, boolToInt(a.Success), a.Detail, a.CreatedAt)
	if err != nil {
		return err
	}
	a.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListLiquidationAudit(debtor string, limit int) ([]model.LiquidationAuditEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows *sql.Rows
	var err error
	if debtor != "" {
		rows, err = s.db.Query(`
			SELECT id, debt_id, debtor, creditor, action, amount, success, detail, created_at
			FROM liquidation_audit WHERE debtor = ? ORDER BY id DESC LIMIT ?
		`, debtor, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT id, debt_id, debtor, creditor, action, amount, success, detail, created_at
			FROM liquidation_audit ORDER BY id DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []model.LiquidationAuditEntry
	for rows.Next() {
		var e model.LiquidationAuditEntry
		var successInt int
		if err := rows.Scan(&e.ID, &e.DebtID, &e.Debtor, &e.Creditor, &e.Action, &e.Amount, &successInt, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Success = successInt != 0
		entries = append(entries, e)
	}
	return entries, nil
}

func (s *Storage) GetLastCollectResult(debtor string) (string, error) {
	var detail string
	err := s.db.QueryRow(`
		SELECT detail FROM liquidation_audit WHERE debtor = ? AND action = 'collect' ORDER BY id DESC LIMIT 1
	`, debtor).Scan(&detail)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return detail, err
}

func (s *Storage) GetAffectedResources(debtor string) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT DISTINCT resource_key FROM debt_records WHERE debtor = ? AND status IN ('active','overdue') AND resource_key != ''
	`, debtor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var resources []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		resources = append(resources, r)
	}
	return resources, nil
}

func (s *Storage) CountDebtsByStatus(debtor string, status string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM debt_records WHERE debtor = ? AND status = ?`, debtor, status).Scan(&count)
	return count, err
}

func (s *Storage) CountCollectFailures(debtor string, windowSec int, now time.Time) (int, error) {
	startTime := now.Add(-time.Duration(windowSec) * time.Second)
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM liquidation_audit WHERE debtor = ? AND action = 'collect' AND success = 0 AND created_at >= ?
	`, debtor, startTime).Scan(&count)
	return count, err
}

func (s *Storage) CreateHandover(h *model.Handover) error {
	result, err := s.db.Exec(`
		INSERT INTO handovers (from_caller, to_caller, status, initiator, description,
			need_confirm, confirm_timeout_sec, confirm_deadline, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, h.FromCaller, h.ToCaller, h.Status, h.Initiator, h.Description,
		boolToInt(h.NeedConfirm), h.ConfirmTimeoutSec, nullTimePtr(h.ConfirmDeadline), h.CreatedAt, h.UpdatedAt)
	if err != nil {
		return err
	}
	h.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) UpdateHandoverStatus(id int64, status model.HandoverStatus, updatedAt time.Time, extraFields ...interface{}) error {
	query := `UPDATE handovers SET status = ?, updated_at = ?`
	args := []interface{}{status, updatedAt}

	for i := 0; i < len(extraFields); i += 2 {
		fieldName := extraFields[i].(string)
		fieldValue := extraFields[i+1]
		query += fmt.Sprintf(`, %s = ?`, fieldName)
		args = append(args, fieldValue)
	}
	query += ` WHERE id = ?`
	args = append(args, id)

	_, err := s.db.Exec(query, args...)
	return err
}

func (s *Storage) GetHandover(id int64) (*model.Handover, error) {
	row := s.db.QueryRow(`
		SELECT id, from_caller, to_caller, status, initiator, description,
			need_confirm, confirm_timeout_sec, confirm_deadline, confirmed_at,
			cancelled_at, completed_at, cancel_reason, created_at, updated_at
		FROM handovers WHERE id = ?
	`, id)

	var h model.Handover
	var needConfirmInt int
	var confirmDeadline, confirmedAt, cancelledAt, completedAt sql.NullTime
	err := row.Scan(&h.ID, &h.FromCaller, &h.ToCaller, &h.Status, &h.Initiator, &h.Description,
		&needConfirmInt, &h.ConfirmTimeoutSec, &confirmDeadline, &confirmedAt,
		&cancelledAt, &completedAt, &h.CancelReason, &h.CreatedAt, &h.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	h.NeedConfirm = needConfirmInt != 0
	if confirmDeadline.Valid {
		t := confirmDeadline.Time
		h.ConfirmDeadline = &t
	}
	if confirmedAt.Valid {
		t := confirmedAt.Time
		h.ConfirmedAt = &t
	}
	if cancelledAt.Valid {
		t := cancelledAt.Time
		h.CancelledAt = &t
	}
	if completedAt.Valid {
		t := completedAt.Time
		h.CompletedAt = &t
	}
	return &h, nil
}

func (s *Storage) ListHandovers(fromCaller, toCaller, status string) ([]model.Handover, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	if fromCaller != "" {
		where = append(where, "from_caller = ?")
		args = append(args, fromCaller)
	}
	if toCaller != "" {
		where = append(where, "to_caller = ?")
		args = append(args, toCaller)
	}
	if status != "" {
		where = append(where, "status = ?")
		args = append(args, status)
	}
	whereClause := strings.Join(where, " AND ")
	query := `SELECT id, from_caller, to_caller, status, initiator, description,
		need_confirm, confirm_timeout_sec, confirm_deadline, confirmed_at,
		cancelled_at, completed_at, cancel_reason, created_at, updated_at
		FROM handovers WHERE ` + whereClause + ` ORDER BY created_at DESC`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.Handover
	for rows.Next() {
		var h model.Handover
		var needConfirmInt int
		var confirmDeadline, confirmedAt, cancelledAt, completedAt sql.NullTime
		err := rows.Scan(&h.ID, &h.FromCaller, &h.ToCaller, &h.Status, &h.Initiator, &h.Description,
			&needConfirmInt, &h.ConfirmTimeoutSec, &confirmDeadline, &confirmedAt,
			&cancelledAt, &completedAt, &h.CancelReason, &h.CreatedAt, &h.UpdatedAt)
		if err != nil {
			return nil, err
		}
		h.NeedConfirm = needConfirmInt != 0
		if confirmDeadline.Valid {
			t := confirmDeadline.Time
			h.ConfirmDeadline = &t
		}
		if confirmedAt.Valid {
			t := confirmedAt.Time
			h.ConfirmedAt = &t
		}
		if cancelledAt.Valid {
			t := cancelledAt.Time
			h.CancelledAt = &t
		}
		if completedAt.Valid {
			t := completedAt.Time
			h.CompletedAt = &t
		}
		result = append(result, h)
	}
	return result, nil
}

func (s *Storage) ListExpiredPendingHandovers(now time.Time) ([]model.Handover, error) {
	rows, err := s.db.Query(`
		SELECT id, from_caller, to_caller, status, initiator, description,
			need_confirm, confirm_timeout_sec, confirm_deadline, confirmed_at,
			cancelled_at, completed_at, cancel_reason, created_at, updated_at
		FROM handovers WHERE status = ? AND confirm_deadline IS NOT NULL AND confirm_deadline <= ?
	`, model.HandoverStatusPending, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.Handover
	for rows.Next() {
		var h model.Handover
		var needConfirmInt int
		var confirmDeadline, confirmedAt, cancelledAt, completedAt sql.NullTime
		err := rows.Scan(&h.ID, &h.FromCaller, &h.ToCaller, &h.Status, &h.Initiator, &h.Description,
			&needConfirmInt, &h.ConfirmTimeoutSec, &confirmDeadline, &confirmedAt,
			&cancelledAt, &completedAt, &h.CancelReason, &h.CreatedAt, &h.UpdatedAt)
		if err != nil {
			return nil, err
		}
		h.NeedConfirm = needConfirmInt != 0
		if confirmDeadline.Valid {
			t := confirmDeadline.Time
			h.ConfirmDeadline = &t
		}
		if confirmedAt.Valid {
			t := confirmedAt.Time
			h.ConfirmedAt = &t
		}
		if cancelledAt.Valid {
			t := cancelledAt.Time
			h.CancelledAt = &t
		}
		if completedAt.Valid {
			t := completedAt.Time
			h.CompletedAt = &t
		}
		result = append(result, h)
	}
	return result, nil
}

func (s *Storage) AddHandoverResource(item *model.HandoverResourceItem) error {
	executedInt := 0
	if item.Executed {
		executedInt = 1
	}
	rolledBackInt := 0
	if item.RolledBack {
		rolledBackInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO handover_resources (handover_id, resource_type, resource_key, resource_name,
			precheck_status, precheck_detail, current_holder, snapshot, executed, rolled_back, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, item.HandoverID, item.ResourceType, item.ResourceKey, item.ResourceName,
		item.PreCheckStatus, item.PreCheckDetail, item.CurrentHolder, item.Snapshot,
		executedInt, rolledBackInt, item.CreatedAt, item.UpdatedAt)
	if err != nil {
		return err
	}
	item.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) UpdateHandoverResource(item *model.HandoverResourceItem) error {
	executedInt := 0
	if item.Executed {
		executedInt = 1
	}
	rolledBackInt := 0
	if item.RolledBack {
		rolledBackInt = 1
	}
	_, err := s.db.Exec(`
		UPDATE handover_resources SET resource_name=?, precheck_status=?, precheck_detail=?,
			current_holder=?, snapshot=?, executed=?, rolled_back=?, updated_at=?
		WHERE id = ?
	`, item.ResourceName, item.PreCheckStatus, item.PreCheckDetail,
		item.CurrentHolder, item.Snapshot, executedInt, rolledBackInt, item.UpdatedAt, item.ID)
	return err
}

func (s *Storage) ListHandoverResources(handoverID int64) ([]model.HandoverResourceItem, error) {
	rows, err := s.db.Query(`
		SELECT id, handover_id, resource_type, resource_key, resource_name,
			precheck_status, precheck_detail, current_holder, snapshot, executed, rolled_back, created_at, updated_at
		FROM handover_resources WHERE handover_id = ? ORDER BY id
	`, handoverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []model.HandoverResourceItem
	for rows.Next() {
		var it model.HandoverResourceItem
		var execInt, rbInt int
		err := rows.Scan(&it.ID, &it.HandoverID, &it.ResourceType, &it.ResourceKey, &it.ResourceName,
			&it.PreCheckStatus, &it.PreCheckDetail, &it.CurrentHolder, &it.Snapshot, &execInt, &rbInt,
			&it.CreatedAt, &it.UpdatedAt)
		if err != nil {
			return nil, err
		}
		it.Executed = execInt != 0
		it.RolledBack = rbInt != 0
		items = append(items, it)
	}
	return items, nil
}

func (s *Storage) AddHandoverTimeline(entry *model.HandoverTimelineEntry) error {
	result, err := s.db.Exec(`
		INSERT INTO handover_timeline (handover_id, status, operator, detail, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, entry.HandoverID, entry.Status, entry.Operator, entry.Detail, entry.CreatedAt)
	if err != nil {
		return err
	}
	entry.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListHandoverTimeline(handoverID int64) ([]model.HandoverTimelineEntry, error) {
	rows, err := s.db.Query(`
		SELECT id, handover_id, status, operator, detail, created_at
		FROM handover_timeline WHERE handover_id = ? ORDER BY id
	`, handoverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []model.HandoverTimelineEntry
	for rows.Next() {
		var e model.HandoverTimelineEntry
		err := rows.Scan(&e.ID, &e.HandoverID, &e.Status, &e.Operator, &e.Detail, &e.CreatedAt)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (s *Storage) CountHandoversByCallerAndStatus(callerID string, isFrom bool, status model.HandoverStatus) (int, error) {
	var field string
	if isFrom {
		field = "from_caller"
	} else {
		field = "to_caller"
	}
	var count int
	err := s.db.QueryRow(fmt.Sprintf(
		`SELECT COUNT(*) FROM handovers WHERE %s = ? AND status = ?`, field),
		callerID, status).Scan(&count)
	return count, err
}

func (s *Storage) UpsertLeaseForTransfer(l *model.Lease) error {
	activeInt := 0
	if l.Active {
		activeInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO leases (lock_name, holder, lease_sec, acquired_at, expires_at, active, fencing_token)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, l.LockName, l.Holder, l.LeaseSec, l.AcquiredAt, l.ExpiresAt, activeInt, l.FencingToken)
	if err != nil {
		return err
	}
	l.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) TransferLockHolder(lockName string, newHolder string, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE locks SET holder = ?, updated_at = ? WHERE name = ?
	`, newHolder, updatedAt, lockName)
	return err
}

func (s *Storage) TransferLeaseHolder(lockName string, newHolder string, newExpiresAt time.Time, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE leases SET holder = ?, expires_at = ?, active = 1
		WHERE lock_name = ? AND active = 1
	`, newHolder, newExpiresAt, lockName)
	return err
}

func (s *Storage) TransferOrchTxHolder(txID string, newHolder string, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE orch_txs SET holder = ?, updated_at = ? WHERE id = ?
	`, newHolder, updatedAt, txID)
	return err
}

func (s *Storage) TransferOrchTxLockHolder(txID string, newHolder string) error {
	_, err := s.db.Exec(`
		UPDATE orch_tx_locks SET holder = ? WHERE tx_id = ?
	`, newHolder, txID)
	return err
}

func (s *Storage) TransferReservationCaller(reservationID int64, newCallerID string, updatedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE rl_reservations SET caller_id = ?, updated_at = ? WHERE id = ?
	`, newCallerID, updatedAt, reservationID)
	return err
}

func nullTimePtr(t *time.Time) interface{} {
	if t == nil || t.IsZero() {
		return nil
	}
	return *t
}

func (s *Storage) CreateHeartbeatRegistration(h *model.HeartbeatRegistration) error {
	result, err := s.db.Exec(`
		INSERT INTO heartbeat_registrations (
			caller_id, group_name, interval_sec, max_missed, strategy,
			last_heartbeat_at, next_expected_at, missed_count, status,
			frozen_at, lost_at, recovered_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			group_name = excluded.group_name,
			interval_sec = excluded.interval_sec,
			max_missed = excluded.max_missed,
			strategy = excluded.strategy,
			last_heartbeat_at = excluded.last_heartbeat_at,
			next_expected_at = excluded.next_expected_at,
			missed_count = excluded.missed_count,
			status = excluded.status,
			frozen_at = excluded.frozen_at,
			lost_at = excluded.lost_at,
			recovered_at = excluded.recovered_at,
			updated_at = excluded.updated_at
	`, h.CallerID, h.GroupName, h.IntervalSec, h.MaxMissed, h.Strategy,
		h.LastHeartbeatAt, h.NextExpectedAt, h.MissedCount, h.Status,
		nullTimePtr(h.FrozenAt), nullTimePtr(h.LostAt), nullTimePtr(h.RecoveredAt),
		h.CreatedAt, h.UpdatedAt)
	if err != nil {
		return err
	}
	if h.ID == 0 {
		h.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetHeartbeatRegistration(callerID string) (*model.HeartbeatRegistration, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, group_name, interval_sec, max_missed, strategy,
			last_heartbeat_at, next_expected_at, missed_count, status,
			frozen_at, lost_at, recovered_at, created_at, updated_at
		FROM heartbeat_registrations WHERE caller_id = ?
	`, callerID)

	var h model.HeartbeatRegistration
	var frozenAt, lostAt, recoveredAt sql.NullTime
	err := row.Scan(&h.ID, &h.CallerID, &h.GroupName, &h.IntervalSec, &h.MaxMissed, &h.Strategy,
		&h.LastHeartbeatAt, &h.NextExpectedAt, &h.MissedCount, &h.Status,
		&frozenAt, &lostAt, &recoveredAt, &h.CreatedAt, &h.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if frozenAt.Valid {
		h.FrozenAt = &frozenAt.Time
	}
	if lostAt.Valid {
		h.LostAt = &lostAt.Time
	}
	if recoveredAt.Valid {
		h.RecoveredAt = &recoveredAt.Time
	}
	return &h, nil
}

func (s *Storage) UpdateHeartbeatRegistration(h *model.HeartbeatRegistration) error {
	_, err := s.db.Exec(`
		UPDATE heartbeat_registrations SET
			group_name = ?,
			interval_sec = ?,
			max_missed = ?,
			strategy = ?,
			last_heartbeat_at = ?,
			next_expected_at = ?,
			missed_count = ?,
			status = ?,
			frozen_at = ?,
			lost_at = ?,
			recovered_at = ?,
			updated_at = ?
		WHERE caller_id = ?
	`, h.GroupName, h.IntervalSec, h.MaxMissed, h.Strategy,
		h.LastHeartbeatAt, h.NextExpectedAt, h.MissedCount, h.Status,
		nullTimePtr(h.FrozenAt), nullTimePtr(h.LostAt), nullTimePtr(h.RecoveredAt),
		h.UpdatedAt, h.CallerID)
	return err
}

func (s *Storage) ListHeartbeatRegistrations() ([]model.HeartbeatRegistration, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, group_name, interval_sec, max_missed, strategy,
			last_heartbeat_at, next_expected_at, missed_count, status,
			frozen_at, lost_at, recovered_at, created_at, updated_at
		FROM heartbeat_registrations ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var registrations []model.HeartbeatRegistration
	for rows.Next() {
		var h model.HeartbeatRegistration
		var frozenAt, lostAt, recoveredAt sql.NullTime
		if err := rows.Scan(&h.ID, &h.CallerID, &h.GroupName, &h.IntervalSec, &h.MaxMissed, &h.Strategy,
			&h.LastHeartbeatAt, &h.NextExpectedAt, &h.MissedCount, &h.Status,
			&frozenAt, &lostAt, &recoveredAt, &h.CreatedAt, &h.UpdatedAt); err != nil {
			return nil, err
		}
		if frozenAt.Valid {
			h.FrozenAt = &frozenAt.Time
		}
		if lostAt.Valid {
			h.LostAt = &lostAt.Time
		}
		if recoveredAt.Valid {
			h.RecoveredAt = &recoveredAt.Time
		}
		registrations = append(registrations, h)
	}
	return registrations, nil
}

func (s *Storage) ListHeartbeatRegistrationsByStatus(status model.HeartbeatStatus) ([]model.HeartbeatRegistration, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, group_name, interval_sec, max_missed, strategy,
			last_heartbeat_at, next_expected_at, missed_count, status,
			frozen_at, lost_at, recovered_at, created_at, updated_at
		FROM heartbeat_registrations WHERE status = ? ORDER BY id
	`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var registrations []model.HeartbeatRegistration
	for rows.Next() {
		var h model.HeartbeatRegistration
		var frozenAt, lostAt, recoveredAt sql.NullTime
		if err := rows.Scan(&h.ID, &h.CallerID, &h.GroupName, &h.IntervalSec, &h.MaxMissed, &h.Strategy,
			&h.LastHeartbeatAt, &h.NextExpectedAt, &h.MissedCount, &h.Status,
			&frozenAt, &lostAt, &recoveredAt, &h.CreatedAt, &h.UpdatedAt); err != nil {
			return nil, err
		}
		if frozenAt.Valid {
			h.FrozenAt = &frozenAt.Time
		}
		if lostAt.Valid {
			h.LostAt = &lostAt.Time
		}
		if recoveredAt.Valid {
			h.RecoveredAt = &recoveredAt.Time
		}
		registrations = append(registrations, h)
	}
	return registrations, nil
}

func (s *Storage) AddHeartbeatEvent(e *model.HeartbeatEvent) error {
	result, err := s.db.Exec(`
		INSERT INTO heartbeat_events (caller_id, event_type, from_status, to_status, detail, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, e.CallerID, e.EventType, e.FromStatus, e.ToStatus, e.Detail, e.CreatedAt)
	if err != nil {
		return err
	}
	e.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListHeartbeatEvents(callerID string, limit int) ([]model.HeartbeatEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	var rows *sql.Rows
	var err error
	if callerID != "" {
		rows, err = s.db.Query(`
			SELECT id, caller_id, event_type, from_status, to_status, detail, created_at
			FROM heartbeat_events WHERE caller_id = ? ORDER BY id DESC LIMIT ?
		`, callerID, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT id, caller_id, event_type, from_status, to_status, detail, created_at
			FROM heartbeat_events ORDER BY id DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.HeartbeatEvent
	for rows.Next() {
		var e model.HeartbeatEvent
		if err := rows.Scan(&e.ID, &e.CallerID, &e.EventType, &e.FromStatus, &e.ToStatus, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

func (s *Storage) CreateFrozenResource(f *model.FrozenResource) error {
	result, err := s.db.Exec(`
		INSERT INTO frozen_resources (caller_id, resource_type, resource_key, frozen_at, released_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, f.CallerID, f.ResourceType, f.ResourceKey, f.FrozenAt, nullTimePtr(f.ReleasedAt), f.FrozenAt)
	if err != nil {
		return err
	}
	f.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListFrozenResources(callerID string) ([]model.FrozenResource, error) {
	var rows *sql.Rows
	var err error
	if callerID != "" {
		rows, err = s.db.Query(`
			SELECT id, caller_id, resource_type, resource_key, frozen_at, released_at
			FROM frozen_resources WHERE caller_id = ? AND released_at IS NULL ORDER BY id
		`, callerID)
	} else {
		rows, err = s.db.Query(`
			SELECT id, caller_id, resource_type, resource_key, frozen_at, released_at
			FROM frozen_resources WHERE released_at IS NULL ORDER BY id
		`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resources []model.FrozenResource
	for rows.Next() {
		var f model.FrozenResource
		var releasedAt sql.NullTime
		if err := rows.Scan(&f.ID, &f.CallerID, &f.ResourceType, &f.ResourceKey, &f.FrozenAt, &releasedAt); err != nil {
			return nil, err
		}
		if releasedAt.Valid {
			f.ReleasedAt = &releasedAt.Time
		}
		resources = append(resources, f)
	}
	return resources, nil
}

func (s *Storage) ReleaseFrozenResource(id int64, releasedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE frozen_resources SET released_at = ? WHERE id = ?
	`, releasedAt, id)
	return err
}

func (s *Storage) ReleaseAllFrozenResources(callerID string, releasedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE frozen_resources SET released_at = ? WHERE caller_id = ? AND released_at IS NULL
	`, releasedAt, callerID)
	return err
}

func (s *Storage) CreateHeartbeatGroup(g *model.HeartbeatGroup) error {
	result, err := s.db.Exec(`
		INSERT INTO heartbeat_groups (name, survival_threshold, status, alive_count, total_count,
			degraded, degraded_reason, degraded_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, g.Name, g.SurvivalThreshold, g.Status, g.AliveCount, g.TotalCount,
		g.Degraded, g.DegradedReason, nullTimePtr(g.DegradedAt),
		g.CreatedAt, g.UpdatedAt)
	if err != nil {
		return err
	}
	g.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetHeartbeatGroup(name string) (*model.HeartbeatGroup, error) {
	row := s.db.QueryRow(`
		SELECT id, name, survival_threshold, status, alive_count, total_count,
			degraded, degraded_reason, degraded_at, created_at, updated_at
		FROM heartbeat_groups WHERE name = ?
	`, name)

	var g model.HeartbeatGroup
	var degradedInt int
	var degradedAt sql.NullTime
	err := row.Scan(&g.ID, &g.Name, &g.SurvivalThreshold, &g.Status, &g.AliveCount, &g.TotalCount,
		&degradedInt, &g.DegradedReason, &degradedAt, &g.CreatedAt, &g.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	g.Degraded = degradedInt != 0
	if degradedAt.Valid {
		g.DegradedAt = &degradedAt.Time
	}
	return &g, nil
}

func (s *Storage) UpdateHeartbeatGroup(g *model.HeartbeatGroup) error {
	degradedInt := 0
	if g.Degraded {
		degradedInt = 1
	}
	_, err := s.db.Exec(`
		UPDATE heartbeat_groups SET
			survival_threshold = ?,
			status = ?,
			alive_count = ?,
			total_count = ?,
			degraded = ?,
			degraded_reason = ?,
			degraded_at = ?,
			updated_at = ?
		WHERE name = ?
	`, g.SurvivalThreshold, g.Status, g.AliveCount, g.TotalCount,
		degradedInt, g.DegradedReason, nullTimePtr(g.DegradedAt),
		g.UpdatedAt, g.Name)
	return err
}

func (s *Storage) DeleteHeartbeatGroup(name string) error {
	_, err := s.db.Exec(`DELETE FROM heartbeat_groups WHERE name = ?`, name)
	return err
}

func (s *Storage) ListHeartbeatGroups() ([]model.HeartbeatGroup, error) {
	rows, err := s.db.Query(`
		SELECT id, name, survival_threshold, status, alive_count, total_count,
			degraded, degraded_reason, degraded_at, created_at, updated_at
		FROM heartbeat_groups ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []model.HeartbeatGroup
	for rows.Next() {
		var g model.HeartbeatGroup
		var degradedInt int
		var degradedAt sql.NullTime
		if err := rows.Scan(&g.ID, &g.Name, &g.SurvivalThreshold, &g.Status, &g.AliveCount, &g.TotalCount,
			&degradedInt, &g.DegradedReason, &degradedAt, &g.CreatedAt, &g.UpdatedAt); err != nil {
			return nil, err
		}
		g.Degraded = degradedInt != 0
		if degradedAt.Valid {
			g.DegradedAt = &degradedAt.Time
		}
		groups = append(groups, g)
	}
	return groups, nil
}

func (s *Storage) ListHeartbeatRegistrationsByGroup(groupName string) ([]model.HeartbeatRegistration, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, group_name, interval_sec, max_missed, strategy,
			last_heartbeat_at, next_expected_at, missed_count, status,
			frozen_at, lost_at, recovered_at, created_at, updated_at
		FROM heartbeat_registrations WHERE group_name = ? ORDER BY id
	`, groupName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var registrations []model.HeartbeatRegistration
	for rows.Next() {
		var h model.HeartbeatRegistration
		var frozenAt, lostAt, recoveredAt sql.NullTime
		if err := rows.Scan(&h.ID, &h.CallerID, &h.GroupName, &h.IntervalSec, &h.MaxMissed, &h.Strategy,
			&h.LastHeartbeatAt, &h.NextExpectedAt, &h.MissedCount, &h.Status,
			&frozenAt, &lostAt, &recoveredAt, &h.CreatedAt, &h.UpdatedAt); err != nil {
			return nil, err
		}
		if frozenAt.Valid {
			h.FrozenAt = &frozenAt.Time
		}
		if lostAt.Valid {
			h.LostAt = &lostAt.Time
		}
		if recoveredAt.Valid {
			h.RecoveredAt = &recoveredAt.Time
		}
		registrations = append(registrations, h)
	}
	return registrations, nil
}

func (s *Storage) CreateGroupDependency(dep *model.HeartbeatGroupDependency) error {
	result, err := s.db.Exec(`
		INSERT OR IGNORE INTO heartbeat_group_dependencies (group_name, depends_on, created_at)
		VALUES (?, ?, ?)
	`, dep.GroupName, dep.DependsOn, dep.CreatedAt)
	if err != nil {
		return err
	}
	dep.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) RemoveGroupDependency(groupName, dependsOn string) error {
	_, err := s.db.Exec(`
		DELETE FROM heartbeat_group_dependencies WHERE group_name = ? AND depends_on = ?
	`, groupName, dependsOn)
	return err
}

func (s *Storage) ListGroupDependencies() ([]model.HeartbeatGroupDependency, error) {
	rows, err := s.db.Query(`
		SELECT id, group_name, depends_on, created_at
		FROM heartbeat_group_dependencies ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []model.HeartbeatGroupDependency
	for rows.Next() {
		var d model.HeartbeatGroupDependency
		if err := rows.Scan(&d.ID, &d.GroupName, &d.DependsOn, &d.CreatedAt); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, nil
}

func (s *Storage) ListGroupDependenciesByGroup(groupName string) ([]model.HeartbeatGroupDependency, error) {
	rows, err := s.db.Query(`
		SELECT id, group_name, depends_on, created_at
		FROM heartbeat_group_dependencies WHERE group_name = ? ORDER BY id
	`, groupName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []model.HeartbeatGroupDependency
	for rows.Next() {
		var d model.HeartbeatGroupDependency
		if err := rows.Scan(&d.ID, &d.GroupName, &d.DependsOn, &d.CreatedAt); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, nil
}

func (s *Storage) ListGroupsThatDependOn(groupName string) ([]model.HeartbeatGroupDependency, error) {
	rows, err := s.db.Query(`
		SELECT id, group_name, depends_on, created_at
		FROM heartbeat_group_dependencies WHERE depends_on = ? ORDER BY id
	`, groupName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []model.HeartbeatGroupDependency
	for rows.Next() {
		var d model.HeartbeatGroupDependency
		if err := rows.Scan(&d.ID, &d.GroupName, &d.DependsOn, &d.CreatedAt); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, nil
}

func (s *Storage) ListDegradedGroups() ([]model.HeartbeatGroup, error) {
	rows, err := s.db.Query(`
		SELECT id, name, survival_threshold, status, alive_count, total_count,
			degraded, degraded_reason, degraded_at, created_at, updated_at
		FROM heartbeat_groups WHERE degraded = 1 ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []model.HeartbeatGroup
	for rows.Next() {
		var g model.HeartbeatGroup
		var degradedInt int
		var degradedAt sql.NullTime
		if err := rows.Scan(&g.ID, &g.Name, &g.SurvivalThreshold, &g.Status, &g.AliveCount, &g.TotalCount,
			&degradedInt, &g.DegradedReason, &degradedAt, &g.CreatedAt, &g.UpdatedAt); err != nil {
			return nil, err
		}
		g.Degraded = degradedInt != 0
		if degradedAt.Valid {
			g.DegradedAt = &degradedAt.Time
		}
		groups = append(groups, g)
	}
	return groups, nil
}

func (s *Storage) UpsertLockContentionStat(stat *model.LockContentionMinuteStat) error {
	result, err := s.db.Exec(`
		INSERT INTO heatmap_lock_stats (lock_name, minute_bucket, request_count, wait_count, total_wait_ms, max_wait_ms, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(lock_name, minute_bucket) DO UPDATE SET
			request_count = request_count + excluded.request_count,
			wait_count = wait_count + excluded.wait_count,
			total_wait_ms = total_wait_ms + excluded.total_wait_ms,
			max_wait_ms = CASE WHEN excluded.max_wait_ms > max_wait_ms THEN excluded.max_wait_ms ELSE max_wait_ms END,
			updated_at = excluded.updated_at
	`, stat.LockName, stat.MinuteBucket, stat.RequestCount, stat.WaitCount, stat.TotalWaitMs, stat.MaxWaitMs, stat.CreatedAt, stat.UpdatedAt)
	if err != nil {
		return err
	}
	if stat.ID == 0 {
		stat.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetLockContentionStatsInWindow(lockName string, startTime time.Time, endTime time.Time) ([]model.LockContentionMinuteStat, error) {
	var rows *sql.Rows
	var err error
	if lockName != "" {
		rows, err = s.db.Query(`
			SELECT id, lock_name, minute_bucket, request_count, wait_count, total_wait_ms, max_wait_ms, created_at, updated_at
			FROM heatmap_lock_stats
			WHERE lock_name = ? AND minute_bucket >= ? AND minute_bucket < ?
			ORDER BY minute_bucket
		`, lockName, startTime, endTime)
	} else {
		rows, err = s.db.Query(`
			SELECT id, lock_name, minute_bucket, request_count, wait_count, total_wait_ms, max_wait_ms, created_at, updated_at
			FROM heatmap_lock_stats
			WHERE minute_bucket >= ? AND minute_bucket < ?
			ORDER BY minute_bucket
		`, startTime, endTime)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []model.LockContentionMinuteStat
	for rows.Next() {
		var st model.LockContentionMinuteStat
		if err := rows.Scan(&st.ID, &st.LockName, &st.MinuteBucket, &st.RequestCount, &st.WaitCount, &st.TotalWaitMs, &st.MaxWaitMs, &st.CreatedAt, &st.UpdatedAt); err != nil {
			return nil, err
		}
		stats = append(stats, st)
	}
	return stats, nil
}

func (s *Storage) GetAggregatedLockHeatInWindow(startTime time.Time, endTime time.Time) ([]model.LockHeatInfo, error) {
	rows, err := s.db.Query(`
		SELECT
			lock_name,
			SUM(request_count) as request_count,
			SUM(wait_count) as wait_count,
			SUM(total_wait_ms) as total_wait_ms,
			MAX(max_wait_ms) as max_wait_ms
		FROM heatmap_lock_stats
		WHERE minute_bucket >= ? AND minute_bucket < ?
		GROUP BY lock_name
		ORDER BY total_wait_ms DESC
	`, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var heats []model.LockHeatInfo
	for rows.Next() {
		var h model.LockHeatInfo
		var totalWaitMs int64
		if err := rows.Scan(&h.LockName, &h.RequestCount, &h.WaitCount, &totalWaitMs, &h.MaxWaitMs); err != nil {
			return nil, err
		}
		if h.WaitCount > 0 {
			h.AvgWaitMs = float64(totalWaitMs) / float64(h.WaitCount)
		}
		h.HeatScore = float64(h.RequestCount)*0.3 + h.AvgWaitMs*0.5 + float64(h.MaxWaitMs)*0.2
		heats = append(heats, h)
	}
	return heats, nil
}

func (s *Storage) PurgeOldLockContentionStats(retentionMin int) (int64, error) {
	cutoff := time.Now().Add(-time.Duration(retentionMin) * time.Minute)
	result, err := s.db.Exec(`DELETE FROM heatmap_lock_stats WHERE minute_bucket < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Storage) CreateHotspotAlert(alert *model.HotspotAlertEvent) error {
	ackInt := 0
	if alert.Acknowledged {
		ackInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO heatmap_alerts (lock_name, avg_wait_ms, threshold_ms, request_count, wait_count, max_wait_ms,
			current_queue_len, window_minutes, alert_type, detail, acknowledged, acknowledged_at, acknowledged_by, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, alert.LockName, alert.AvgWaitMs, alert.ThresholdMs, alert.RequestCount, alert.WaitCount, alert.MaxWaitMs,
		alert.CurrentQueueLen, alert.WindowMinutes, alert.AlertType, alert.Detail, ackInt, nullTimePtr(alert.AcknowledgedAt), alert.AcknowledgedBy, alert.CreatedAt)
	if err != nil {
		return err
	}
	alert.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListHotspotAlerts(lockName string, acknowledged *bool, limit int) ([]model.HotspotAlertEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT id, lock_name, avg_wait_ms, threshold_ms, request_count, wait_count, max_wait_ms,
			current_queue_len, window_minutes, alert_type, detail, acknowledged, acknowledged_at, acknowledged_by, created_at
		FROM heatmap_alerts WHERE 1=1
	`
	var args []interface{}
	if lockName != "" {
		query += " AND lock_name = ?"
		args = append(args, lockName)
	}
	if acknowledged != nil {
		query += " AND acknowledged = ?"
		ackInt := 0
		if *acknowledged {
			ackInt = 1
		}
		args = append(args, ackInt)
	}
	query += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []model.HotspotAlertEvent
	for rows.Next() {
		var a model.HotspotAlertEvent
		var ackInt int
		var ackAt sql.NullTime
		var ackBy sql.NullString
		if err := rows.Scan(&a.ID, &a.LockName, &a.AvgWaitMs, &a.ThresholdMs, &a.RequestCount, &a.WaitCount, &a.MaxWaitMs,
			&a.CurrentQueueLen, &a.WindowMinutes, &a.AlertType, &a.Detail, &ackInt, &ackAt, &ackBy, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.Acknowledged = ackInt != 0
		if ackAt.Valid {
			a.AcknowledgedAt = &ackAt.Time
		}
		if ackBy.Valid {
			a.AcknowledgedBy = ackBy.String
		}
		alerts = append(alerts, a)
	}
	return alerts, nil
}

func (s *Storage) AcknowledgeHotspotAlert(id int64, acknowledgedBy string) error {
	now := time.Now()
	result, err := s.db.Exec(`
		UPDATE heatmap_alerts SET acknowledged = 1, acknowledged_at = ?, acknowledged_by = ? WHERE id = ?
	`, now, acknowledgedBy, id)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("alert %d not found", id)
	}
	return nil
}

func (s *Storage) UpsertCooldownState(state *model.LockCooldownState) error {
	now := time.Now()
	if state.ID > 0 {
		result, err := s.db.Exec(`
			UPDATE lock_cooldown_states SET
				status = ?, trigger_type = ?, original_lease_sec = ?, cooldown_lease_sec = ?,
				leases_shortened = ?, consecutive_hot_cycles = ?, avg_wait_ms_at_start = ?,
				threshold_ms_at_start = ?, started_at = ?, resolved_at = ?, resolve_reason = ?,
				updated_at = ?
			WHERE id = ?
		`, state.Status, state.TriggerType, state.OriginalLeaseSec, state.CooldownLeaseSec,
			state.LeasesShortened, state.ConsecutiveHotCycles, state.AvgWaitMsAtStart,
			state.ThresholdMsAtStart, state.StartedAt, state.ResolvedAt, state.ResolveReason,
			now, state.ID)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows > 0 {
			return nil
		}
	}

	result, err := s.db.Exec(`
		INSERT INTO lock_cooldown_states (
			lock_name, status, trigger_type, original_lease_sec, cooldown_lease_sec,
			leases_shortened, consecutive_hot_cycles, avg_wait_ms_at_start, threshold_ms_at_start,
			started_at, resolved_at, resolve_reason, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, state.LockName, state.Status, state.TriggerType, state.OriginalLeaseSec, state.CooldownLeaseSec,
		state.LeasesShortened, state.ConsecutiveHotCycles, state.AvgWaitMsAtStart, state.ThresholdMsAtStart,
		state.StartedAt, state.ResolvedAt, state.ResolveReason, now, now)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err == nil {
		state.ID = id
	}
	return nil
}

func (s *Storage) GetActiveCooldown(lockName string) (*model.LockCooldownState, error) {
	row := s.db.QueryRow(`
		SELECT id, lock_name, status, trigger_type, original_lease_sec, cooldown_lease_sec,
			leases_shortened, consecutive_hot_cycles, avg_wait_ms_at_start, threshold_ms_at_start,
			started_at, resolved_at, resolve_reason, created_at, updated_at
		FROM lock_cooldown_states
		WHERE lock_name = ? AND status = 'active'
		ORDER BY id DESC LIMIT 1
	`, lockName)

	var state model.LockCooldownState
	err := row.Scan(&state.ID, &state.LockName, &state.Status, &state.TriggerType, &state.OriginalLeaseSec, &state.CooldownLeaseSec,
		&state.LeasesShortened, &state.ConsecutiveHotCycles, &state.AvgWaitMsAtStart, &state.ThresholdMsAtStart,
		&state.StartedAt, &state.ResolvedAt, &state.ResolveReason, &state.CreatedAt, &state.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *Storage) ListActiveCooldowns() ([]model.LockCooldownState, error) {
	rows, err := s.db.Query(`
		SELECT id, lock_name, status, trigger_type, original_lease_sec, cooldown_lease_sec,
			leases_shortened, consecutive_hot_cycles, avg_wait_ms_at_start, threshold_ms_at_start,
			started_at, resolved_at, resolve_reason, created_at, updated_at
		FROM lock_cooldown_states
		WHERE status = 'active'
		ORDER BY started_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var states []model.LockCooldownState
	for rows.Next() {
		var state model.LockCooldownState
		err := rows.Scan(&state.ID, &state.LockName, &state.Status, &state.TriggerType, &state.OriginalLeaseSec, &state.CooldownLeaseSec,
			&state.LeasesShortened, &state.ConsecutiveHotCycles, &state.AvgWaitMsAtStart, &state.ThresholdMsAtStart,
			&state.StartedAt, &state.ResolvedAt, &state.ResolveReason, &state.CreatedAt, &state.UpdatedAt)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

func (s *Storage) CreateCooldownHistory(history *model.CooldownHistoryRecord) error {
	now := time.Now()
	result, err := s.db.Exec(`
		INSERT INTO cooldown_history_records (
			lock_name, trigger_type, original_lease_sec, cooldown_lease_sec, leases_shortened,
			avg_wait_ms_at_start, avg_wait_ms_at_end, threshold_ms, duration_sec,
			started_at, ended_at, resolve_reason, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, history.LockName, history.TriggerType, history.OriginalLeaseSec, history.CooldownLeaseSec, history.LeasesShortened,
		history.AvgWaitMsAtStart, history.AvgWaitMsAtEnd, history.ThresholdMs, history.DurationSec,
		history.StartedAt, history.EndedAt, history.ResolveReason, now)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err == nil {
		history.ID = id
	}
	return nil
}

func (s *Storage) ListCooldownHistory(lockName string, limit int) ([]model.CooldownHistoryRecord, error) {
	var rows *sql.Rows
	var err error

	query := `
		SELECT id, lock_name, trigger_type, original_lease_sec, cooldown_lease_sec, leases_shortened,
			avg_wait_ms_at_start, avg_wait_ms_at_end, threshold_ms, duration_sec,
			started_at, ended_at, resolve_reason, created_at
		FROM cooldown_history_records
	`
	if lockName != "" {
		query += ` WHERE lock_name = ? `
	}
	query += ` ORDER BY created_at DESC LIMIT ? `

	if lockName != "" {
		rows, err = s.db.Query(query, lockName, limit)
	} else {
		rows, err = s.db.Query(query, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []model.CooldownHistoryRecord
	for rows.Next() {
		var r model.CooldownHistoryRecord
		err := rows.Scan(&r.ID, &r.LockName, &r.TriggerType, &r.OriginalLeaseSec, &r.CooldownLeaseSec, &r.LeasesShortened,
			&r.AvgWaitMsAtStart, &r.AvgWaitMsAtEnd, &r.ThresholdMs, &r.DurationSec,
			&r.StartedAt, &r.EndedAt, &r.ResolveReason, &r.CreatedAt)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) GetHeatmapConfig() (*model.HeatmapConfig, error) {
	row := s.db.QueryRow(`
		SELECT window_minutes, alert_threshold_ms, alert_suppress_min, top_n, history_retention_min,
			cooldown_enabled, cooldown_consecutive_hot_cycles, cooldown_lease_sec, cooldown_lease_min_pct,
			cooldown_resolve_threshold_ms, cooldown_resolve_cycles, cooldown_max_sec, cooldown_accelerated_grant
		FROM heatmap_config WHERE id = 1
	`)
	var cfg model.HeatmapConfig
	var cooldownEnabledInt, cooldownAcceleratedGrantInt int
	err := row.Scan(&cfg.WindowMinutes, &cfg.AlertThresholdMs, &cfg.AlertSuppressMin, &cfg.TopN, &cfg.HistoryRetentionMin,
		&cooldownEnabledInt, &cfg.Cooldown.ConsecutiveHotCycles, &cfg.Cooldown.CooldownLeaseSec, &cfg.Cooldown.CooldownLeaseMinPct,
		&cfg.Cooldown.ResolveThresholdMs, &cfg.Cooldown.ResolveConsecutiveCycles, &cfg.Cooldown.MaxCooldownSec, &cooldownAcceleratedGrantInt)
	if err == sql.ErrNoRows {
		defaultCfg := &model.HeatmapConfig{
			WindowMinutes:       5,
			AlertThresholdMs:    5000,
			AlertSuppressMin:    10,
			TopN:                10,
			HistoryRetentionMin: 1440,
			Cooldown: model.CooldownConfig{
				Enabled:                  true,
				ConsecutiveHotCycles:     3,
				CooldownLeaseSec:         30,
				CooldownLeaseMinPct:      10,
				ResolveThresholdMs:       1000,
				ResolveConsecutiveCycles: 2,
				MaxCooldownSec:           3600,
				AcceleratedGrant:         true,
			},
		}
		_, err := s.db.Exec(`
			INSERT INTO heatmap_config (id, window_minutes, alert_threshold_ms, alert_suppress_min, top_n, history_retention_min,
				cooldown_enabled, cooldown_consecutive_hot_cycles, cooldown_lease_sec, cooldown_lease_min_pct,
				cooldown_resolve_threshold_ms, cooldown_resolve_cycles, cooldown_max_sec, cooldown_accelerated_grant, updated_at)
			VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, defaultCfg.WindowMinutes, defaultCfg.AlertThresholdMs, defaultCfg.AlertSuppressMin, defaultCfg.TopN, defaultCfg.HistoryRetentionMin,
			1, defaultCfg.Cooldown.ConsecutiveHotCycles, defaultCfg.Cooldown.CooldownLeaseSec, defaultCfg.Cooldown.CooldownLeaseMinPct,
			defaultCfg.Cooldown.ResolveThresholdMs, defaultCfg.Cooldown.ResolveConsecutiveCycles, defaultCfg.Cooldown.MaxCooldownSec, 1, time.Now())
		if err != nil {
			return nil, err
		}
		return defaultCfg, nil
	}
	if err != nil {
		return nil, err
	}
	if cfg.AlertSuppressMin <= 0 {
		cfg.AlertSuppressMin = 10
	}
	cfg.Cooldown.Enabled = cooldownEnabledInt != 0
	cfg.Cooldown.AcceleratedGrant = cooldownAcceleratedGrantInt != 0
	return &cfg, nil
}

func (s *Storage) UpdateHeatmapConfig(cfg *model.HeatmapConfig) error {
	if cfg.AlertSuppressMin <= 0 {
		cfg.AlertSuppressMin = 10
	}
	cooldownEnabledInt := 0
	if cfg.Cooldown.Enabled {
		cooldownEnabledInt = 1
	}
	cooldownAcceleratedGrantInt := 0
	if cfg.Cooldown.AcceleratedGrant {
		cooldownAcceleratedGrantInt = 1
	}
	_, err := s.db.Exec(`
		INSERT INTO heatmap_config (id, window_minutes, alert_threshold_ms, alert_suppress_min, top_n, history_retention_min,
			cooldown_enabled, cooldown_consecutive_hot_cycles, cooldown_lease_sec, cooldown_lease_min_pct,
			cooldown_resolve_threshold_ms, cooldown_resolve_cycles, cooldown_max_sec, cooldown_accelerated_grant, updated_at)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			window_minutes = excluded.window_minutes,
			alert_threshold_ms = excluded.alert_threshold_ms,
			alert_suppress_min = excluded.alert_suppress_min,
			top_n = excluded.top_n,
			history_retention_min = excluded.history_retention_min,
			cooldown_enabled = excluded.cooldown_enabled,
			cooldown_consecutive_hot_cycles = excluded.cooldown_consecutive_hot_cycles,
			cooldown_lease_sec = excluded.cooldown_lease_sec,
			cooldown_lease_min_pct = excluded.cooldown_lease_min_pct,
			cooldown_resolve_threshold_ms = excluded.cooldown_resolve_threshold_ms,
			cooldown_resolve_cycles = excluded.cooldown_resolve_cycles,
			cooldown_max_sec = excluded.cooldown_max_sec,
			cooldown_accelerated_grant = excluded.cooldown_accelerated_grant,
			updated_at = excluded.updated_at
	`, cfg.WindowMinutes, cfg.AlertThresholdMs, cfg.AlertSuppressMin, cfg.TopN, cfg.HistoryRetentionMin,
		cooldownEnabledInt, cfg.Cooldown.ConsecutiveHotCycles, cfg.Cooldown.CooldownLeaseSec, cfg.Cooldown.CooldownLeaseMinPct,
		cfg.Cooldown.ResolveThresholdMs, cfg.Cooldown.ResolveConsecutiveCycles, cfg.Cooldown.MaxCooldownSec, cooldownAcceleratedGrantInt, time.Now())
	return err
}

func (s *Storage) UpsertLockBudgetConfig(cfg *model.LockBudgetConfig) error {
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_configs (caller_id, budget_limit, period_sec, warning_pct, overdraft_limit, max_concurrent_locks, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			budget_limit = excluded.budget_limit,
			period_sec = excluded.period_sec,
			warning_pct = excluded.warning_pct,
			overdraft_limit = excluded.overdraft_limit,
			max_concurrent_locks = excluded.max_concurrent_locks,
			updated_at = excluded.updated_at
	`, cfg.CallerID, cfg.BudgetLimit, cfg.PeriodSec, cfg.WarningPct, cfg.OverdraftLimit, cfg.MaxConcurrentLocks, cfg.CreatedAt, cfg.UpdatedAt)
	if err != nil {
		return err
	}
	if cfg.ID == 0 {
		cfg.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetLockBudgetConfig(callerID string) (*model.LockBudgetConfig, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, budget_limit, period_sec, warning_pct, overdraft_limit, COALESCE(max_concurrent_locks, 0), created_at, updated_at
		FROM lock_budget_configs WHERE caller_id = ?
	`, callerID)

	var cfg model.LockBudgetConfig
	err := row.Scan(&cfg.ID, &cfg.CallerID, &cfg.BudgetLimit, &cfg.PeriodSec, &cfg.WarningPct, &cfg.OverdraftLimit, &cfg.MaxConcurrentLocks, &cfg.CreatedAt, &cfg.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *Storage) ListLockBudgetConfigs() ([]model.LockBudgetConfig, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, budget_limit, period_sec, warning_pct, overdraft_limit, COALESCE(max_concurrent_locks, 0), created_at, updated_at
		FROM lock_budget_configs ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []model.LockBudgetConfig
	for rows.Next() {
		var cfg model.LockBudgetConfig
		if err := rows.Scan(&cfg.ID, &cfg.CallerID, &cfg.BudgetLimit, &cfg.PeriodSec, &cfg.WarningPct, &cfg.OverdraftLimit, &cfg.MaxConcurrentLocks, &cfg.CreatedAt, &cfg.UpdatedAt); err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	return configs, nil
}

func (s *Storage) DeleteLockBudgetConfig(callerID string) error {
	_, err := s.db.Exec(`DELETE FROM lock_budget_configs WHERE caller_id = ?`, callerID)
	return err
}

type BudgetHoldingRecord struct {
	ID            int64
	CallerID      string
	LockName      string
	AcquiredAt    time.Time
	ExpiresAt     time.Time
	LastMeteredAt time.Time
	UnitsAccrued  int
}

func (s *Storage) UpsertBudgetHolding(callerID, lockName string, acquiredAt, expiresAt, lastMeteredAt time.Time, unitsAccrued int) error {
	_, err := s.db.Exec(`
		INSERT INTO lock_budget_holdings (caller_id, lock_name, acquired_at, expires_at, last_metered_at, units_accrued)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id, lock_name) DO UPDATE SET
			expires_at = excluded.expires_at,
			last_metered_at = excluded.last_metered_at,
			units_accrued = excluded.units_accrued
	`, callerID, lockName, acquiredAt, expiresAt, lastMeteredAt, unitsAccrued)
	return err
}

func (s *Storage) UpdateBudgetHoldingMeter(callerID, lockName string, lastMeteredAt time.Time, unitsAccrued int) error {
	_, err := s.db.Exec(`
		UPDATE lock_budget_holdings SET last_metered_at = ?, units_accrued = ?
		WHERE caller_id = ? AND lock_name = ?
	`, lastMeteredAt, unitsAccrued, callerID, lockName)
	return err
}

func (s *Storage) UpdateBudgetHoldingExpiry(callerID, lockName string, expiresAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE lock_budget_holdings SET expires_at = ?
		WHERE caller_id = ? AND lock_name = ?
	`, expiresAt, callerID, lockName)
	return err
}

func (s *Storage) RemoveBudgetHolding(callerID, lockName string) error {
	_, err := s.db.Exec(`
		DELETE FROM lock_budget_holdings WHERE caller_id = ? AND lock_name = ?
	`, callerID, lockName)
	return err
}

func (s *Storage) ListBudgetHoldingsByCaller(callerID string) ([]BudgetHoldingRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, lock_name, acquired_at, expires_at, last_metered_at, units_accrued
		FROM lock_budget_holdings WHERE caller_id = ? ORDER BY acquired_at
	`, callerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []BudgetHoldingRecord
	for rows.Next() {
		var r BudgetHoldingRecord
		if err := rows.Scan(&r.ID, &r.CallerID, &r.LockName, &r.AcquiredAt, &r.ExpiresAt, &r.LastMeteredAt, &r.UnitsAccrued); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) ListAllBudgetHoldings() ([]BudgetHoldingRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, lock_name, acquired_at, expires_at, last_metered_at, units_accrued
		FROM lock_budget_holdings ORDER BY caller_id, acquired_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []BudgetHoldingRecord
	for rows.Next() {
		var r BudgetHoldingRecord
		if err := rows.Scan(&r.ID, &r.CallerID, &r.LockName, &r.AcquiredAt, &r.ExpiresAt, &r.LastMeteredAt, &r.UnitsAccrued); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) CountActiveBudgetHoldingsByCaller(callerID string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM lock_budget_holdings WHERE caller_id = ?`, callerID).Scan(&count)
	return count, err
}

func (s *Storage) AddBudgetExhaustEvent(e *model.BudgetExhaustEvent) error {
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_exhaust_events (
			caller_id, consumed_units, budget_limit, period_start_at, period_end_at,
			attempted_lock, units_requested, detail, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, e.CallerID, e.ConsumedUnits, e.BudgetLimit, e.PeriodStartAt, e.PeriodEndAt,
		e.AttemptedLock, e.UnitsRequested, e.Detail, e.CreatedAt)
	if err != nil {
		return err
	}
	e.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListBudgetExhaustEvents(callerID string, limit int) ([]model.BudgetExhaustEvent, error) {
	if limit <= 0 {
		limit = 50
	}

	var rows *sql.Rows
	var err error

	if callerID != "" {
		rows, err = s.db.Query(`
			SELECT id, caller_id, consumed_units, budget_limit, period_start_at, period_end_at,
				attempted_lock, units_requested, detail, created_at
			FROM lock_budget_exhaust_events WHERE caller_id = ? ORDER BY id DESC LIMIT ?
		`, callerID, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT id, caller_id, consumed_units, budget_limit, period_start_at, period_end_at,
				attempted_lock, units_requested, detail, created_at
			FROM lock_budget_exhaust_events ORDER BY id DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []model.BudgetExhaustEvent
	for rows.Next() {
		var e model.BudgetExhaustEvent
		if err := rows.Scan(&e.ID, &e.CallerID, &e.ConsumedUnits, &e.BudgetLimit, &e.PeriodStartAt, &e.PeriodEndAt,
			&e.AttemptedLock, &e.UnitsRequested, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

func (s *Storage) CountBudgetExhaustEventsSince(callerID string, since time.Time) (int64, error) {
	var count int64
	var err error

	if callerID != "" {
		err = s.db.QueryRow(`
			SELECT COUNT(*) FROM lock_budget_exhaust_events WHERE caller_id = ? AND created_at >= ?
		`, callerID, since).Scan(&count)
	} else {
		err = s.db.QueryRow(`
			SELECT COUNT(*) FROM lock_budget_exhaust_events WHERE created_at >= ?
		`, since).Scan(&count)
	}
	return count, err
}

func (s *Storage) UpsertBudgetPeriodSummary(summary *model.BudgetPeriodSummary) error {
	_, err := s.db.Exec(`
		INSERT INTO lock_budget_period_summaries (
			caller_id, period_start_at, period_end_at, budget_limit, overdraft_limit,
			total_consumed, overdraft_used, overdraft_penalty, transferred_in, transferred_out,
			carry_over_deduction, peak_concurrent, lock_count, exhaust_events, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id, period_start_at) DO UPDATE SET
			period_end_at = excluded.period_end_at,
			budget_limit = excluded.budget_limit,
			overdraft_limit = excluded.overdraft_limit,
			total_consumed = excluded.total_consumed,
			overdraft_used = lock_budget_period_summaries.overdraft_used + excluded.overdraft_used,
			overdraft_penalty = lock_budget_period_summaries.overdraft_penalty + excluded.overdraft_penalty,
			transferred_in = lock_budget_period_summaries.transferred_in + excluded.transferred_in,
			transferred_out = lock_budget_period_summaries.transferred_out + excluded.transferred_out,
			carry_over_deduction = excluded.carry_over_deduction,
			peak_concurrent = MAX(peak_concurrent, excluded.peak_concurrent),
			lock_count = excluded.lock_count,
			exhaust_events = lock_budget_period_summaries.exhaust_events + excluded.exhaust_events
	`, summary.CallerID, summary.PeriodStartAt, summary.PeriodEndAt, summary.BudgetLimit, summary.OverdraftLimit,
		summary.TotalConsumed, summary.OverdraftUsed, summary.OverdraftPenalty,
		summary.TransferredIn, summary.TransferredOut, summary.CarryOverDeduction,
		summary.PeakConcurrent, summary.LockCount, summary.ExhaustEvents, time.Now())
	return err
}

func (s *Storage) ListBudgetPeriodSummaries(callerID string, limit int) ([]model.BudgetPeriodSummary, error) {
	if limit <= 0 {
		limit = 50
	}

	var rows *sql.Rows
	var err error

	if callerID != "" {
		rows, err = s.db.Query(`
			SELECT caller_id, period_start_at, period_end_at, budget_limit, overdraft_limit,
				total_consumed, overdraft_used, overdraft_penalty, transferred_in, transferred_out,
				carry_over_deduction, peak_concurrent, lock_count, exhaust_events
			FROM lock_budget_period_summaries WHERE caller_id = ? ORDER BY period_start_at DESC LIMIT ?
		`, callerID, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT caller_id, period_start_at, period_end_at, budget_limit, overdraft_limit,
				total_consumed, overdraft_used, overdraft_penalty, transferred_in, transferred_out,
				carry_over_deduction, peak_concurrent, lock_count, exhaust_events
			FROM lock_budget_period_summaries ORDER BY period_start_at DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []model.BudgetPeriodSummary
	for rows.Next() {
		var s model.BudgetPeriodSummary
		if err := rows.Scan(&s.CallerID, &s.PeriodStartAt, &s.PeriodEndAt, &s.BudgetLimit, &s.OverdraftLimit,
			&s.TotalConsumed, &s.OverdraftUsed, &s.OverdraftPenalty,
			&s.TransferredIn, &s.TransferredOut, &s.CarryOverDeduction,
			&s.PeakConcurrent, &s.LockCount, &s.ExhaustEvents); err != nil {
			return nil, err
		}
		summaries = append(summaries, s)
	}
	return summaries, nil
}

func (s *Storage) SumTotalConsumedSince(since time.Time) (int64, error) {
	var total sql.NullInt64
	err := s.db.QueryRow(`
		SELECT COALESCE(SUM(total_consumed), 0)
		FROM lock_budget_period_summaries WHERE period_start_at >= ?
	`, since).Scan(&total)
	if err != nil {
		return 0, err
	}
	if total.Valid {
		return total.Int64, nil
	}
	return 0, nil
}

func (s *Storage) AddBudgetTransfer(r *model.BudgetTransferRecord) error {
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_transfers (from_caller, to_caller, amount, reason, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, r.FromCaller, r.ToCaller, r.Amount, r.Reason, r.CreatedAt)
	if err != nil {
		return err
	}
	r.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListBudgetTransfers(query *model.BudgetTransferListQuery) ([]model.BudgetTransferRecord, int64, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}

	whereClauses := []string{}
	args := []interface{}{}

	if query.CallerID != "" {
		whereClauses = append(whereClauses, "(from_caller = ? OR to_caller = ?)")
		args = append(args, query.CallerID, query.CallerID)
	}
	if query.FromCaller != "" {
		whereClauses = append(whereClauses, "from_caller = ?")
		args = append(args, query.FromCaller)
	}
	if query.ToCaller != "" {
		whereClauses = append(whereClauses, "to_caller = ?")
		args = append(args, query.ToCaller)
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	countRow := s.db.QueryRow("SELECT COUNT(*) FROM lock_budget_transfers"+whereSQL, args...)
	var total int64
	if err := countRow.Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, limit, offset)
	rows, err := s.db.Query(`
		SELECT id, from_caller, to_caller, amount, reason, created_at
		FROM lock_budget_transfers`+whereSQL+`
		ORDER BY id DESC LIMIT ? OFFSET ?
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var records []model.BudgetTransferRecord
	for rows.Next() {
		var r model.BudgetTransferRecord
		if err := rows.Scan(&r.ID, &r.FromCaller, &r.ToCaller, &r.Amount, &r.Reason, &r.CreatedAt); err != nil {
			return nil, 0, err
		}
		records = append(records, r)
	}
	return records, total, nil
}

func (s *Storage) ListBudgetTransfersByCaller(callerID string, limit int) ([]model.BudgetTransferRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(`
		SELECT id, from_caller, to_caller, amount, reason, created_at
		FROM lock_budget_transfers
		WHERE from_caller = ? OR to_caller = ?
		ORDER BY id DESC LIMIT ?
	`, callerID, callerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []model.BudgetTransferRecord
	for rows.Next() {
		var r model.BudgetTransferRecord
		if err := rows.Scan(&r.ID, &r.FromCaller, &r.ToCaller, &r.Amount, &r.Reason, &r.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) CreateBudgetSettlementBill(bill *model.BudgetSettlementBill) error {
	hadOverdraftInt := 0
	if bill.HadOverdraft {
		hadOverdraftInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_settlement_bills (
			caller_id, period_start_at, period_end_at, budget_limit, overdraft_limit,
			normal_consumption, overdraft_consumption, overdraft_penalty, total_consumption,
			transferred_in, transferred_out, ending_balance, had_overdraft, peak_overdraft,
			peak_concurrent, exhaust_events, carry_over_to_next_period, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, bill.CallerID, bill.PeriodStartAt, bill.PeriodEndAt, bill.BudgetLimit, bill.OverdraftLimit,
		bill.NormalConsumption, bill.OverdraftConsumption, bill.OverdraftPenalty, bill.TotalConsumption,
		bill.TransferredIn, bill.TransferredOut, bill.EndingBalance, hadOverdraftInt, bill.PeakOverdraft,
		bill.PeakConcurrent, bill.ExhaustEvents, bill.CarryOverToNextPeriod, bill.Status, bill.CreatedAt)
	if err != nil {
		return err
	}
	bill.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) GetBudgetSettlementBill(id int64) (*model.BudgetSettlementBill, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, period_start_at, period_end_at, budget_limit, overdraft_limit,
			normal_consumption, overdraft_consumption, overdraft_penalty, total_consumption,
			transferred_in, transferred_out, ending_balance, had_overdraft, peak_overdraft,
			peak_concurrent, exhaust_events, carry_over_to_next_period, status, created_at
		FROM lock_budget_settlement_bills WHERE id = ?
	`, id)

	var bill model.BudgetSettlementBill
	var hadOverdraftInt int
	err := row.Scan(&bill.ID, &bill.CallerID, &bill.PeriodStartAt, &bill.PeriodEndAt, &bill.BudgetLimit, &bill.OverdraftLimit,
		&bill.NormalConsumption, &bill.OverdraftConsumption, &bill.OverdraftPenalty, &bill.TotalConsumption,
		&bill.TransferredIn, &bill.TransferredOut, &bill.EndingBalance, &hadOverdraftInt, &bill.PeakOverdraft,
		&bill.PeakConcurrent, &bill.ExhaustEvents, &bill.CarryOverToNextPeriod, &bill.Status, &bill.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	bill.HadOverdraft = hadOverdraftInt != 0
	return &bill, nil
}

func (s *Storage) ListBudgetSettlementBills(query *model.BudgetBillListQuery) ([]model.BudgetSettlementBill, int64, error) {
	if query == nil {
		query = &model.BudgetBillListQuery{Limit: 50}
	}
	if query.Limit <= 0 {
		query.Limit = 50
	}
	if query.Offset < 0 {
		query.Offset = 0
	}

	where := []string{"1=1"}
	args := []interface{}{}

	if query.CallerID != "" {
		where = append(where, "caller_id = ?")
		args = append(args, query.CallerID)
	}

	whereClause := strings.Join(where, " AND ")

	var total int64
	countQuery := "SELECT COUNT(*) FROM lock_budget_settlement_bills WHERE " + whereClause
	if err := s.db.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listQuery := `
		SELECT id, caller_id, period_start_at, period_end_at, budget_limit, overdraft_limit,
			normal_consumption, overdraft_consumption, overdraft_penalty, total_consumption,
			transferred_in, transferred_out, ending_balance, had_overdraft, peak_overdraft,
			peak_concurrent, exhaust_events, carry_over_to_next_period, status, created_at
		FROM lock_budget_settlement_bills WHERE ` + whereClause + `
		ORDER BY period_start_at DESC LIMIT ? OFFSET ?
	`
	args = append(args, query.Limit, query.Offset)

	rows, err := s.db.Query(listQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var bills []model.BudgetSettlementBill
	for rows.Next() {
		var bill model.BudgetSettlementBill
		var hadOverdraftInt int
		if err := rows.Scan(&bill.ID, &bill.CallerID, &bill.PeriodStartAt, &bill.PeriodEndAt, &bill.BudgetLimit, &bill.OverdraftLimit,
			&bill.NormalConsumption, &bill.OverdraftConsumption, &bill.OverdraftPenalty, &bill.TotalConsumption,
			&bill.TransferredIn, &bill.TransferredOut, &bill.EndingBalance, &hadOverdraftInt, &bill.PeakOverdraft,
			&bill.PeakConcurrent, &bill.ExhaustEvents, &bill.CarryOverToNextPeriod, &bill.Status, &bill.CreatedAt); err != nil {
			return nil, 0, err
		}
		bill.HadOverdraft = hadOverdraftInt != 0
		bills = append(bills, bill)
	}
	return bills, total, nil
}

func (s *Storage) AddBudgetBillTransferDetail(detail *model.BudgetBillTransferDetail) error {
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_bill_transfer_details (
			bill_id, caller_id, direction, peer_caller, amount, reason, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, detail.BillID, detail.CallerID, detail.Direction, detail.PeerCaller, detail.Amount, detail.Reason, detail.CreatedAt)
	if err != nil {
		return err
	}
	detail.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListBudgetBillTransferDetails(billID int64) ([]model.BudgetBillTransferDetail, error) {
	rows, err := s.db.Query(`
		SELECT id, bill_id, caller_id, direction, peer_caller, amount, reason, created_at
		FROM lock_budget_bill_transfer_details WHERE bill_id = ? ORDER BY id
	`, billID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var details []model.BudgetBillTransferDetail
	for rows.Next() {
		var d model.BudgetBillTransferDetail
		if err := rows.Scan(&d.ID, &d.BillID, &d.CallerID, &d.Direction, &d.PeerCaller, &d.Amount, &d.Reason, &d.CreatedAt); err != nil {
			return nil, err
		}
		details = append(details, d)
	}
	return details, nil
}

func (s *Storage) CreateBudgetCallerArrears(arrears *model.BudgetCallerArrears) error {
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_caller_arrears (
			caller_id, arrears_amount, original_bill_id, original_period_start_at,
			original_period_end_at, status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			arrears_amount = excluded.arrears_amount,
			original_bill_id = excluded.original_bill_id,
			original_period_start_at = excluded.original_period_start_at,
			original_period_end_at = excluded.original_period_end_at,
			status = excluded.status,
			updated_at = excluded.updated_at
	`, arrears.CallerID, arrears.ArrearsAmount, arrears.OriginalBillID, arrears.OriginalPeriodStartAt,
		arrears.OriginalPeriodEndAt, arrears.Status, arrears.CreatedAt, arrears.UpdatedAt)
	if err != nil {
		return err
	}
	if arrears.ID == 0 {
		arrears.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetBudgetCallerArrears(callerID string) (*model.BudgetCallerArrears, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, arrears_amount, original_bill_id, original_period_start_at,
			original_period_end_at, status, cleared_at, created_at, updated_at
		FROM lock_budget_caller_arrears WHERE caller_id = ? AND status = 'active'
	`, callerID)

	var a model.BudgetCallerArrears
	var clearedAt sql.NullTime
	err := row.Scan(&a.ID, &a.CallerID, &a.ArrearsAmount, &a.OriginalBillID, &a.OriginalPeriodStartAt,
		&a.OriginalPeriodEndAt, &a.Status, &clearedAt, &a.CreatedAt, &a.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if clearedAt.Valid {
		a.ClearedAt = &clearedAt.Time
	}
	return &a, nil
}

func (s *Storage) ListActiveBudgetArrears() ([]model.BudgetCallerArrears, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, arrears_amount, original_bill_id, original_period_start_at,
			original_period_end_at, status, cleared_at, created_at, updated_at
		FROM lock_budget_caller_arrears WHERE status = 'active' ORDER BY created_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var arrearsList []model.BudgetCallerArrears
	for rows.Next() {
		var a model.BudgetCallerArrears
		var clearedAt sql.NullTime
		if err := rows.Scan(&a.ID, &a.CallerID, &a.ArrearsAmount, &a.OriginalBillID, &a.OriginalPeriodStartAt,
			&a.OriginalPeriodEndAt, &a.Status, &clearedAt, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		if clearedAt.Valid {
			a.ClearedAt = &clearedAt.Time
		}
		arrearsList = append(arrearsList, a)
	}
	return arrearsList, nil
}

func (s *Storage) ClearBudgetCallerArrears(callerID string, clearedAt time.Time) error {
	_, err := s.db.Exec(`
		UPDATE lock_budget_caller_arrears
		SET status = 'cleared', cleared_at = ?, updated_at = ?
		WHERE caller_id = ? AND status = 'active'
	`, clearedAt, clearedAt, callerID)
	return err
}

func (s *Storage) RechargeBudget(callerID string, amount int) (*model.BudgetRechargeResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRow(`
		SELECT id, caller_id, budget_limit, period_sec, warning_pct, overdraft_limit, COALESCE(max_concurrent_locks, 0), created_at, updated_at
		FROM lock_budget_configs WHERE caller_id = ?
	`, callerID)

	var cfg model.LockBudgetConfig
	err = row.Scan(&cfg.ID, &cfg.CallerID, &cfg.BudgetLimit, &cfg.PeriodSec, &cfg.WarningPct, &cfg.OverdraftLimit, &cfg.MaxConcurrentLocks, &cfg.CreatedAt, &cfg.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no budget configured for caller: %s", callerID)
	}
	if err != nil {
		return nil, err
	}

	arrearsRow := tx.QueryRow(`
		SELECT id, arrears_amount FROM lock_budget_caller_arrears WHERE caller_id = ? AND status = 'active'
	`, callerID)

	var arrearsID int64
	var arrearsAmount int
	err = arrearsRow.Scan(&arrearsID, &arrearsAmount)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	arrearsCleared := 0
	remainingArrears := 0
	now := time.Now()

	if arrearsAmount > 0 {
		if amount >= arrearsAmount {
			arrearsCleared = arrearsAmount
			remainingArrears = 0
			amount -= arrearsAmount

			_, err = tx.Exec(`
				UPDATE lock_budget_caller_arrears
				SET status = 'cleared', cleared_at = ?, updated_at = ?
				WHERE id = ?
			`, now, now, arrearsID)
			if err != nil {
				return nil, err
			}
		} else {
			arrearsCleared = amount
			remainingArrears = arrearsAmount - amount
			amount = 0

			_, err = tx.Exec(`
				UPDATE lock_budget_caller_arrears
				SET arrears_amount = ?, updated_at = ?
				WHERE id = ?
			`, remainingArrears, now, arrearsID)
			if err != nil {
				return nil, err
			}
		}
	}

	newBudgetLimit := cfg.BudgetLimit + amount
	_, err = tx.Exec(`
		UPDATE lock_budget_configs
		SET budget_limit = ?, updated_at = ?
		WHERE id = ?
	`, newBudgetLimit, now, cfg.ID)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	result := &model.BudgetRechargeResult{
		Success:          true,
		CallerID:         callerID,
		RechargedAmount:  arrearsCleared + amount,
		ArrearsCleared:   arrearsCleared,
		RemainingArrears: remainingArrears,
		NewBudgetLimit:   newBudgetLimit,
	}
	if arrearsCleared > 0 && remainingArrears == 0 {
		result.Message = "充值成功，欠费已全部结清"
	} else if arrearsCleared > 0 {
		result.Message = fmt.Sprintf("充值成功，已抵扣部分欠费，仍欠 %d 单位", remainingArrears)
	} else {
		result.Message = "充值成功"
	}

	return result, nil
}

func (s *Storage) ListBudgetTransfersInPeriod(callerID string, startAt, endAt time.Time) ([]model.BudgetTransferRecord, error) {
	rows, err := s.db.Query(`
		SELECT id, from_caller, to_caller, amount, reason, created_at
		FROM lock_budget_transfers
		WHERE (from_caller = ? OR to_caller = ?)
		  AND created_at >= ? AND created_at < ?
		ORDER BY created_at
	`, callerID, callerID, startAt, endAt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []model.BudgetTransferRecord
	for rows.Next() {
		var r model.BudgetTransferRecord
		if err := rows.Scan(&r.ID, &r.FromCaller, &r.ToCaller, &r.Amount, &r.Reason, &r.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

func (s *Storage) UpsertRateAlertRule(rule *model.LockBudgetRateAlertRule) error {
	enabledInt := 0
	if rule.Enabled {
		enabledInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_rate_alert_rules (
			caller_id, window_sec, max_units_in_window, freeze_trigger_n, enabled, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			window_sec = excluded.window_sec,
			max_units_in_window = excluded.max_units_in_window,
			freeze_trigger_n = excluded.freeze_trigger_n,
			enabled = excluded.enabled,
			updated_at = excluded.updated_at
	`, rule.CallerID, rule.WindowSec, rule.MaxUnitsInWindow, rule.FreezeTriggerN, enabledInt, rule.CreatedAt, rule.UpdatedAt)
	if err != nil {
		return err
	}
	if rule.ID == 0 {
		rule.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) GetRateAlertRule(callerID string) (*model.LockBudgetRateAlertRule, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, window_sec, max_units_in_window, freeze_trigger_n, enabled, created_at, updated_at
		FROM lock_budget_rate_alert_rules WHERE caller_id = ?
	`, callerID)

	var rule model.LockBudgetRateAlertRule
	var enabledInt int
	err := row.Scan(&rule.ID, &rule.CallerID, &rule.WindowSec, &rule.MaxUnitsInWindow, &rule.FreezeTriggerN, &enabledInt, &rule.CreatedAt, &rule.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rule.Enabled = enabledInt != 0
	return &rule, nil
}

func (s *Storage) ListRateAlertRules() ([]model.LockBudgetRateAlertRule, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, window_sec, max_units_in_window, freeze_trigger_n, enabled, created_at, updated_at
		FROM lock_budget_rate_alert_rules ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []model.LockBudgetRateAlertRule
	for rows.Next() {
		var rule model.LockBudgetRateAlertRule
		var enabledInt int
		if err := rows.Scan(&rule.ID, &rule.CallerID, &rule.WindowSec, &rule.MaxUnitsInWindow, &rule.FreezeTriggerN, &enabledInt, &rule.CreatedAt, &rule.UpdatedAt); err != nil {
			return nil, err
		}
		rule.Enabled = enabledInt != 0
		rules = append(rules, rule)
	}
	return rules, nil
}

func (s *Storage) DeleteRateAlertRule(callerID string) error {
	_, err := s.db.Exec(`DELETE FROM lock_budget_rate_alert_rules WHERE caller_id = ?`, callerID)
	return err
}

func (s *Storage) AddRateAlertEvent(e *model.LockBudgetRateAlertEvent) error {
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_rate_alert_events (
			caller_id, window_sec, max_units_in_window, actual_rate, consumed_in_window, detail, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`, e.CallerID, e.WindowSec, e.MaxUnitsInWindow, e.ActualRate, e.ConsumedInWindow, e.Detail, e.CreatedAt)
	if err != nil {
		return err
	}
	e.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListRateAlertEvents(query *model.RateAlertEventListQuery) ([]model.LockBudgetRateAlertEvent, int64, error) {
	if query.Limit <= 0 {
		query.Limit = 50
	}
	if query.Offset < 0 {
		query.Offset = 0
	}

	var total int64
	var rows *sql.Rows
	var err error

	if query.CallerID != "" {
		err = s.db.QueryRow(`
			SELECT COUNT(*) FROM lock_budget_rate_alert_events WHERE caller_id = ?
		`, query.CallerID).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
		rows, err = s.db.Query(`
			SELECT id, caller_id, window_sec, max_units_in_window, actual_rate, consumed_in_window, detail, created_at
			FROM lock_budget_rate_alert_events WHERE caller_id = ?
			ORDER BY id DESC LIMIT ? OFFSET ?
		`, query.CallerID, query.Limit, query.Offset)
	} else {
		err = s.db.QueryRow(`SELECT COUNT(*) FROM lock_budget_rate_alert_events`).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
		rows, err = s.db.Query(`
			SELECT id, caller_id, window_sec, max_units_in_window, actual_rate, consumed_in_window, detail, created_at
			FROM lock_budget_rate_alert_events
			ORDER BY id DESC LIMIT ? OFFSET ?
		`, query.Limit, query.Offset)
	}
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var events []model.LockBudgetRateAlertEvent
	for rows.Next() {
		var e model.LockBudgetRateAlertEvent
		if err := rows.Scan(&e.ID, &e.CallerID, &e.WindowSec, &e.MaxUnitsInWindow, &e.ActualRate, &e.ConsumedInWindow, &e.Detail, &e.CreatedAt); err != nil {
			return nil, 0, err
		}
		events = append(events, e)
	}
	return events, total, nil
}

func (s *Storage) CountRecentRateAlertEvents(callerID string, since time.Time) (int64, error) {
	var count int64
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM lock_budget_rate_alert_events
		WHERE caller_id = ? AND created_at >= ?
		ORDER BY id DESC
	`, callerID, since).Scan(&count)
	return count, err
}

func (s *Storage) FreezeCaller(freeze *model.LockBudgetCallerFreeze) error {
	activeInt := 0
	if freeze.Active {
		activeInt = 1
	}
	result, err := s.db.Exec(`
		INSERT INTO lock_budget_caller_freezes (
			caller_id, frozen_at, frozen_by, reason, alert_count_before,
			unfrozen_at, unfrozen_by, unfreeze_reason, active, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			frozen_at = excluded.frozen_at,
			frozen_by = excluded.frozen_by,
			reason = excluded.reason,
			alert_count_before = excluded.alert_count_before,
			unfrozen_at = excluded.unfrozen_at,
			unfrozen_by = excluded.unfrozen_by,
			unfreeze_reason = excluded.unfreeze_reason,
			active = excluded.active,
			updated_at = excluded.updated_at
	`, freeze.CallerID, freeze.FrozenAt, freeze.FrozenBy, freeze.Reason, freeze.AlertCountBefore,
		nullTimePtr(freeze.UnfrozenAt), freeze.UnfrozenBy, freeze.UnfreezeReason, activeInt,
		freeze.CreatedAt, freeze.UpdatedAt)
	if err != nil {
		return err
	}
	if freeze.ID == 0 {
		freeze.ID, _ = result.LastInsertId()
	}
	return nil
}

func (s *Storage) UnfreezeCaller(callerID string, unfrozenAt time.Time, unfrozenBy string, reason string) error {
	_, err := s.db.Exec(`
		UPDATE lock_budget_caller_freezes SET
			active = 0,
			unfrozen_at = ?,
			unfrozen_by = ?,
			unfreeze_reason = ?,
			updated_at = ?
		WHERE caller_id = ? AND active = 1
	`, unfrozenAt, unfrozenBy, reason, unfrozenAt, callerID)
	return err
}

func (s *Storage) GetActiveFreeze(callerID string) (*model.LockBudgetCallerFreeze, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, frozen_at, frozen_by, reason, alert_count_before,
			unfrozen_at, unfrozen_by, unfreeze_reason, active, created_at, updated_at
		FROM lock_budget_caller_freezes WHERE caller_id = ? AND active = 1
	`, callerID)

	var freeze model.LockBudgetCallerFreeze
	var activeInt int
	var unfrozenAt sql.NullTime
	var unfrozenBy, unfreezeReason sql.NullString
	err := row.Scan(&freeze.ID, &freeze.CallerID, &freeze.FrozenAt, &freeze.FrozenBy, &freeze.Reason,
		&freeze.AlertCountBefore, &unfrozenAt, &unfrozenBy, &unfreezeReason, &activeInt,
		&freeze.CreatedAt, &freeze.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	freeze.Active = activeInt != 0
	if unfrozenAt.Valid {
		freeze.UnfrozenAt = &unfrozenAt.Time
	}
	if unfrozenBy.Valid {
		freeze.UnfrozenBy = unfrozenBy.String
	}
	if unfreezeReason.Valid {
		freeze.UnfreezeReason = unfreezeReason.String
	}
	return &freeze, nil
}

func (s *Storage) ListActiveFreezes() ([]model.LockBudgetCallerFreeze, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, frozen_at, frozen_by, reason, alert_count_before,
			unfrozen_at, unfrozen_by, unfreeze_reason, active, created_at, updated_at
		FROM lock_budget_caller_freezes WHERE active = 1 ORDER BY frozen_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var freezes []model.LockBudgetCallerFreeze
	for rows.Next() {
		var freeze model.LockBudgetCallerFreeze
		var activeInt int
		var unfrozenAt sql.NullTime
		var unfrozenBy, unfreezeReason sql.NullString
		if err := rows.Scan(&freeze.ID, &freeze.CallerID, &freeze.FrozenAt, &freeze.FrozenBy, &freeze.Reason,
			&freeze.AlertCountBefore, &unfrozenAt, &unfrozenBy, &unfreezeReason, &activeInt,
			&freeze.CreatedAt, &freeze.UpdatedAt); err != nil {
			return nil, err
		}
		freeze.Active = activeInt != 0
		if unfrozenAt.Valid {
			freeze.UnfrozenAt = &unfrozenAt.Time
		}
		if unfrozenBy.Valid {
			freeze.UnfrozenBy = unfrozenBy.String
		}
		if unfreezeReason.Valid {
			freeze.UnfreezeReason = unfreezeReason.String
		}
		freezes = append(freezes, freeze)
	}
	return freezes, nil
}

func (s *Storage) CountConsecutiveAlertsSince(callerID string, since time.Time) (int64, error) {
	var count int64
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM lock_budget_rate_alert_events
		WHERE caller_id = ? AND created_at >= ?
	`, callerID, since).Scan(&count)
	return count, err
}

func (s *Storage) UpsertCallerReputation(rep *model.CallerReputation) error {
	_, err := s.db.Exec(`
		INSERT INTO caller_reputations (
			caller_id, score, tier, on_time_release_score, overdraft_reverse_score,
			circuit_breaker_score, arrear_score, rate_alert_score, calculated_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id) DO UPDATE SET
			score = excluded.score,
			tier = excluded.tier,
			on_time_release_score = excluded.on_time_release_score,
			overdraft_reverse_score = excluded.overdraft_reverse_score,
			circuit_breaker_score = excluded.circuit_breaker_score,
			arrear_score = excluded.arrear_score,
			rate_alert_score = excluded.rate_alert_score,
			calculated_at = excluded.calculated_at,
			updated_at = excluded.updated_at
	`, rep.CallerID, rep.Score, rep.Tier, rep.OnTimeReleaseScore, rep.OverdraftReverseScore,
		rep.CircuitBreakerScore, rep.ArrearScore, rep.RateAlertScore, rep.CalculatedAt,
		rep.CreatedAt, rep.UpdatedAt)
	return err
}

func (s *Storage) GetCallerReputation(callerID string) (*model.CallerReputation, error) {
	row := s.db.QueryRow(`
		SELECT id, caller_id, score, tier, on_time_release_score, overdraft_reverse_score,
			circuit_breaker_score, arrear_score, rate_alert_score, calculated_at, created_at, updated_at
		FROM caller_reputations WHERE caller_id = ?
	`, callerID)
	var rep model.CallerReputation
	err := row.Scan(&rep.ID, &rep.CallerID, &rep.Score, &rep.Tier, &rep.OnTimeReleaseScore,
		&rep.OverdraftReverseScore, &rep.CircuitBreakerScore, &rep.ArrearScore, &rep.RateAlertScore,
		&rep.CalculatedAt, &rep.CreatedAt, &rep.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rep, nil
}

func (s *Storage) ListAllCallerReputations() ([]model.CallerReputation, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, score, tier, on_time_release_score, overdraft_reverse_score,
			circuit_breaker_score, arrear_score, rate_alert_score, calculated_at, created_at, updated_at
		FROM caller_reputations ORDER BY score DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var reps []model.CallerReputation
	for rows.Next() {
		var rep model.CallerReputation
		if err := rows.Scan(&rep.ID, &rep.CallerID, &rep.Score, &rep.Tier, &rep.OnTimeReleaseScore,
			&rep.OverdraftReverseScore, &rep.CircuitBreakerScore, &rep.ArrearScore, &rep.RateAlertScore,
			&rep.CalculatedAt, &rep.CreatedAt, &rep.UpdatedAt); err != nil {
			return nil, err
		}
		reps = append(reps, rep)
	}
	return reps, nil
}

func (s *Storage) AddTierChangeEvent(evt *model.TierChangeEvent) error {
	result, err := s.db.Exec(`
		INSERT INTO tier_change_events (caller_id, old_tier, new_tier, score, changed_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, evt.CallerID, evt.OldTier, evt.NewTier, evt.Score, evt.ChangedAt, evt.CreatedAt)
	if err != nil {
		return err
	}
	evt.ID, _ = result.LastInsertId()
	return nil
}

func (s *Storage) ListTierChangeEvents(callerID string, limit int, offset int) ([]model.TierChangeEvent, int64, error) {
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	var total int64
	var rows *sql.Rows
	var err error
	if callerID != "" {
		err = s.db.QueryRow(`SELECT COUNT(*) FROM tier_change_events WHERE caller_id = ?`, callerID).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
		rows, err = s.db.Query(`
			SELECT id, caller_id, old_tier, new_tier, score, changed_at, created_at
			FROM tier_change_events WHERE caller_id = ?
			ORDER BY changed_at DESC LIMIT ? OFFSET ?
		`, callerID, limit, offset)
	} else {
		err = s.db.QueryRow(`SELECT COUNT(*) FROM tier_change_events`).Scan(&total)
		if err != nil {
			return nil, 0, err
		}
		rows, err = s.db.Query(`
			SELECT id, caller_id, old_tier, new_tier, score, changed_at, created_at
			FROM tier_change_events
			ORDER BY changed_at DESC LIMIT ? OFFSET ?
		`, limit, offset)
	}
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var events []model.TierChangeEvent
	for rows.Next() {
		var evt model.TierChangeEvent
		if err := rows.Scan(&evt.ID, &evt.CallerID, &evt.OldTier, &evt.NewTier, &evt.Score, &evt.ChangedAt, &evt.CreatedAt); err != nil {
			return nil, 0, err
		}
		events = append(events, evt)
	}
	return events, total, nil
}

func (s *Storage) CountOnTimeReleases(callerID string, since time.Time) (total int, onTime int, err error) {
	err = s.db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN operation = 'release' AND detail NOT LIKE '%expired%' THEN 1 ELSE 0 END), 0)
		FROM op_history
		WHERE holder = ? AND created_at >= ? AND (operation = 'release' OR operation = 'expire')
	`, callerID, since).Scan(&total, &onTime)
	return
}

func (s *Storage) CountOverdraftPeriods(callerID string, since time.Time) (totalPeriods int, overdraftPeriods int, err error) {
	err = s.db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN overdraft_used > 0 THEN 1 ELSE 0 END), 0)
		FROM lock_budget_period_summaries
		WHERE caller_id = ? AND period_start_at >= ?
	`, callerID, since).Scan(&totalPeriods, &overdraftPeriods)
	return
}

func (s *Storage) CountCircuitBreakerTriggers(callerID string, since time.Time) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM cb_history WHERE caller_id = ? AND triggered_at >= ?
	`, callerID, since).Scan(&count)
	return count, err
}

func (s *Storage) CountArrearEvents(callerID string, since time.Time) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM debt_ledger_events
		WHERE debtor = ? AND event_type = 'overdue_mark' AND created_at >= ?
	`, callerID, since).Scan(&count)
	return count, err
}

func (s *Storage) CountRateAlertEventsSince(callerID string, since time.Time) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM lock_budget_rate_alert_events
		WHERE caller_id = ? AND created_at >= ?
	`, callerID, since).Scan(&count)
	return count, err
}

func (s *Storage) ListBudgetConfigs() ([]model.LockBudgetConfig, error) {
	rows, err := s.db.Query(`
		SELECT id, caller_id, budget_limit, period_sec, warning_pct, overdraft_limit, COALESCE(max_concurrent_locks, 0), created_at, updated_at
		FROM lock_budget_configs ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var configs []model.LockBudgetConfig
	for rows.Next() {
		var cfg model.LockBudgetConfig
		if err := rows.Scan(&cfg.ID, &cfg.CallerID, &cfg.BudgetLimit, &cfg.PeriodSec, &cfg.WarningPct, &cfg.OverdraftLimit, &cfg.MaxConcurrentLocks, &cfg.CreatedAt, &cfg.UpdatedAt); err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	return configs, nil
}

func (s *Storage) CountActiveLeasesByHolder(holder string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM leases WHERE holder = ? AND active = 1`, holder).Scan(&count)
	return count, err
}

func (s *Storage) ReorderWaitQueueForGold(lockName string, goldHolders map[string]bool) error {
	items, err := s.ListWaitQueue(lockName)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	var goldItems, otherItems []model.WaitQueueItem
	for _, item := range items {
		if goldHolders[item.Holder] {
			goldItems = append(goldItems, item)
		} else {
			otherItems = append(otherItems, item)
		}
	}

	if len(goldItems) == 0 || len(otherItems) == 0 {
		return nil
	}

	if err := s.RemoveFromQueueByLock(lockName); err != nil {
		return err
	}

	for _, item := range goldItems {
		if err := s.Enqueue(&item); err != nil {
			return err
		}
	}
	for _, item := range otherItems {
		if err := s.Enqueue(&item); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) RemoveFromQueueByLock(lockName string) error {
	_, err := s.db.Exec(`DELETE FROM wait_queue WHERE lock_name = ?`, lockName)
	return err
}
