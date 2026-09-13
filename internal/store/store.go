package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	// ErrNotFound is returned when a requested record is not found.
	ErrNotFound = errors.New("record not found")
	// ErrStoreClosed is returned when an operation is attempted on a closed store.
	ErrStoreClosed = errors.New("store is closed")
)

// RunRecord represents a workflow run in SQLite.
type RunRecord struct {
	ID              string     `json:"id"`
	SessionID       string     `json:"session_id,omitempty"`
	Goal            string     `json:"goal"`
	Status          string     `json:"status"`
	TotalTokens     int        `json:"total_tokens"`
	CheckpointState string     `json:"checkpoint_state,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

// TaskRecord represents a task execution record in SQLite.
type TaskRecord struct {
	ID               string    `json:"id"`
	RunID            string    `json:"run_id"`
	Action           string    `json:"action"`
	Status           string    `json:"status"`
	RequiresApproval bool      `json:"requires_approval"`
	DependsOn        []string  `json:"depends_on"`
	InputData        string    `json:"input_data,omitempty"`
	OutputData       string    `json:"output_data,omitempty"`
	ToolCallsCount   int       `json:"tool_calls_count"`
	DurationMs       int64     `json:"duration_ms"`
	Tokens           int       `json:"tokens"`
	Error            string    `json:"error,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

// SessionRecord represents a session in SQLite.
type SessionRecord struct {
	ID               string     `json:"id"`
	ProjectPath      string     `json:"project_path"`
	Status           string     `json:"status"`
	Summary          string     `json:"summary,omitempty"`
	PermissionsScope string     `json:"permissions_scope"`
	PrefixHash       string     `json:"prefix_hash,omitempty"`
	CompiledPrefix   string     `json:"compiled_prefix,omitempty"`
	StartedAt        time.Time  `json:"started_at"`
	EndedAt          *time.Time `json:"ended_at,omitempty"`
}

// FactRecord represents an extracted fact in SQLite.
type FactRecord struct {
	ID        string    `json:"id"`
	SessionID string    `json:"session_id,omitempty"`
	Topic     string    `json:"topic"`
	Content   string    `json:"content"`
	Scope     string    `json:"scope"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type writeOp struct {
	ctx     context.Context
	fn      func(db *sql.DB) error
	errChan chan error
}

// Store encapsulates the pure-Go SQLite storage engine with WAL mode
// and a single-writer channel worker to prevent SQLITE_BUSY under heavy concurrency.
type Store struct {
	db      *sql.DB
	writeCh chan writeOp
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// New opens a SQLite database at dbPath, applies WAL and performance PRAGMAs,
// executes initial schema migrations, and launches the single-writer worker.
func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database at %s: %w", dbPath, err)
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA synchronous=NORMAL;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to execute pragma %s: %w", p, err)
		}
	}

	if _, err := db.Exec(Schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	s := &Store{
		db:      db,
		writeCh: make(chan writeOp, 512),
		closeCh: make(chan struct{}),
	}

	s.wg.Add(1)
	go s.writeLoop()

	return s, nil
}

func (s *Store) writeLoop() {
	defer s.wg.Done()
	for op := range s.writeCh {
		if op.ctx != nil && op.ctx.Err() != nil {
			op.errChan <- op.ctx.Err()
			continue
		}
		err := op.fn(s.db)
		op.errChan <- err
	}
}

func (s *Store) execWrite(ctx context.Context, fn func(db *sql.DB) error) error {
	errChan := make(chan error, 1)
	op := writeOp{
		ctx:     ctx,
		fn:      fn,
		errChan: errChan,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return ErrStoreClosed
	case s.writeCh <- op:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// Close gracefully flushes pending write operations and closes the database connection.
func (s *Store) Close() error {
	select {
	case <-s.closeCh:
		return nil
	default:
		close(s.closeCh)
	}

	close(s.writeCh)
	s.wg.Wait()
	return s.db.Close()
}

// CreateRun writes a new run record into SQLite.
func (s *Store) CreateRun(ctx context.Context, run RunRecord) error {
	return s.execWrite(ctx, func(db *sql.DB) error {
		var created any
		if !run.CreatedAt.IsZero() {
			created = run.CreatedAt.UTC().Format(time.RFC3339)
		}
		var finished any
		if run.FinishedAt != nil {
			finished = run.FinishedAt.UTC().Format(time.RFC3339)
		}

		query := `
			INSERT INTO runs (id, session_id, goal, status, total_tokens, checkpoint_state, created_at, finished_at)
			VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
		`
		_, err := db.ExecContext(ctx, query,
			run.ID,
			nullString(run.SessionID),
			run.Goal,
			run.Status,
			run.TotalTokens,
			nullString(run.CheckpointState),
			created,
			finished,
		)
		return err
	})
}

// UpdateRunStatus updates the status, checkpoint_state, total_tokens, and finished_at timestamp of a run.
func (s *Store) UpdateRunStatus(ctx context.Context, runID string, status string, checkpointState string, totalTokens int) error {
	return s.execWrite(ctx, func(db *sql.DB) error {
		query := `
			UPDATE runs
			SET status = ?,
			    checkpoint_state = ?,
			    total_tokens = ?,
			    finished_at = CASE
			        WHEN ? IN ('COMPLETED', 'FAILED', 'FAILED_WITH_CHECKPOINT') AND finished_at IS NULL
			        THEN CURRENT_TIMESTAMP
			        ELSE finished_at
			    END
			WHERE id = ?
		`
		res, err := db.ExecContext(ctx, query, status, nullString(checkpointState), totalTokens, status, runID)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// CreateTasks inserts a batch of task records within a single transaction.
func (s *Store) CreateTasks(ctx context.Context, tasks []TaskRecord) error {
	if len(tasks) == 0 {
		return nil
	}
	return s.execWrite(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO tasks (
				id, run_id, action, status, requires_approval, depends_on,
				input_data, output_data, tool_calls_count, duration_ms, tokens, error, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, t := range tasks {
			depsBytes, err := json.Marshal(t.DependsOn)
			if err != nil {
				return fmt.Errorf("failed to marshal depends_on for task %s: %w", t.ID, err)
			}
			var created any
			if !t.CreatedAt.IsZero() {
				created = t.CreatedAt.UTC().Format(time.RFC3339)
			}

			_, err = stmt.ExecContext(ctx,
				t.ID,
				t.RunID,
				t.Action,
				t.Status,
				t.RequiresApproval,
				string(depsBytes),
				nullString(t.InputData),
				nullString(t.OutputData),
				t.ToolCallsCount,
				t.DurationMs,
				t.Tokens,
				nullString(t.Error),
				created,
			)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	})
}

// UpdateTaskStatus updates a task's status, output_data, and error.
func (s *Store) UpdateTaskStatus(ctx context.Context, taskID string, status string, result json.RawMessage, errStr string) error {
	return s.execWrite(ctx, func(db *sql.DB) error {
		var output any
		if len(result) > 0 {
			output = string(result)
		}
		query := `
			UPDATE tasks
			SET status = ?, output_data = ?, error = ?
			WHERE id = ?
		`
		res, err := db.ExecContext(ctx, query, status, output, nullString(errStr), taskID)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// GetRun reads a single run record by ID.
func (s *Store) GetRun(ctx context.Context, runID string) (*RunRecord, error) {
	query := `
		SELECT id, session_id, goal, status, total_tokens, checkpoint_state, created_at, finished_at
		FROM runs
		WHERE id = ?
	`
	row := s.db.QueryRowContext(ctx, query, runID)

	var run RunRecord
	var sessionID, checkpointState, createdAtStr, finishedAtStr sql.NullString

	err := row.Scan(
		&run.ID,
		&sessionID,
		&run.Goal,
		&run.Status,
		&run.TotalTokens,
		&checkpointState,
		&createdAtStr,
		&finishedAtStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if sessionID.Valid {
		run.SessionID = sessionID.String
	}
	if checkpointState.Valid {
		run.CheckpointState = checkpointState.String
	}
	if t := parseTime(createdAtStr); t != nil {
		run.CreatedAt = *t
	}
	run.FinishedAt = parseTime(finishedAtStr)

	return &run, nil
}

// GetTasks reads all task records for a given runID.
func (s *Store) GetTasks(ctx context.Context, runID string) ([]TaskRecord, error) {
	query := `
		SELECT id, run_id, action, status, requires_approval, depends_on,
		       input_data, output_data, tool_calls_count, duration_ms, tokens, error, created_at
		FROM tasks
		WHERE run_id = ?
		ORDER BY created_at ASC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskRecord
	for rows.Next() {
		var t TaskRecord
		var depsStr, inputData, outputData, errStr, createdAtStr sql.NullString

		if err := rows.Scan(
			&t.ID,
			&t.RunID,
			&t.Action,
			&t.Status,
			&t.RequiresApproval,
			&depsStr,
			&inputData,
			&outputData,
			&t.ToolCallsCount,
			&t.DurationMs,
			&t.Tokens,
			&errStr,
			&createdAtStr,
		); err != nil {
			return nil, err
		}

		if depsStr.Valid && depsStr.String != "" {
			_ = json.Unmarshal([]byte(depsStr.String), &t.DependsOn)
		}
		if inputData.Valid {
			t.InputData = inputData.String
		}
		if outputData.Valid {
			t.OutputData = outputData.String
		}
		if errStr.Valid {
			t.Error = errStr.String
		}
		if parsed := parseTime(createdAtStr); parsed != nil {
			t.CreatedAt = *parsed
		}

		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

// GetTask reads a single task record by ID.
func (s *Store) GetTask(ctx context.Context, taskID string) (*TaskRecord, error) {
	query := `
		SELECT id, run_id, action, status, requires_approval, depends_on,
		       input_data, output_data, tool_calls_count, duration_ms, tokens, error, created_at
		FROM tasks
		WHERE id = ?
	`
	row := s.db.QueryRowContext(ctx, query, taskID)

	var t TaskRecord
	var depsStr, inputData, outputData, errStr, createdAtStr sql.NullString

	if err := row.Scan(
		&t.ID,
		&t.RunID,
		&t.Action,
		&t.Status,
		&t.RequiresApproval,
		&depsStr,
		&inputData,
		&outputData,
		&t.ToolCallsCount,
		&t.DurationMs,
		&t.Tokens,
		&errStr,
		&createdAtStr,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if depsStr.Valid && depsStr.String != "" {
		_ = json.Unmarshal([]byte(depsStr.String), &t.DependsOn)
	}
	if inputData.Valid {
		t.InputData = inputData.String
	}
	if outputData.Valid {
		t.OutputData = outputData.String
	}
	if errStr.Valid {
		t.Error = errStr.String
	}
	if parsed := parseTime(createdAtStr); parsed != nil {
		t.CreatedAt = *parsed
	}

	return &t, nil
}

// ApproveTask records approval for a task and optionally all tasks in the run if scope is session.
// It clears requires_approval and transitions WAITING_APPROVAL tasks back to PENDING.
func (s *Store) ApproveTask(ctx context.Context, runID, taskID string, scope string) error {
	return s.execWrite(ctx, func(db *sql.DB) error {
		var currentRunID, status string
		err := db.QueryRowContext(ctx, "SELECT run_id, status FROM tasks WHERE id = ?", taskID).Scan(&currentRunID, &status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if runID != "" && currentRunID != runID {
			return fmt.Errorf("task %q does not belong to run %q", taskID, runID)
		}

		if scope == "session" {
			_, err = db.ExecContext(ctx, `
				UPDATE tasks
				SET requires_approval = 0,
				    status = CASE WHEN status = 'WAITING_APPROVAL' THEN 'PENDING' ELSE status END
				WHERE run_id = ?
			`, currentRunID)
			if err != nil {
				return err
			}
		} else {
			_, err = db.ExecContext(ctx, `
				UPDATE tasks
				SET requires_approval = 0,
				    status = CASE WHEN status = 'WAITING_APPROVAL' THEN 'PENDING' ELSE status END
				WHERE id = ?
			`, taskID)
			if err != nil {
				return err
			}
		}

		_, err = db.ExecContext(ctx, `
			UPDATE runs
			SET status = 'RUNNING'
			WHERE id = ? AND status = 'WAITING_APPROVAL'
		`, currentRunID)
		return err
	})
}

// CreateSession persists a new session record into SQLite.
func (s *Store) CreateSession(ctx context.Context, sess SessionRecord) error {
	return s.execWrite(ctx, func(db *sql.DB) error {
		var started any
		if !sess.StartedAt.IsZero() {
			started = sess.StartedAt.UTC().Format(time.RFC3339)
		}
		var ended any
		if sess.EndedAt != nil {
			ended = sess.EndedAt.UTC().Format(time.RFC3339)
		}

		query := `
			INSERT INTO sessions (
				id, project_path, status, summary, permissions_scope,
				prefix_hash, compiled_prefix, started_at, ended_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
		`
		_, err := db.ExecContext(ctx, query,
			sess.ID,
			sess.ProjectPath,
			sess.Status,
			nullString(sess.Summary),
			sess.PermissionsScope,
			nullString(sess.PrefixHash),
			nullString(sess.CompiledPrefix),
			started,
			ended,
		)
		return err
	})
}

// GetSession retrieves a session record by ID.
func (s *Store) GetSession(ctx context.Context, sessionID string) (*SessionRecord, error) {
	query := `
		SELECT id, project_path, status, summary, permissions_scope,
		       prefix_hash, compiled_prefix, started_at, ended_at
		FROM sessions
		WHERE id = ?
	`
	row := s.db.QueryRowContext(ctx, query, sessionID)

	var sess SessionRecord
	var summary, prefixHash, compiledPrefix, startedAtStr, endedAtStr sql.NullString

	err := row.Scan(
		&sess.ID,
		&sess.ProjectPath,
		&sess.Status,
		&summary,
		&sess.PermissionsScope,
		&prefixHash,
		&compiledPrefix,
		&startedAtStr,
		&endedAtStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if summary.Valid {
		sess.Summary = summary.String
	}
	if prefixHash.Valid {
		sess.PrefixHash = prefixHash.String
	}
	if compiledPrefix.Valid {
		sess.CompiledPrefix = compiledPrefix.String
	}
	if t := parseTime(startedAtStr); t != nil {
		sess.StartedAt = *t
	}
	sess.EndedAt = parseTime(endedAtStr)

	return &sess, nil
}

// UpdateSessionStatus updates a session's status, summary, and ended_at timestamp.
func (s *Store) UpdateSessionStatus(ctx context.Context, sessionID string, status string, summary string) error {
	return s.execWrite(ctx, func(db *sql.DB) error {
		query := `
			UPDATE sessions
			SET status = ?,
			    summary = CASE WHEN ? IS NOT NULL THEN ? ELSE summary END,
			    ended_at = CASE
			        WHEN ? IN ('ENDED', 'COMPLETED', 'FAILED') AND ended_at IS NULL
			        THEN CURRENT_TIMESTAMP
			        ELSE ended_at
			    END
			WHERE id = ?
		`
		res, err := db.ExecContext(ctx, query, status, nullString(summary), nullString(summary), status, sessionID)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// SaveFact inserts a new fact record or updates an existing one if the ID already exists.
func (s *Store) SaveFact(ctx context.Context, fact FactRecord) error {
	if fact.ID == "" {
		return errors.New("fact id cannot be empty")
	}
	if fact.Scope == "" {
		fact.Scope = "project"
	}
	return s.execWrite(ctx, func(db *sql.DB) error {
		var created any
		if !fact.CreatedAt.IsZero() {
			created = fact.CreatedAt.UTC().Format(time.RFC3339)
		}
		var updated any
		if !fact.UpdatedAt.IsZero() {
			updated = fact.UpdatedAt.UTC().Format(time.RFC3339)
		}

		query := `
			INSERT INTO facts (id, session_id, topic, content, scope, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
			ON CONFLICT(id) DO UPDATE SET
				session_id = excluded.session_id,
				topic = excluded.topic,
				content = excluded.content,
				scope = excluded.scope,
				updated_at = COALESCE(excluded.updated_at, CURRENT_TIMESTAMP)
		`
		_, err := db.ExecContext(ctx, query,
			fact.ID,
			nullString(fact.SessionID),
			fact.Topic,
			fact.Content,
			fact.Scope,
			created,
			updated,
		)
		return err
	})
}

// GetFact retrieves a single fact record by its primary key ID.
func (s *Store) GetFact(ctx context.Context, id string) (*FactRecord, error) {
	query := `
		SELECT id, session_id, topic, content, scope, created_at, updated_at
		FROM facts
		WHERE id = ?
	`
	row := s.db.QueryRowContext(ctx, query, id)

	var fact FactRecord
	var sessionID, createdAtStr, updatedAtStr sql.NullString

	err := row.Scan(
		&fact.ID,
		&sessionID,
		&fact.Topic,
		&fact.Content,
		&fact.Scope,
		&createdAtStr,
		&updatedAtStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if sessionID.Valid {
		fact.SessionID = sessionID.String
	}
	if t := parseTime(createdAtStr); t != nil {
		fact.CreatedAt = *t
	}
	if t := parseTime(updatedAtStr); t != nil {
		fact.UpdatedAt = *t
	}

	return &fact, nil
}

// SearchFacts queries facts filtered optionally by topic (partial match) and scope (exact match).
func (s *Store) SearchFacts(ctx context.Context, topic string, scope string) ([]FactRecord, error) {
	query := `
		SELECT id, session_id, topic, content, scope, created_at, updated_at
		FROM facts
		WHERE 1=1
	`
	var args []any
	if topic != "" {
		query += " AND topic LIKE ?"
		args = append(args, "%"+topic+"%")
	}
	if scope != "" {
		query += " AND scope = ?"
		args = append(args, scope)
	}
	query += " ORDER BY updated_at DESC, created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	facts := make([]FactRecord, 0)
	for rows.Next() {
		var fact FactRecord
		var sessionID, createdAtStr, updatedAtStr sql.NullString
		if err := rows.Scan(
			&fact.ID,
			&sessionID,
			&fact.Topic,
			&fact.Content,
			&fact.Scope,
			&createdAtStr,
			&updatedAtStr,
		); err != nil {
			return nil, err
		}
		if sessionID.Valid {
			fact.SessionID = sessionID.String
		}
		if t := parseTime(createdAtStr); t != nil {
			fact.CreatedAt = *t
		}
		if t := parseTime(updatedAtStr); t != nil {
			fact.UpdatedAt = *t
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}

// ListFactsBySession retrieves all facts created within a specific session.
func (s *Store) ListFactsBySession(ctx context.Context, sessionID string) ([]FactRecord, error) {
	query := `
		SELECT id, session_id, topic, content, scope, created_at, updated_at
		FROM facts
		WHERE session_id = ?
		ORDER BY created_at ASC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, query, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	facts := make([]FactRecord, 0)
	for rows.Next() {
		var fact FactRecord
		var sessID, createdAtStr, updatedAtStr sql.NullString
		if err := rows.Scan(
			&fact.ID,
			&sessID,
			&fact.Topic,
			&fact.Content,
			&fact.Scope,
			&createdAtStr,
			&updatedAtStr,
		); err != nil {
			return nil, err
		}
		if sessID.Valid {
			fact.SessionID = sessID.String
		}
		if t := parseTime(createdAtStr); t != nil {
			fact.CreatedAt = *t
		}
		if t := parseTime(updatedAtStr); t != nil {
			fact.UpdatedAt = *t
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}


func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func parseTime(val sql.NullString) *time.Time {
	if !val.Valid || val.String == "" {
		return nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	} {
		if t, err := time.Parse(layout, val.String); err == nil {
			return &t
		}
	}
	return nil
}
