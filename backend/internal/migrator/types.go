package migrator

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"pg-migrator/internal/config"
	"pg-migrator/internal/store"
)

var validIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

func ValidateIdentifier(name string) error {
	if !validIdentRe.MatchString(name) {
		return fmt.Errorf("invalid identifier %q: must match ^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", name)
	}
	return nil
}

type CheckItem struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // pass | warn | fail
	Detail  string `json:"detail"`
	FixSQL  string `json:"fix_sql,omitempty"`
}

type PreflightReport struct {
	CheckedAt         time.Time   `json:"checked_at"`
	Passed            bool        `json:"passed"`
	SourceVersion     string      `json:"source_version"`
	TargetVersion     string      `json:"target_version"`
	SourceSizeBytes   int64       `json:"source_size_bytes"`
	TableCount        int         `json:"table_count"`
	SequenceCount     int         `json:"sequence_count"`
	TablesWithoutPK   []string    `json:"tables_without_pk"`
	PartitionedTables []string    `json:"partitioned_tables"`
	UnloggedTables    []string    `json:"unlogged_tables"`
	UserTriggers      []string    `json:"user_triggers"`
	Checks            []CheckItem `json:"checks"`
}

type TableSyncState struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	State         string `json:"state"`       // i, d, f, s, r
	StateLabel    string `json:"state_label"` // Init, Copying, Finished Copy, Synced, Streaming
	TargetBytes   int64  `json:"target_bytes"`
	EstRows       int64  `json:"est_rows"`
}

type VerifyTableResult struct {
	Schema         string `json:"schema"`
	Table          string `json:"table"`
	SourceRows     int64  `json:"source_rows"`
	TargetRows     int64  `json:"target_rows"`
	RowDiff        int64  `json:"row_diff"`
	SampleMatch    bool   `json:"sample_match"`
	Match          bool   `json:"match"`
	DurationMs     int64  `json:"duration_ms"`
}

type VerifySequenceResult struct {
	Schema      string `json:"schema"`
	Sequence    string `json:"sequence"`
	SourceValue int64  `json:"source_value"`
	TargetValue int64  `json:"target_value"`
	Drift       int64  `json:"drift"`
}

type VerifyReport struct {
	VerifiedAt time.Time              `json:"verified_at"`
	AllMatched bool                   `json:"all_matched"`
	Tables     []VerifyTableResult    `json:"tables"`
	Sequences  []VerifySequenceResult `json:"sequences"`
}

type CutoverRequest struct {
	UsePgBouncer       bool `json:"use_pgbouncer"`
	SetSourceReadOnly  bool `json:"set_source_readonly"`
	ApplyDeferredFKs   bool `json:"apply_deferred_fks"`
	SequenceBuffer     int64 `json:"sequence_buffer"`
	MaxWaitLagSeconds  int  `json:"max_wait_lag_seconds"`
}

type CutoverStep struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	DurationMs int64  `json:"duration_ms"`
	Detail     string `json:"detail"`
}

type CutoverReport struct {
	CompletedAt     time.Time     `json:"completed_at"`
	DowntimeMs      int64         `json:"downtime_ms"`
	SequencesSynced int           `json:"sequences_synced"`
	FKsApplied      int           `json:"fks_applied"`
	Steps           []CutoverStep `json:"steps"`
}

type Engine struct {
	store *store.Store
	cfg   *config.Config
	locks *JobLocker

	mu          sync.Mutex
	monitors    map[string]context.CancelFunc
	subscribers map[string]map[chan []byte]struct{}
}

func NewEngine(st *store.Store, cfg *config.Config) *Engine {
	return &Engine{
		store:       st,
		cfg:         cfg,
		locks:       NewJobLocker(),
		monitors:    make(map[string]context.CancelFunc),
		subscribers: make(map[string]map[chan []byte]struct{}),
	}
}

func (e *Engine) TryLockJob(jobID, op string) (func(), error) {
	return e.locks.TryAcquire(jobID, op)
}

func (e *Engine) Shutdown() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, cancel := range e.monitors {
		cancel()
		delete(e.monitors, id)
	}
}

func (e *Engine) Subscribe(jobID string) (chan []byte, func()) {
	ch := make(chan []byte, 16)
	e.mu.Lock()
	if _, ok := e.subscribers[jobID]; !ok {
		e.subscribers[jobID] = make(map[chan []byte]struct{})
	}
	e.subscribers[jobID][ch] = struct{}{}
	e.mu.Unlock()

	unsub := func() {
		e.mu.Lock()
		if subs, ok := e.subscribers[jobID]; ok {
			delete(subs, ch)
			if len(subs) == 0 {
				delete(e.subscribers, jobID)
			}
		}
		e.mu.Unlock()
	}
	return ch, unsub
}

func (e *Engine) broadcast(jobID string, payload []byte) {
	e.mu.Lock()
	subs := e.subscribers[jobID]
	for ch := range subs {
		select {
		case ch <- payload:
		default:
			// slow consumer, skip frame
		}
	}
	e.mu.Unlock()
}

func buildConnURL(host string, port int, db, user, password, sslmode string) string {
	if sslmode == "" {
		sslmode = "prefer"
	}
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + db,
	}
	q := u.Query()
	q.Set("sslmode", sslmode)
	q.Set("connect_timeout", "8")
	u.RawQuery = q.Encode()
	return u.String()
}

func connectSource(ctx context.Context, j *store.Job) (*pgx.Conn, error) {
	dsn := buildConnURL(j.SourceHost, j.SourcePort, j.SourceDB, j.SourceUser, j.SourcePassword, j.SourceSSLMode)
	return pgx.Connect(ctx, dsn)
}

func connectTarget(ctx context.Context, j *store.Job) (*pgx.Conn, error) {
	dsn := buildConnURL(j.TargetHost, j.TargetPort, j.TargetDB, j.TargetUser, j.TargetPassword, j.TargetSSLMode)
	return pgx.Connect(ctx, dsn)
}

func (e *Engine) log(ctx context.Context, jobID, phase, level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	_ = e.store.AppendLog(ctx, jobID, phase, level, msg)
}
