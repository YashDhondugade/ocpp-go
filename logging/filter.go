package logging

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
)

// FilterState is a snapshot of the current filter configuration.
type FilterState struct {
	DebugEnabled  bool            `json:"debug_enabled"`
	EnabledEvents map[Event]bool  `json:"enabled_events"`
	TracedClients map[string]bool `json:"traced_clients"`
}

// FilteringLogger wraps an underlying Logger and holds runtime-controllable
// filter state. It implements the Logger interface as a pure passthrough —
// all log calls are forwarded unconditionally to the underlying logger so
// that logrus hooks (e.g. SQS pipeline) always fire.
//
// The ShouldEmit method is used by application-level hooks (e.g. ConsoleHook)
// to decide whether a log entry should be printed to console/CloudWatch.
//
// Filtering is based on three mechanisms:
//   - Global debug toggle: when enabled, everything prints
//   - Per-client tracing: logs containing a traced client ID print
//   - Event categories: logs matching a registered event pattern print when that event is enabled
type FilteringLogger struct {
	underlying    Logger
	mu            sync.RWMutex
	debugEnabled  bool
	tracedClients map[string]bool
	enabledEvents map[Event]bool
	eventPatterns map[Event][]string // event -> list of message substrings
}

// NewFilteringLogger creates a FilteringLogger wrapping the given Logger.
func NewFilteringLogger(underlying Logger) *FilteringLogger {
	return &FilteringLogger{
		underlying:    underlying,
		tracedClients: make(map[string]bool),
		enabledEvents: make(map[Event]bool),
		eventPatterns: make(map[Event][]string),
	}
}

// Logger interface — pure passthrough so all logrus hooks fire.

func (fl *FilteringLogger) Debug(args ...interface{})                 { fl.underlying.Debug(args...) }
func (fl *FilteringLogger) Debugf(format string, args ...interface{}) { fl.underlying.Debugf(format, args...) }
func (fl *FilteringLogger) Info(args ...interface{})                  { fl.underlying.Info(args...) }
func (fl *FilteringLogger) Infof(format string, args ...interface{})  { fl.underlying.Infof(format, args...) }
func (fl *FilteringLogger) Error(args ...interface{})                 { fl.underlying.Error(args...) }
func (fl *FilteringLogger) Errorf(format string, args ...interface{}) { fl.underlying.Errorf(format, args...) }

// RegisterEventPattern associates a message substring with an event category.
// When the event is enabled, any log message containing the pattern will print.
// Call this at application init time to map library log messages to events.
func (fl *FilteringLogger) RegisterEventPattern(e Event, pattern string) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.eventPatterns[e] = append(fl.eventPatterns[e], pattern)
}

// EnableEvent toggles an event category on or off.
func (fl *FilteringLogger) EnableEvent(e Event, on bool) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if on {
		fl.enabledEvents[e] = true
	} else {
		delete(fl.enabledEvents, e)
	}
}

// EnableDebug toggles global debug logging on or off.
func (fl *FilteringLogger) EnableDebug(on bool) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.debugEnabled = on
}

// TraceClient toggles per-chargepoint tracing on or off.
func (fl *FilteringLogger) TraceClient(id string, on bool) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if on {
		fl.tracedClients[id] = true
	} else {
		delete(fl.tracedClients, id)
	}
}

// ShouldEmit checks whether a log entry should be printed to console output.
// Returns true if:
//   - debugEnabled (global toggle), OR
//   - any traced client ID is found in the message, OR
//   - the message matches a pattern of an enabled event
//
// Error-level entries should bypass this check (always print).
func (fl *FilteringLogger) ShouldEmit(message string) bool {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	if fl.debugEnabled {
		return true
	}
	// Check traced clients
	for id := range fl.tracedClients {
		if strings.Contains(message, id) {
			return true
		}
	}
	// Check enabled events
	for event := range fl.enabledEvents {
		for _, pattern := range fl.eventPatterns[event] {
			if strings.Contains(message, pattern) {
				return true
			}
		}
	}
	return false
}

// Snapshot returns a copy of the current filter state.
func (fl *FilteringLogger) Snapshot() FilterState {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	events := make(map[Event]bool, len(fl.enabledEvents))
	for k, v := range fl.enabledEvents {
		events[k] = v
	}
	clients := make(map[string]bool, len(fl.tracedClients))
	for k, v := range fl.tracedClients {
		clients[k] = v
	}
	return FilterState{
		DebugEnabled:  fl.debugEnabled,
		EnabledEvents: events,
		TracedClients: clients,
	}
}

// DebugHTTPHandler returns an http.Handler for the /debug/loglevel endpoint.
//
// GET returns the current filter state as JSON.
// POST accepts a JSON body:
//
//	{"debug":true}                          — toggle global debug
//	{"event":"connect","enabled":true}      — toggle an event category
//	{"client":"ENQ88N7PP","enabled":true}   — toggle per-chargepoint trace
func (fl *FilteringLogger) DebugHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			fl.handleGet(w)
		case http.MethodPost:
			fl.handlePost(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func (fl *FilteringLogger) handleGet(w http.ResponseWriter) {
	snap := fl.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}

type toggleRequest struct {
	Debug   *bool  `json:"debug,omitempty"`
	Event   Event  `json:"event,omitempty"`
	Client  string `json:"client,omitempty"`
	Enabled bool   `json:"enabled"`
}

func (fl *FilteringLogger) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req toggleRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Debug == nil && req.Event == "" && req.Client == "" {
		http.Error(w, "must specify \"debug\", \"event\", or \"client\"", http.StatusBadRequest)
		return
	}

	if req.Debug != nil {
		fl.EnableDebug(*req.Debug)
	}
	if req.Event != "" {
		fl.EnableEvent(req.Event, req.Enabled)
	}
	if req.Client != "" {
		fl.TraceClient(req.Client, req.Enabled)
	}

	fl.handleGet(w)
}
