package logging

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type testLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *testLogger) Debug(args ...interface{})                 { l.record("DEBUG", fmt.Sprint(args...)) }
func (l *testLogger) Debugf(format string, args ...interface{}) { l.record("DEBUG", fmt.Sprintf(format, args...)) }
func (l *testLogger) Info(args ...interface{})                  { l.record("INFO", fmt.Sprint(args...)) }
func (l *testLogger) Infof(format string, args ...interface{})  { l.record("INFO", fmt.Sprintf(format, args...)) }
func (l *testLogger) Error(args ...interface{})                 { l.record("ERROR", fmt.Sprint(args...)) }
func (l *testLogger) Errorf(format string, args ...interface{}) { l.record("ERROR", fmt.Sprintf(format, args...)) }

func (l *testLogger) record(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, level+": "+msg)
}

func (l *testLogger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages)
}

func TestPassthrough(t *testing.T) {
	tl := &testLogger{}
	fl := NewFilteringLogger(tl)

	fl.Debug("d")
	fl.Debugf("df %d", 1)
	fl.Info("i")
	fl.Infof("if %d", 2)
	fl.Error("e")
	fl.Errorf("ef %d", 3)
	if tl.count() != 6 {
		t.Errorf("expected 6 passthrough logs, got %d", tl.count())
	}
}

func TestShouldEmit_DefaultFalse(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	if fl.ShouldEmit("some message") {
		t.Error("expected false by default")
	}
}

func TestShouldEmit_DebugEnabled(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.EnableDebug(true)
	if !fl.ShouldEmit("any message") {
		t.Error("expected true when debug enabled")
	}
}

func TestShouldEmit_TracedClient(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.TraceClient("CP001", true)

	if !fl.ShouldEmit("received JSON message from CP001: [2,...]") {
		t.Error("expected true for traced client")
	}
	if fl.ShouldEmit("received JSON message from CP002: [2,...]") {
		t.Error("expected false for untraced client")
	}
}

func TestShouldEmit_TracedClientDisabled(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.TraceClient("CP001", true)
	fl.TraceClient("CP001", false)
	if fl.ShouldEmit("msg from CP001") {
		t.Error("expected false after untrace")
	}
}

func TestShouldEmit_EventEnabled(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.RegisterEventPattern(EventConnect, "Client connected")
	fl.RegisterEventPattern(EventDisconnect, "Client disconnected")

	// Events not enabled yet
	if fl.ShouldEmit("[Server] Client connected: CP001") {
		t.Error("expected false when event not enabled")
	}

	fl.EnableEvent(EventConnect, true)
	if !fl.ShouldEmit("[Server] Client connected: CP001") {
		t.Error("expected true when connect event enabled")
	}
	if fl.ShouldEmit("[Server] Client disconnected: CP001") {
		t.Error("expected false for disconnect (not enabled)")
	}

	fl.EnableEvent(EventConnect, false)
	if fl.ShouldEmit("[Server] Client connected: CP001") {
		t.Error("expected false after disabling connect event")
	}
}

func TestShouldEmit_MultiplePatterns(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.RegisterEventPattern(EventConnect, "Client connected")
	fl.RegisterEventPattern(EventConnect, "Client connection setup completed")
	fl.EnableEvent(EventConnect, true)

	if !fl.ShouldEmit("[Server] Client connected: CP001") {
		t.Error("expected true for first pattern")
	}
	if !fl.ShouldEmit("[Server] Client connection setup completed: CP001") {
		t.Error("expected true for second pattern")
	}
}

func TestShouldEmit_HealthEvent(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.RegisterEventPattern(EventServerHealth, "[health]")
	fl.EnableEvent(EventServerHealth, true)

	if !fl.ShouldEmit("[health] OCPP gateway alive") {
		t.Error("expected true for health message")
	}
}

func TestSnapshot(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.EnableDebug(true)
	fl.EnableEvent(EventConnect, true)
	fl.TraceClient("CP001", true)

	snap := fl.Snapshot()
	if !snap.DebugEnabled {
		t.Error("expected debug enabled")
	}
	if !snap.EnabledEvents[EventConnect] {
		t.Error("expected connect event enabled")
	}
	if !snap.TracedClients["CP001"] {
		t.Error("expected CP001 traced")
	}

	// Snapshot is a copy
	fl.EnableDebug(false)
	if !snap.DebugEnabled {
		t.Error("snapshot should be a copy")
	}
}

func TestHTTPHandler_Get(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.EnableEvent(EventConnect, true)
	fl.TraceClient("CP001", true)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/loglevel", nil)
	fl.DebugHTTPHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var state FilterState
	json.Unmarshal(w.Body.Bytes(), &state)
	if !state.EnabledEvents[EventConnect] {
		t.Error("expected connect event in response")
	}
	if !state.TracedClients["CP001"] {
		t.Error("expected CP001 in response")
	}
}

func TestHTTPHandler_PostEvent(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.RegisterEventPattern(EventConnect, "Client connected")

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/loglevel", strings.NewReader(`{"event":"connect","enabled":true}`))
	fl.DebugHTTPHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !fl.Snapshot().EnabledEvents[EventConnect] {
		t.Error("expected connect enabled after POST")
	}
}

func TestHTTPHandler_PostDebug(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/loglevel", strings.NewReader(`{"debug":true}`))
	fl.DebugHTTPHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !fl.Snapshot().DebugEnabled {
		t.Error("expected debug enabled after POST")
	}
}

func TestHTTPHandler_PostClient(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/loglevel", strings.NewReader(`{"client":"ENQ88N7PP","enabled":true}`))
	fl.DebugHTTPHandler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !fl.Snapshot().TracedClients["ENQ88N7PP"] {
		t.Error("expected client traced after POST")
	}
}

func TestHTTPHandler_PostInvalid(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/debug/loglevel", strings.NewReader(`{"enabled":true}`))
	fl.DebugHTTPHandler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestConcurrentAccess(t *testing.T) {
	fl := NewFilteringLogger(&VoidLogger{})
	fl.RegisterEventPattern(EventConnect, "Client connected")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			fl.ShouldEmit("[Server] Client connected: CP001")
		}()
		go func() {
			defer wg.Done()
			fl.EnableEvent(EventConnect, true)
		}()
		go func() {
			defer wg.Done()
			fl.TraceClient("CP001", true)
		}()
	}
	wg.Wait()
}
