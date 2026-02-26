package metrics

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	eh "github.com/Clarilab/eventhorizon"
	"github.com/Clarilab/eventhorizon/namespace"
	"github.com/Clarilab/eventhorizon/uuid"
	vm "github.com/VictoriaMetrics/metrics"
)

type TestCommand struct {
	ID       uuid.UUID `metrics:"label:aggregate_id"`
	Name     string    `metrics:"label:cmd_name"`
	Internal string    `metrics:"exclude"`
	Category string
	private  string
}

var _ = eh.Command(TestCommand{})

func (c TestCommand) AggregateID() uuid.UUID          { return c.ID }
func (c TestCommand) AggregateType() eh.AggregateType { return "test_aggregate" }
func (c TestCommand) CommandType() eh.CommandType     { return "test_command" }

type TestEventData struct {
	UserID   string `metrics:"label:user_id"`
	Secret   string `metrics:"exclude"`
	Category string
	Status   string `metrics:"label:event_status"`
	internal int
}

// NestedStruct tests one-level nested struct flattening.
type NestedInner struct {
	InnerValue string `metrics:"label:inner_val"`
	Secret     string `metrics:"exclude"` // should be excluded
	private    string // private, ignored
}

type NestedOuter struct {
	OuterValue string
	Inner      NestedInner `bson:",inline"` // bson inline tag to flatten
}

// CollisionStruct tests that duplicate field names are ignored.
type CollisionInner struct {
	Name  string // collides with Outer.Name
	Extra string
}

type CollisionOuter struct {
	Name  string
	Inner CollisionInner `bson:",inline"` // bson inline tag to flatten
}

// TimeStruct tests that time.Time fields are excluded.
type TimeInner struct {
	Module string
}

type TimeOuter struct {
	Name      string
	Timestamp time.Time
	Inner     TimeInner `metrics:"include"`
}

// DoubleFlattenStruct tests flattening two structs.
type FirstInner struct {
	FirstValue string
}

type SecondInner struct {
	SecondValue string
}

type DoubleFlattenOuter struct {
	Name   string
	First  FirstInner  `metrics:"include"`
	Second SecondInner `metrics:"include"`
}

// IncludeOnNonStruct tests that metrics:include on non-struct fields doesn't break.
type IncludeOnNonStruct struct {
	Value    string `metrics:"include"` // include on string - should work normally
	Category string
}

type TestEventHandler struct {
	handlerType eh.EventHandlerType
}

var _ = eh.EventHandler(&TestEventHandler{})

func (h *TestEventHandler) HandlerType() eh.EventHandlerType                      { return h.handlerType }
func (h *TestEventHandler) HandleEvent(ctx context.Context, event eh.Event) error { return nil }

type TestCommandHandler struct{}

var _ = eh.CommandHandler(&TestCommandHandler{})

func (h *TestCommandHandler) HandleCommand(ctx context.Context, cmd eh.Command) error { return nil }

func cleanupMetrics() {
	vm.UnregisterAllMetrics()
	metricsCommandHandlerMiddleware = nil
	metricsEventHandlerMiddleware = nil
	enableOnce = sync.Once{}
}

func TestExtractLabels(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected map[string]string
	}{
		{
			name: "command with labels",
			input: TestCommand{
				ID:       uuid.Nil,
				Name:     "create_user",
				Internal: "secret",
				Category: "user_management",
				private:  "ignored",
			},
			expected: map[string]string{
				"aggregate_id": "00000000-0000-0000-0000-000000000000",
				"cmd_name":     "create_user",
				"category":     "user_management",
			},
		},
		{
			name: "event data with labels",
			input: TestEventData{
				UserID:   "user_123",
				Secret:   "should_be_ignored",
				Category: "test_category",
				Status:   "completed",
				internal: 42,
			},
			expected: map[string]string{
				"user_id":      "user_123",
				"category":     "test_category",
				"event_status": "completed",
			},
		},
		{name: "nil input", input: nil, expected: map[string]string{}},
		{name: "empty struct", input: struct{}{}, expected: map[string]string{}},
		{
			name: "nested struct one level",
			input: NestedOuter{
				OuterValue: "outer_val",
				Inner: NestedInner{
					InnerValue: "inner_val",
					Secret:     "should_be_excluded",
					private:    "private_ignored",
				},
			},
			expected: map[string]string{
				"outer_value": "outer_val",
				"inner_val":   "inner_val",
			},
		},
		{
			name: "collision nested wins",
			input: CollisionOuter{
				Name: "outer_name",
				Inner: CollisionInner{
					Name:  "inner_name", // duplicate, wins over outer
					Extra: "extra_val",
				},
			},
			expected: map[string]string{
				"name":  "inner_name", // nested wins (last write)
				"extra": "extra_val",
			},
		},
		{
			name: "time fields excluded",
			input: TimeOuter{
				Name:      "test_name",
				Timestamp: time.Date(2026, 2, 25, 13, 10, 16, 0, time.UTC),
				Inner: TimeInner{
					Module: "test_module",
				},
			},
			expected: map[string]string{
				"name":   "test_name",
				"module": "test_module",
				// timestamp should NOT be included
			},
		},
		{
			name: "flatten two structs",
			input: DoubleFlattenOuter{
				Name:   "outer_name",
				First:  FirstInner{FirstValue: "first_val"},
				Second: SecondInner{SecondValue: "second_val"},
			},
			expected: map[string]string{
				"name":         "outer_name",
				"first_value":  "first_val",
				"second_value": "second_val",
			},
		},
		{
			name: "include on non-struct field works normally",
			input: IncludeOnNonStruct{
				Value:    "test_value",
				Category: "test_category",
			},
			expected: map[string]string{
				"value":    "test_value",
				"category": "test_category",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractLabels(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d labels, got %d", len(tt.expected), len(result))
			}
			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("label %q: expected %q, got %q", k, v, result[k])
				}
			}
		})
	}
}

func TestCommandMiddleware(t *testing.T) {
	defer cleanupMetrics()

	EnableMetrics()

	middleware := NewCommandHandlerMiddleware()
	wrappedHandler := middleware(&TestCommandHandler{})

	ctx := namespace.NewContext(context.Background(), "tenant_123")
	cmd := TestCommand{
		ID:       uuid.Nil,
		Name:     "test_cmd",
		Internal: "should_be_ignored",
		Category: "test",
	}

	if err := wrappedHandler.HandleCommand(ctx, cmd); err != nil {
		t.Fatalf("command handler failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	var buf bytes.Buffer
	vm.WritePrometheus(&buf, false)
	output := buf.String()

	checks := []string{
		`kycnow_eventsourcing_total{`,
		`tenant="tenant_123"`,
		`handler_type="command"`,
		`action="test_command"`,
		`cmd_name="test_cmd"`,
		`category="test"`,
	}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("missing %q in output", check)
		}
	}

	if strings.Contains(output, "Internal") {
		t.Error("output should not contain Internal field (nolabel)")
	}
}

func TestEventMiddleware(t *testing.T) {
	defer cleanupMetrics()

	EnableMetrics()

	middleware := NewEventHandlerMiddleware()
	wrappedHandler := middleware(&TestEventHandler{handlerType: "test_projector"})

	ctx := namespace.NewContext(context.Background(), "tenant_456")
	eventData := TestEventData{
		UserID:   "user_456",
		Secret:   "should_be_ignored",
		Category: "user_events",
		Status:   "processed",
	}
	event := eh.NewEvent("user_created", eventData, time.Now(), eh.ForAggregate("test_aggregate", uuid.Nil, 1))

	if err := wrappedHandler.HandleEvent(ctx, event); err != nil {
		t.Fatalf("event handler failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	var buf bytes.Buffer
	vm.WritePrometheus(&buf, false)
	output := buf.String()

	checks := []string{
		`kycnow_eventsourcing_total{`,
		`tenant="tenant_456"`,
		`handler_type="event"`,
		`action="user_created"`,
		`user_id="user_456"`,
		`category="user_events"`,
	}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Errorf("missing %q in output", check)
		}
	}

	if strings.Contains(output, "Secret") {
		t.Error("output should not contain Secret field (nolabel)")
	}
}

func TestEnableMetrics(t *testing.T) {
	defer cleanupMetrics()

	EnableMetrics()

	if GetCommandMiddleware() == nil {
		t.Error("command middleware should be registered")
	}
	if GetEventMiddleware() == nil {
		t.Error("event middleware should be registered")
	}
}

// noopCommandMiddleware returns a middleware that just passes through to the handler.
// Used to measure the overhead of middleware wrapping itself.
func noopCommandMiddleware() eh.CommandHandlerMiddleware {
	return func(h eh.CommandHandler) eh.CommandHandler {
		return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
			return h.HandleCommand(ctx, cmd)
		})
	}
}

// noopEventMiddleware returns a middleware that just passes through to the handler.
// Used to measure the overhead of middleware wrapping itself.
func noopEventMiddleware() eh.EventHandlerMiddleware {
	return func(h eh.EventHandler) eh.EventHandler {
		return eh.EventHandlerFunc(func(ctx context.Context, event eh.Event) error {
			return h.HandleEvent(ctx, event)
		})
	}
}

// BenchmarkBaseline measures the baseline performance without metrics.
func BenchmarkBaseline(b *testing.B) {
	defer cleanupMetrics()

	ctx := namespace.NewContext(context.Background(), "tenant_benchmark")
	cmd := TestCommand{
		ID:       uuid.Nil,
		Name:     "benchmark_cmd",
		Internal: "ignored",
		Category: "benchmark",
	}
	eventData := TestEventData{
		UserID:   "bench_user",
		Secret:   "ignored",
		Category: "benchmark",
		Status:   "benchmarking",
	}
	event := eh.NewEvent("benchmark_event", eventData, time.Now(), eh.ForAggregate("benchmark_aggregate", uuid.Nil, 1))

	b.Run("NoMiddleware", func(b *testing.B) {
		cmdHandler := &TestCommandHandler{}
		eventHandler := &TestEventHandler{handlerType: "benchmark_handler"}

		b.ResetTimer()
		for b.Loop() {
			if err := cmdHandler.HandleCommand(ctx, cmd); err != nil {
				b.Fatalf("command failed: %v", err)
			}
			if err := eventHandler.HandleEvent(ctx, event); err != nil {
				b.Fatalf("event failed: %v", err)
			}
		}
	})

	b.Run("EmptyMiddleware", func(b *testing.B) {
		cmdMiddleware := noopCommandMiddleware()
		eventMiddleware := noopEventMiddleware()
		cmdHandler := cmdMiddleware(&TestCommandHandler{})
		eventHandler := eventMiddleware(&TestEventHandler{handlerType: "benchmark_handler"})

		b.ResetTimer()
		for b.Loop() {
			if err := cmdHandler.HandleCommand(ctx, cmd); err != nil {
				b.Fatalf("command failed: %v", err)
			}
			if err := eventHandler.HandleEvent(ctx, event); err != nil {
				b.Fatalf("event failed: %v", err)
			}
		}
	})
}

// BenchmarkMetrics measures metrics performance at different levels.
func BenchmarkMetrics(b *testing.B) {
	defer cleanupMetrics()

	ctx := namespace.NewContext(context.Background(), "tenant_benchmark")
	cmd := TestCommand{
		ID:       uuid.Nil,
		Name:     "benchmark_cmd",
		Internal: "ignored",
		Category: "benchmark",
	}
	eventData := TestEventData{
		UserID:   "bench_user",
		Secret:   "ignored",
		Category: "benchmark",
		Status:   "benchmarking",
	}
	event := eh.NewEvent("benchmark_event", eventData, time.Now(), eh.ForAggregate("benchmark_aggregate", uuid.Nil, 1))

	b.Run("QueueOnly", func(b *testing.B) {
		// Setup but DON'T start background goroutine

		b.ResetTimer()
		for b.Loop() {
			// Direct call to queue - this is the hot path
			queue(ctx, "command", "test_command", true, cmd)
		}
	})

	b.Run("MiddlewareOnly", func(b *testing.B) {
		// Setup middleware but DON'T start background goroutine
		setCommandMiddleware(NewCommandHandlerMiddleware())
		setEventMiddleware(NewEventHandlerMiddleware())

		baseCmdHandler := &TestCommandHandler{}
		baseEventHandler := &TestEventHandler{handlerType: "benchmark_handler"}

		// Wrap with metrics middleware
		cmdHandler := GetCommandMiddleware()(baseCmdHandler)
		eventHandler := GetEventMiddleware()(baseEventHandler)

		b.ResetTimer()
		for b.Loop() {
			if err := cmdHandler.HandleCommand(ctx, cmd); err != nil {
				b.Fatalf("command failed: %v", err)
			}
			if err := eventHandler.HandleEvent(ctx, event); err != nil {
				b.Fatalf("event failed: %v", err)
			}
		}
	})

	b.Run("FullSystem", func(b *testing.B) {
		cleanupMetrics()

		EnableMetrics()

		cmdHandler := NewCommandHandlerMiddleware()(&TestCommandHandler{})
		eventHandler := NewEventHandlerMiddleware()(&TestEventHandler{handlerType: "benchmark_handler"})

		b.ResetTimer()
		for b.Loop() {
			if err := cmdHandler.HandleCommand(ctx, cmd); err != nil {
				b.Fatalf("command failed: %v", err)
			}
			if err := eventHandler.HandleEvent(ctx, event); err != nil {
				b.Fatalf("event failed: %v", err)
			}
		}
	})
}
