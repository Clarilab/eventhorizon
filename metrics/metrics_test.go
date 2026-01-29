package metrics

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	eh "github.com/Clarilab/eventhorizon"
	"github.com/Clarilab/eventhorizon/namespace"
	"github.com/Clarilab/eventhorizon/uuid"
	vm "github.com/VictoriaMetrics/metrics"
)

type TestCommand struct {
	ID       uuid.UUID `eh:"label:aggregate_id"`
	Name     string    `eh:"label:cmd_name"`
	Internal string    `eh:"nolabel"`
	Category string
	private  string
}

var _ = eh.Command(TestCommand{})

func (c TestCommand) AggregateID() uuid.UUID          { return c.ID }
func (c TestCommand) AggregateType() eh.AggregateType { return "test_aggregate" }
func (c TestCommand) CommandType() eh.CommandType     { return "test_command" }

type TestEventData struct {
	UserID   string `eh:"label:user_id"`
	Secret   string `eh:"nolabel"`
	Category string
	Status   string `eh:"label:event_status"`
	internal int
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

	if err := EnableMetrics("test_service"); err != nil {
		t.Fatalf("failed to enable metrics: %v", err)
	}

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
		`workload="test_service"`,
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

	if err := EnableMetrics("test_service"); err != nil {
		t.Fatalf("failed to enable metrics: %v", err)
	}

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
		`workload="test_service"`,
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

	if err := EnableMetrics(""); err == nil {
		t.Error("expected error for empty workload")
	}

	if err := EnableMetrics("my_service"); err != nil {
		t.Fatalf("failed to enable metrics: %v", err)
	}

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
		// Setup workload but DON'T start background goroutine
		workload = "queue_only_benchmark"

		b.ResetTimer()
		for b.Loop() {
			// Direct call to queue - this is the hot path
			queue(ctx, "command", "test_command", cmd)
		}
	})

	b.Run("MiddlewareOnly", func(b *testing.B) {
		// Setup middleware but DON'T start background goroutine
		workload = "middleware_only_benchmark"
		SetCommandMiddleware(NewCommandHandlerMiddleware())
		SetEventMiddleware(NewEventHandlerMiddleware())

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

		if err := EnableMetrics("full_system_benchmark"); err != nil {
			b.Fatalf("failed to enable metrics: %v", err)
		}

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
