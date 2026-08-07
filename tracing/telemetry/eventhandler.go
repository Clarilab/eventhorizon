package telemetry

import (
	"context"
	"fmt"

	eh "github.com/Clarilab/eventhorizon"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// NewEventHandlerMiddleware returns an event handler middleware that adds tracing spans.
func NewEventHandlerMiddleware() eh.EventHandlerMiddleware {
	return eh.EventHandlerMiddleware(func(h eh.EventHandler) eh.EventHandler {
		return &eventHandler{EventHandler: h, Tracer: otel.Tracer("eventhorizon")}
	})
}

type eventHandler struct {
	eh.EventHandler
	Tracer trace.Tracer
}

// InnerHandler implements MiddlewareChain
func (h *eventHandler) InnerHandler() eh.EventHandler {
	return h.EventHandler
}

// HandleEvent implements the HandleEvent method of the EventHandler.
func (h *eventHandler) HandleEvent(ctx context.Context, event eh.Event) error {
	opName := fmt.Sprintf("%s.Event(%s)", h.HandlerType(), event.EventType())
	ctx, span := h.Tracer.Start(ctx, opName)
	defer span.End()

	err := h.EventHandler.HandleEvent(ctx, event)
	if err != nil {
		span.RecordError(err)
	}

	span.SetAttributes(
		attribute.String("eh.event_type", event.EventType().String()),
		attribute.String("eh.aggregate_type", event.AggregateType().String()),
		attribute.String("eh.aggregate_id", event.AggregateID().String()),
		attribute.Int("eh.event_version", event.Version()),
	)

	return err
}
