package metrics

import (
	"context"
	"fmt"

	eh "github.com/Clarilab/eventhorizon"
)

func NewCommandHandlerMiddleware(r recorder) eh.CommandHandlerMiddleware {
	return func(h eh.CommandHandler) eh.CommandHandler {
		return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
			err := h.HandleCommand(ctx, cmd)

			if r != nil {
				r.RecordHandlerExecution(ctx, metrics{
					HandlerType: "command",
					Action:      string(cmd.CommandType()),
					Labels:      extractLabels(cmd),
					Error:       err,
				})
			}

			if err != nil {
				return fmt.Errorf("could not handle command: %w", err)
			}

			return nil
		})
	}
}

func NewEventHandlerMiddleware(r recorder) eh.EventHandlerMiddleware {
	return func(h eh.EventHandler) eh.EventHandler {
		return &eventHandler{
			EventHandler: h,
			recorder:     r,
		}
	}
}

type eventHandler struct {
	eh.EventHandler
	recorder recorder
}

func (h *eventHandler) InnerHandler() eh.EventHandler {
	return h.EventHandler
}

func (h *eventHandler) HandleEvent(ctx context.Context, event eh.Event) error {
	err := h.EventHandler.HandleEvent(ctx, event)

	if h.recorder != nil {
		h.recorder.RecordHandlerExecution(ctx, metrics{
			Name:        string(h.HandlerType()),
			HandlerType: "event",
			Action:      string(event.EventType()),
			Labels:      extractLabels(event.Data()),
			Error:       err,
		})
	}

	if err != nil {
		return fmt.Errorf("could not handle event: %w", err)
	}

	return nil
}
