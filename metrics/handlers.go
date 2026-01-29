package metrics

import (
	"context"
	"fmt"

	eh "github.com/Clarilab/eventhorizon"
)

func NewCommandHandlerMiddleware() eh.CommandHandlerMiddleware {
	return func(h eh.CommandHandler) eh.CommandHandler {
		return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
			err := h.HandleCommand(ctx, cmd)

			queue(ctx, "command", string(cmd.CommandType()), cmd)

			if err != nil {
				return fmt.Errorf("could not handle command: %w", err)
			}

			return nil
		})
	}
}

func NewEventHandlerMiddleware() eh.EventHandlerMiddleware {
	return func(h eh.EventHandler) eh.EventHandler {
		return eh.EventHandlerFunc(func(ctx context.Context, event eh.Event) error {
			err := h.HandleEvent(ctx, event)

			queue(ctx, "event", string(event.EventType()), event.Data())

			if err != nil {
				return fmt.Errorf("could not handle event: %w", err)
			}

			return nil
		})
	}
}
