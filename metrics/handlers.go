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

			successful := err == nil
			queue(ctx, "command", string(cmd.CommandType()), successful, cmd)

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

			successful := err == nil
			queue(ctx, "event", string(event.EventType()), successful, event.Data())

			if err != nil {
				return fmt.Errorf("could not handle event: %w", err)
			}

			return nil
		})
	}
}
