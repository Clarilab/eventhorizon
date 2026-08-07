package telemetry

import (
    "context"
    "fmt"

    eh "github.com/Clarilab/eventhorizon"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
)

// NewCommandHandlerMiddleware returns a new command handler middleware that adds tracing spans.
func NewCommandHandlerMiddleware() eh.CommandHandlerMiddleware {
    tracer := otel.Tracer("eventhorizon")

    return eh.CommandHandlerMiddleware(func(h eh.CommandHandler) eh.CommandHandler {
        return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
            opName := fmt.Sprintf("Command(%s)", cmd.CommandType())
            ctx, span := tracer.Start(ctx, opName)
            defer span.End()

            span.SetAttributes(
                attribute.String("eh.command_type", string(cmd.CommandType())),
                attribute.String("eh.aggregate_type", string(cmd.AggregateType())),
                attribute.String("eh.aggregate_id", cmd.AggregateID().String()),
            )

            err := h.HandleCommand(ctx, cmd)
            if err != nil {
                span.RecordError(err)
                span.SetStatus(codes.Error, err.Error())
            }

            return err
        })
    })
}
