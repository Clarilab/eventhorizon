package metrics

import (
	eh "github.com/Clarilab/eventhorizon"
)

var (
	metricsCommandHandlerMiddleware eh.CommandHandlerMiddleware
	metricsEventHandlerMiddleware   eh.EventHandlerMiddleware
)

// SetCommandMiddleware sets the metrics command handler middleware.
func SetCommandMiddleware(m eh.CommandHandlerMiddleware) {
	metricsCommandHandlerMiddleware = m
}

// GetCommandMiddleware returns the metrics command handler middleware.
func GetCommandMiddleware() eh.CommandHandlerMiddleware {
	return metricsCommandHandlerMiddleware
}

// SetEventMiddleware sets the metrics event handler middleware.
func SetEventMiddleware(m eh.EventHandlerMiddleware) {
	metricsEventHandlerMiddleware = m
}

// GetEventMiddleware returns the metrics event handler middleware.
func GetEventMiddleware() eh.EventHandlerMiddleware {
	return metricsEventHandlerMiddleware
}
