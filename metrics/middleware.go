package metrics

import (
	eh "github.com/Clarilab/eventhorizon"
)

var (
	metricsCommandHandlerMiddleware eh.CommandHandlerMiddleware
	metricsEventHandlerMiddleware   eh.EventHandlerMiddleware
)

// setCommandMiddleware sets the metrics command handler middleware.
func setCommandMiddleware(m eh.CommandHandlerMiddleware) {
	metricsCommandHandlerMiddleware = m
}

// GetCommandMiddleware returns the metrics command handler middleware.
func GetCommandMiddleware() eh.CommandHandlerMiddleware {
	return metricsCommandHandlerMiddleware
}

// setEventMiddleware sets the metrics event handler middleware.
func setEventMiddleware(m eh.EventHandlerMiddleware) {
	metricsEventHandlerMiddleware = m
}

// GetEventMiddleware returns the metrics event handler middleware.
func GetEventMiddleware() eh.EventHandlerMiddleware {
	return metricsEventHandlerMiddleware
}
