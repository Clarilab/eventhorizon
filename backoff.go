package eventhorizon

// BackOff is an interface for implementing backoff strategies.
type BackOff interface {
	Do(fn func() error) error
}
