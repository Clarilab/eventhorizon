package backoff

// DefaultBackOff is a simple backoff strategy that retries a fixed number of times.
type DefaultBackOff struct {
	retries int
}

// NewDefaultBackOff creates a new DefaultBackOff with the given number of retries.
func NewDefaultBackOff(retries int) *DefaultBackOff {
	return &DefaultBackOff{retries: retries}
}

// Do implements the BackOff interface.
func (b *DefaultBackOff) Do(fn func() error) error {
	var err error

	for i := 0; i < b.retries; i++ {
		err = fn()

		if err == nil {
			return nil
		}
	}

	return err
}
