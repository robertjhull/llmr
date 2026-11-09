package llmr

import "errors"

type transientError struct {
	err error
}

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

func MarkTransient(err error) error {
	if err == nil {
		return nil
	}
	return &transientError{err: err}
}

func isTransientError(err error) bool {
	var t *transientError
	return errors.As(err, &t)
}

func isTransientStatus(code int) bool {
	switch code {
	case 408, 409, 425, 429:
		return true
	}
	return code >= 500
}
