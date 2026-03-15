package ingest

import "fmt"

type PermanentError struct {
	Cause error
}

func (e *PermanentError) Error() string {
	if e == nil || e.Cause == nil {
		return "permanent error"
	}
	return fmt.Sprintf("permanent error: %v", e.Cause)
}

func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newPermanentError(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Cause: err}
}

func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*PermanentError)
	return ok
}
