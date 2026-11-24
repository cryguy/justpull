package justpull

import "errors"

// permanentError marks a failure that retrying inside this pull cannot fix.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return permanentError{err} }

func isPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// errRangeNotSatisfiable means the registry refused our resume offset.
var errRangeNotSatisfiable = errors.New("registry refused the resume offset")
