package events

import "errors"

// ErrBusClosed is returned when attempting to publish or subscribe on a closed bus.
var ErrBusClosed = errors.New("event bus is closed")
