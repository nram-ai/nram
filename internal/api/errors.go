package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// APIError represents a structured error response returned by the API.
type APIError struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Status  int         `json:"-"`
	Details interface{} `json:"details,omitempty"`
}

// Error implements the error interface.
func (e *APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// errorEnvelope is the JSON wrapper for error responses.
type errorEnvelope struct {
	Error *errorBody `json:"error"`
}

type errorBody struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details"`
}

// ErrBadRequest returns a 400 Bad Request error.
func ErrBadRequest(message string) *APIError {
	return &APIError{
		Code:    "bad_request",
		Message: message,
		Status:  http.StatusBadRequest,
	}
}

// ErrUnauthorized returns a 401 Unauthorized error.
func ErrUnauthorized(message string) *APIError {
	return &APIError{
		Code:    "unauthorized",
		Message: message,
		Status:  http.StatusUnauthorized,
	}
}

// ErrForbidden returns a 403 Forbidden error.
func ErrForbidden(message string) *APIError {
	return &APIError{
		Code:    "forbidden",
		Message: message,
		Status:  http.StatusForbidden,
	}
}

// ErrNotFound returns a 404 Not Found error.
func ErrNotFound(message string) *APIError {
	return &APIError{
		Code:    "not_found",
		Message: message,
		Status:  http.StatusNotFound,
	}
}

// ErrConflict returns a 409 Conflict error.
func ErrConflict(message string) *APIError {
	return &APIError{
		Code:    "conflict",
		Message: message,
		Status:  http.StatusConflict,
	}
}

// ErrRateLimited returns a 429 Too Many Requests error.
func ErrRateLimited(message string) *APIError {
	return &APIError{
		Code:    "rate_limited",
		Message: message,
		Status:  http.StatusTooManyRequests,
	}
}

// ErrInternal returns a 500 Internal Server Error.
func ErrInternal(message string) *APIError {
	return &APIError{
		Code:    "internal_error",
		Message: message,
		Status:  http.StatusInternalServerError,
	}
}

// WriteError writes a structured JSON error response to the given ResponseWriter.
// It sets the Content-Type header to application/json and writes the appropriate
// HTTP status code.
func WriteError(w http.ResponseWriter, err *APIError) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(err.Status)

	envelope := errorEnvelope{
		Error: &errorBody{
			Code:    err.Code,
			Message: err.Message,
			Details: err.Details,
		},
	}

	if encErr := json.NewEncoder(w).Encode(envelope); encErr != nil {
		log.Printf("api: failed to encode error response: %v", encErr)
	}
}

// ErrorMiddleware returns middleware that recovers from panics and returns
// a structured 500 Internal Server Error response.
func ErrorMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("api: panic recovered: %v", rec)
				WriteError(w, ErrInternal("an unexpected error occurred"))
			}
		}()

		next.ServeHTTP(w, r)
	})
}
