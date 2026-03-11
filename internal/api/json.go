package api

import (
	"encoding/json"
	"log"
	"net/http"
)

// writeJSON encodes v as JSON and writes it to w with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: failed to encode JSON response: %v", err)
	}
}
