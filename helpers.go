package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func safeWrite(w http.ResponseWriter, statusCode int, content []byte) {
	w.WriteHeader(statusCode)
	_, err := w.Write(content)
	if err != nil {
		http.Error(w, "unexpected error", http.StatusInternalServerError)
	}
}

// reply on request with valid json.
func reply(w http.ResponseWriter, response Response) {
	respBytes, err := json.Marshal(response)
	if err != nil {
		safeWrite(w, response.status, []byte(fmt.Sprintf("{\"error\":\"%s\"}", err)))
		return
	}
	safeWrite(w, response.status, respBytes)
}
