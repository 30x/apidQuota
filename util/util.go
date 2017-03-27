package util

import (
	"encoding/json"
	"net/http"
)

// WriteErrorResponse will write the HTTP header and payload for the HTTP ResponseWriter provided
func WriteErrorResponse(status int, errorType string, errorDescription string, res http.ResponseWriter, req *http.Request) {
	response := make(map[string]string)
	response["error"] = errorType
	response["errorDescription"] = errorDescription
	responseJson, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(status)
	res.Write(responseJson)
}
