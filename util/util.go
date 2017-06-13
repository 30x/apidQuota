// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
