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

package apidQuota_test

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	"io/ioutil"
	"net/http"
	"time"
)

func init() {
	//testAPIDQuotaURL = ""
}

var testhttpClient *http.Client = &http.Client{
	//setting the timeout to 60 sec for requests to counterService
	Timeout: time.Duration(60 * time.Second),
}

const testQuotaAPIURL = "http://localhost:9000/quota"
const testValidOrg = "edgexfeb1"

var _ = Describe("Api Tests", func() {
	It("test Synchronous quota - valid test cases", func() {
		requestData := make(map[string]interface{})
		requestData["edgeOrgID"] = testValidOrg
		requestData["id"] = "testAppID"
		requestData["interval"] = 1
		requestData["timeUnit"] = "HOUR"
		requestData["type"] = "CALENDAR"
		requestData["preciseAtSecondsLevel"] = false
		requestData["startTime"] = time.Now().UTC().AddDate(0, 0, 1).Unix()
		requestData["maxCount"] = 5
		requestData["weight"] = 2
		requestData["distributed"] = true
		requestData["synchronous"] = true
		requestData["weight"] = 2

		reqBytes, err := json.Marshal(requestData)
		if err != nil {
			Fail("error converting requestBody into bytes: " + err.Error())
		}

		//valid request body
		req, err := http.NewRequest("POST", testQuotaAPIURL, ioutil.NopCloser(bytes.NewReader(reqBytes)))
		if err != nil {
			Fail("error getting newRequest: " + err.Error())
		}

		res, err := testhttpClient.Do(req)
		if err != nil {
			Fail("error calling the api: " + err.Error())
		}

		// Check the status code is 200 OK.
		if status := res.StatusCode; status != http.StatusOK {
			respBodyBytes, err := ioutil.ReadAll(res.Body)
			respBody := make(map[string]interface{})
			err = json.Unmarshal(respBodyBytes, &respBody)
			if err != nil {
				Fail("error: " + err.Error())
			}

			Fail("wrong status code: " + res.Status)
		}

		//TestCase1: valid request - case insensitive - timeUnit, quotaType, bucketType
		requestData["timeUnit"] = "HoUR"
		requestData["quotaType"] = "cALEndar"
		requestData["bucketType"] = "syncHRonous"

		req, err = http.NewRequest("POST", testQuotaAPIURL, ioutil.NopCloser(bytes.NewReader(reqBytes)))
		if err != nil {
			Fail("error getting newRequest: " + err.Error())
		}

		res, err = testhttpClient.Do(req)
		if err != nil {
			Fail("error calling the api: " + err.Error())
		}

		// Check the status code is what we expect.
		if status := res.StatusCode; status != http.StatusOK {
			Fail("wrong status code: " + res.Status)
		}

		//TestCase2: valid request - startTime not sent in request - optional.
		delete(requestData, "startTime")

		req, err = http.NewRequest("POST", testQuotaAPIURL, ioutil.NopCloser(bytes.NewReader(reqBytes)))
		if err != nil {
			Fail("error getting newRequest: " + err.Error())
		}

		res, err = testhttpClient.Do(req)
		if err != nil {
			Fail("error calling the api: " + err.Error())
		}

		// Check the status code is what we expect.
		if status := res.StatusCode; status != http.StatusOK {
			respBodyBytes, err := ioutil.ReadAll(res.Body)
			respBody := make(map[string]interface{})
			err = json.Unmarshal(respBodyBytes, &respBody)
			if err != nil {
				Fail("error: " + err.Error())
			}

			Fail("wrong status code: " + res.Status)
		}

		//TestCase3: quotaType = "RollingWidow"
		requestData["quotaType"] = "RollingWindow"
		requestData["startTime"] = time.Now().UTC().AddDate(0, 0, 1).Unix()
		req, err = http.NewRequest("POST", testQuotaAPIURL, ioutil.NopCloser(bytes.NewReader(reqBytes)))
		if err != nil {
			Fail("error getting newRequest: " + err.Error())
		}

		res, err = testhttpClient.Do(req)
		if err != nil {
			Fail("error calling the api: " + err.Error())
		}

		// Check the status code is what we expect.
		if status := res.StatusCode; status != http.StatusOK {
			Fail("wrong status code: " + res.Status)
		}

	})

	It("test Synchronous quota - invalidation test cases", func() {
		requestData := make(map[string]interface{})
		uuid, err := uuid.NewUUID()
		if err != nil {
			Fail("error getting uuid")
		}

		requestData["edgeOrgID"] = "testTenant"
		requestData["id"] = "testID" + uuid.String()
		requestData["interval"] = 1
		requestData["timeUnit"] = "HOUR"
		requestData["quotaType"] = "CALENDAR"
		requestData["preciseAtSecondsLevel"] = false
		requestData["startTime"] = time.Now().UTC().AddDate(0, 0, 1).Unix()
		requestData["maxCount"] = 5
		requestData["bucketType"] = "Synchronous"
		requestData["weight"] = 2

		//invalid request body - interval not string
		requestData["interval"] = "test"

		reqBytes, err := json.Marshal(requestData)
		if err != nil {
			Fail("error converting requestBody into bytes: " + err.Error())
		}

		req, err := http.NewRequest("POST", testQuotaAPIURL, ioutil.NopCloser(bytes.NewReader(reqBytes)))
		if err != nil {
			Fail("error getting newRequest: " + err.Error())
		}

		res, err := testhttpClient.Do(req)
		if err != nil {
			Fail("error calling the api: " + err.Error())
		}

		// Check the status code is what we expect.
		if status := res.StatusCode; status != http.StatusBadRequest {
			respBodyBytes, err := ioutil.ReadAll(res.Body)
			respBody := make(map[string]interface{})
			err = json.Unmarshal(respBodyBytes, &respBody)
			if err != nil {
				Fail("error: " + err.Error())
			}

			Fail("wrong status code: " + res.Status)
		}

		//invalid request - timeUnit invalid
		requestData["interval"] = 1
		requestData["timeUnit"] = "invalidTimeUnit"
		reqBytes, err = json.Marshal(requestData)
		if err != nil {
			Fail("error converting requestBody into bytes: " + err.Error())
		}

		req, err = http.NewRequest("POST", testQuotaAPIURL, ioutil.NopCloser(bytes.NewReader(reqBytes)))
		if err != nil {
			Fail("error getting newRequest: " + err.Error())
		}

		res, err = testhttpClient.Do(req)
		if err != nil {
			Fail("error calling the api: " + err.Error())
		}

		// Check the status code is what we expect.
		if status := res.StatusCode; status != http.StatusBadRequest {
			Fail("wrong status code: " + res.Status)
		}

	})

})
