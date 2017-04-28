package apidQuota_test

import (
	. "github.com/onsi/ginkgo"
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
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

var _ = Describe("Api Tests", func() {
	It("test Synchronous quota - valid test cases", func() {
		requestData := make(map[string]interface{})
		requestData["edgeOrgID"] = "testTenant"
		requestData["id"] = "testID"
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
