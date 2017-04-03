package apidQuota_test

import (
	. "github.com/onsi/ginkgo"
	//"net/http"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"bytes"
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
	FIt("test Synchronous quota - valid test cases", func() {
		requestData := make(map[string]interface{})
		requestData["edgeOrgID"] = "testTenant"
		requestData["id"] = "testID"
		requestData["interval"] = 1
		requestData["timeUnit"] = "HOUR"
		requestData["quotaType"] = "CALENDAR"
		requestData["preciseAtSecondsLevel"] = false
		requestData["startTime"] = 1489189921
		requestData["maxCount"] = 5
		requestData["bucketType"] = "Synchronous"
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
			Fail("wrong status code: " + res.Status)
		}

		//valid request - case insensitive - timeUnit, quotaType, bucketType
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

		//valid request - startTime not sent in request - optional.
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
			Fail("wrong status code: " + res.Status)
		}

	})
})
