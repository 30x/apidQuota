package services

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/30x/apidQuota/constants"
	"github.com/30x/apidQuota/globalVariables"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
	"fmt"
	"os"
)

const (
	edgeOrgID = "orgId"
	key       = "key"
	delta     = "delta"
	startTime = "startTime"
	endTime   = "endTime"
)

var client *http.Client = &http.Client{
	//setting the timeout to 60 sec for requests to counterService
	Timeout: time.Duration(60 * time.Second),
}

var token string

func addApigeeSyncTokenToHeader(req *http.Request) {
	token = globalVariables.Config.GetString(constants.ApigeeSyncBearerToken)
	req.Header.Set("Authorization", "Bearer "+token)
}

func GetCount(orgID string, quotaKey string, startTimeInt int64, endTimeInt int64) (int64, error) {

	return IncrementAndGetCount(orgID, quotaKey, 0, startTimeInt, endTimeInt)
}

func IncrementAndGetCount(orgID string, quotaKey string, count int64, startTimeInt int64, endTimeInt int64) (int64, error) {
	headers := http.Header{}
	headers.Set("Accept", "application/json")
	headers.Set("Content-Type", "application/json")
	method := "POST"

	if globalVariables.CounterServiceURL == "" {
		return 0, errors.New(constants.URLCounterServiceNotSet)
	}

	serviceURL, err := url.Parse(globalVariables.CounterServiceURL)
	if err != nil {
		return 0, errors.New(constants.URLCounterServiceInvalid)
	}

	//'{  "orgId": "test_org",  "delta": 1,  "key": "fixed-test-key" } '
	reqBody := make(map[string]interface{})
	reqBody[edgeOrgID] = orgID
	reqBody[key] = quotaKey
	reqBody[delta] = count
	reqBody[startTime] = startTimeInt * int64(1000)
	reqBody[endTime] = endTimeInt * int64(1000)

	reqBodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, errors.New(constants.MarshalJSONError)
	}

	contentLength := len(reqBodyBytes)
	request := &http.Request{
		Header:        headers,
		Method:        method,
		URL:           serviceURL,
		Body:          ioutil.NopCloser(bytes.NewReader(reqBodyBytes)),
		ContentLength: int64(contentLength),
	}
	addApigeeSyncTokenToHeader(request)

	fmt.Println("request: ", request)
	resp, err := client.Do(request)

	if err != nil {
		return 0, errors.New("error calling CounterService: " + err.Error())
	}
	defer resp.Body.Close()


	globalVariables.Log.Debug("response: ", resp)
	if resp.StatusCode != http.StatusOK {
		fmt.Println("resp: ", resp)

		respBodyBytes, err := ioutil.ReadAll(resp.Body)
		fmt.Println(os.Stdout, string(respBodyBytes)) //<-- here !


		if resp.StatusCode == http.StatusNotFound {
			return 0, errors.New("response from counter service: " + resp.Status + " and response body is: " + string(respBodyBytes))
		}
		if err != nil {

		}
		return 0, errors.New("response from counter service: " + resp.Status + " and response body is: " + string(respBodyBytes))

	}

	respBodyBytes, err := ioutil.ReadAll(resp.Body)
	respBody := make(map[string]interface{})
	err = json.Unmarshal(respBodyBytes, &respBody)
	if err != nil {
		return 0, errors.New("unable to parse response from counter service, error: " + err.Error())
	}

	respCount, ok := respBody["count"]
	if !ok {
		return 0, errors.New(`invalid response from counter service. field 'count' not sent in the response`)
	}

	globalVariables.Log.Debug("responseCount: ", respCount)

	respCountInt, ok := respCount.(float64)
	if !ok {
		return 0, errors.New(`invalid response from counter service. field 'count' sent in the response is not float`)
	}

	return int64(respCountInt), nil

}
