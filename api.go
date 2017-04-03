package apidQuota

import (
	"encoding/json"
	"github.com/30x/apid-core"
	"github.com/30x/apidQuota/constants"
	"github.com/30x/apidQuota/globalVariables"
	"github.com/30x/apidQuota/quotaBucket"
	"github.com/30x/apidQuota/util"
	"io/ioutil"
	"net/http"
)

func InitAPI(services apid.Services) {
	globalVariables.Log.Debug("initializing apidQuota plugin APIs")
	quotaBasePath := globalVariables.Config.GetString(constants.ConfigQuotaBasePath)
	services.API().HandleFunc(quotaBasePath, checkQuotaLimitExceeded).Methods("POST")

}

func checkQuotaLimitExceeded(res http.ResponseWriter, req *http.Request) {

	bodyBytes, err := ioutil.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		util.WriteErrorResponse(http.StatusBadRequest, constants.UnableToParseBody, "unable to read request body: "+err.Error(), res, req)
		return
	}

	quotaBucketMap := make(map[string]interface{}, 0)
	if err := json.Unmarshal(bodyBytes, &quotaBucketMap); err != nil {
		util.WriteErrorResponse(http.StatusBadRequest, constants.UnMarshalJSONError, "unable to convert request body to an object: "+err.Error(), res, req)
		return
	}

	// parse the request body into the QuotaBucket struct
	qBucket := new(quotaBucket.QuotaBucket)
	if err = qBucket.FromAPIRequest(quotaBucketMap); err != nil {
		util.WriteErrorResponse(http.StatusBadRequest, constants.ErrorConvertReqBodyToEntity, err.Error(), res, req)
		return
	}

	results, err := qBucket.IncrementQuotaLimit()
	if err != nil {
		util.WriteErrorResponse(http.StatusBadRequest, constants.ErrorCheckingQuotaLimit, "error retrieving count for the give identifier "+err.Error(), res, req)
		return
	}

	respMap := results.ToAPIResponse()
	respbytes, err := json.Marshal(respMap)

	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)
	res.Write(respbytes)

}
