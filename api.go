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
	"strconv"
)

func InitAPI(services apid.Services) {
	globalVariables.Log.Debug("initialized API's exposed by apidQuota plugin")
	quotaBasePath := globalVariables.Config.GetString(constants.ConfigQuotaBasePath)
	services.API().HandleFunc(quotaBasePath, getAllQuotaValues).Methods("GET") //yet to implement.
	services.API().HandleFunc(quotaBasePath+"/{quotaItentifier}", incrementAndCheckQuotaLimit).Methods("POST")

}

func getAllQuotaValues(res http.ResponseWriter, req *http.Request) {
	stringbytes := []byte("yet to implement")
	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)
	res.Write(stringbytes)

}

func incrementAndCheckQuotaLimit(res http.ResponseWriter, req *http.Request) {

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
	globalVariables.Log.Println("quotaBucketMap from request: ", quotaBucketMap)

	// parse the request body into the Event struct
	qBucket := new(quotaBucket.QuotaBucket)
	if err = qBucket.FromAPIRequest(quotaBucketMap); err != nil {
		util.WriteErrorResponse(http.StatusBadRequest, constants.ErrorConvertReqBodyToEntity, err.Error(), res, req)
		return
	}

	quotaCount, err := qBucket.GetQuotaCount()
	if err != nil {
		util.WriteErrorResponse(http.StatusBadRequest, constants.UnMarshalJSONError, "error retrieving count for the give identifier "+err.Error(), res, req)
		return
	}

	stringbytes := []byte(strconv.Itoa(quotaCount))
	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)
	res.Write(stringbytes)

}
