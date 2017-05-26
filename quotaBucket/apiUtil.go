package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/constants"
	"reflect"
	"time"
)

const (
	//request response common params
	reqEdgeOrgID = "edgeOrgID"
	reqID        = "id"
	reqMaxCount  = "maxCount"

	//request specific params
	reqInterval = "interval"
	reqTimeUnit = "timeUnit"
	reqQType = "type"
	reqStartTimestamp = "startTimestamp"
	reqSyncTimeInSec = "syncTimeInSec"
	reqSyncMessageCount = "syncMessageCount"
	reqWeight = "weight"

	//response specific params
	respExceeded = "exceeded"
	respRemainingCount = "remainingCount"
	respExpiresTimestamp = "expiresTimestamp"
	respStartTimestamp = "startTimestamp"

)

type QuotaBucketResults struct {
	EdgeOrgID        string
	ID               string
	MaxCount         int64
	exceeded         bool
	remainingCount   int64
	startTimestamp   int64
	expiresTimestamp int64
}

func (qBucketRequest *QuotaBucket) FromAPIRequest(quotaBucketMap map[string]interface{}) error {
	var cacheKey string
	var edgeOrgID, id, timeUnit, quotaType string
	var interval int
	var startTime, maxCount, weight int64
	newQBucket := &QuotaBucket{}
	var err error

	value, ok := quotaBucketMap[reqEdgeOrgID]
	if !ok {
		return errors.New("missing field: "+ reqEdgeOrgID + " is required")
	}
	if edgeOrgIDType := reflect.TypeOf(value); edgeOrgIDType.Kind() != reflect.String {
		return errors.New("invalid type : "+ reqEdgeOrgID + " should be a string")
	}
	edgeOrgID = value.(string)

	value, ok = quotaBucketMap[reqID]
	if !ok {
		return errors.New("missing field: "+ reqID + " is required")
	}
	if idType := reflect.TypeOf(value); idType.Kind() != reflect.String {
		return errors.New("invalid type : "+ reqID + " should be a string")
	}
	id = value.(string)

	//build cacheKey - to retrieve from or add to quotaCache
	cacheKey = edgeOrgID + constants.CacheKeyDelimiter + id

	value, ok = quotaBucketMap[reqInterval]
	if !ok {
		return errors.New("missing field: "+ reqInterval + " is required")
	}
	//from input its read as float, hence need to then convert to int.
	if intervalType := reflect.TypeOf(value); intervalType.Kind() != reflect.Float64 {
		return errors.New("invalid type : "+ reqInterval + " should be a number")
	}
	intervalFloat := value.(float64)
	interval = int(intervalFloat)

	//TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	value, ok = quotaBucketMap[reqTimeUnit]
	if !ok {
		return errors.New("missing field: "+ reqTimeUnit + " is required")
	}
	if timeUnitType := reflect.TypeOf(value); timeUnitType.Kind() != reflect.String {
		return errors.New("invalid type : "+ reqTimeUnit + " should be a string")
	}
	timeUnit = value.(string)

	//QuotaType {CALENDAR, FLEXI, ROLLING_WINDOW}
	value, ok = quotaBucketMap[reqQType]
	if !ok {
		return errors.New("missing field: "+ reqQType + " is required")
	}
	if quotaTypeType := reflect.TypeOf(value); quotaTypeType.Kind() != reflect.String {
		return errors.New("invalid type : "+ reqQType + " should be a string")
	}
	quotaType = value.(string)

	value, ok = quotaBucketMap[reqStartTimestamp]
	if !ok { //todo: in the current cps code startTime is optional for QuotaBucket. should we make startTime optional to NewQuotaBucket?
		startTime = time.Now().UTC().Unix()
	} else {
		//	//from input when its read its float, need to then convert to int.
		if startTimeType := reflect.TypeOf(value); startTimeType.Kind() != reflect.Float64 {
			return errors.New("invalid type : "+ reqStartTimestamp + " should be UNIX timestamp")
		}
		startTimeFloat := value.(float64)
		startTime = int64(startTimeFloat)
	}

	value, ok = quotaBucketMap[reqMaxCount]
	if !ok {
		return errors.New("missing field: "+ reqMaxCount + " is required")
	}
	//from input when its read its float, need to then convert to int.
	if maxCountType := reflect.TypeOf(value); maxCountType.Kind() != reflect.Float64 {
		return errors.New("invalid type : "+ reqMaxCount + " should be a number")
	}
	maxCountFloat := value.(float64)
	maxCount = int64(maxCountFloat)

	value, ok = quotaBucketMap[reqWeight]
	if !ok {
		return errors.New("missing field: "+ reqWeight + " is required")
	}
	//from input when its read its float, need to then convert to int.
	if weightType := reflect.TypeOf(value); weightType.Kind() != reflect.Float64 {
		return errors.New("invalid type : "+ reqWeight + " should be a number")
	}
	weightFloat := value.(float64)
	weight = int64(weightFloat)

	syncTimeValue, syncTimeOK := quotaBucketMap[reqSyncTimeInSec]
	syncMsgCountValue, syncMsgCountOK := quotaBucketMap[reqSyncMessageCount]

	if syncTimeOK && syncMsgCountOK {
		return errors.New("either "+ reqSyncTimeInSec + " or "+ reqSyncMessageCount + " should be present but not both.")
	}

	if !syncTimeOK && !syncMsgCountOK {
		return errors.New("either "+ reqSyncTimeInSec + " or "+ reqSyncMessageCount + " should be present. both cant be empty.")
	}

	if syncTimeOK {
		if syncTimeType := reflect.TypeOf(syncTimeValue); syncTimeType.Kind() != reflect.Float64 {
			return errors.New("invalid type : "+ reqSyncTimeInSec + " should be a number")
		}
		syncTimeFloat := syncTimeValue.(float64)
		syncTimeInt := int64(syncTimeFloat)

		//try to retrieve from cache
		newQBucket, ok = getFromCache(cacheKey, weight)

		if !ok {
			newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType,
				startTime, maxCount, weight, syncTimeInt, -1)
			if err != nil {
				return errors.New("error creating quotaBucket: " + err.Error())
			}

			qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

			if err := qBucketRequest.Validate(); err != nil {
				return errors.New("error validating quotaBucket: " + err.Error())
			}

			addToCache(qBucketRequest)
			return nil
		}
		qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

		return nil

	} else if syncMsgCountOK {
		if syncMsgCountType := reflect.TypeOf(syncMsgCountValue); syncMsgCountType.Kind() != reflect.Float64 {
			return errors.New("invalid type : "+ reqSyncMessageCount + " should be a number")
		}
		syncMsgCountFloat := syncMsgCountValue.(float64)
		syncMsgCountInt := int64(syncMsgCountFloat)
		//try to retrieve from cache
		newQBucket, ok = getFromCache(cacheKey, weight)

		if !ok {
			newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType,
				startTime, maxCount, weight, -1, syncMsgCountInt)
			if err != nil {
				return errors.New("error creating quotaBucket: " + err.Error())
			}
			qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

			if err := qBucketRequest.Validate(); err != nil {
				return errors.New("error validating quotaBucket: " + err.Error())
			}

			addToCache(qBucketRequest)
			return nil

		}
		qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

		return nil
	}

	//try to retrieve from cache
	newQBucket, ok = getFromCache(cacheKey, weight)

	if !ok {
		//for synchronous quotaBucket
		newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType,
			startTime, maxCount, weight, -1, -1)
		if err != nil {
			return errors.New("error creating quotaBucket: " + err.Error())
		}
		qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

		if err := qBucketRequest.Validate(); err != nil {
			return errors.New("error validating quotaBucket: " + err.Error())
		}
		addToCache(qBucketRequest)
		return nil
	}

	qBucketRequest.quotaBucketData = newQBucket.quotaBucketData
	return nil

}

func (qBucketResults *QuotaBucketResults) ToAPIResponse() map[string]interface{} {
	resultsMap := make(map[string]interface{})
	resultsMap[reqEdgeOrgID] = qBucketResults.EdgeOrgID
	resultsMap[reqID] = qBucketResults.ID
	resultsMap[reqMaxCount] = qBucketResults.MaxCount
	resultsMap[respExceeded] = qBucketResults.exceeded
	resultsMap[respRemainingCount] = qBucketResults.remainingCount
	resultsMap[respStartTimestamp] = qBucketResults.startTimestamp
	resultsMap[respExpiresTimestamp] = qBucketResults.expiresTimestamp

	return resultsMap
}
