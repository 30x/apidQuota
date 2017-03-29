package quotaBucket

import (
	"errors"
	"reflect"
)

type QuotaBucketResults struct {
	EdgeOrgID      string
	ID             string
	exceededTokens bool
	allowedTokens  int64
	MaxCount       int64
	startedAt      int64
	expiresAt      int64
}

func (qBucket *QuotaBucket) FromAPIRequest(quotaBucketMap map[string]interface{}) error {
	var edgeOrgID, id, timeUnit, quotaType, bucketType string
	var interval int
	var startTime, maxCount, weight int64
	var preciseAtSecondsLevel bool

	value, ok := quotaBucketMap["edgeOrgID"]
	if !ok {
		return errors.New(`missing field: 'edgeOrgID' is required`)
	}
	if edgeOrgIDType := reflect.TypeOf(value); edgeOrgIDType.Kind() != reflect.String {
		return errors.New(`invalid type : 'edgeOrgID' should be a string`)
	}
	edgeOrgID = value.(string)

	value, ok = quotaBucketMap["id"]
	if !ok {
		return errors.New(`missing field: 'id' is required`)
	}
	if idType := reflect.TypeOf(value); idType.Kind() != reflect.String {
		return errors.New(`invalid type : 'id' should be a string`)
	}
	id = value.(string)

	value, ok = quotaBucketMap["interval"]
	if !ok {
		return errors.New(`missing field: 'interval' is required`)
	}
	//from input when its read its float, need to then convert to int.
	if intervalType := reflect.TypeOf(value); intervalType.Kind() != reflect.Float64 {
		return errors.New(`invalid type : 'interval' should be a number`)
	}
	intervalFloat := value.(float64)
	interval = int(intervalFloat)

	//TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	value, ok = quotaBucketMap["timeUnit"]
	if !ok {
		return errors.New(`missing field: 'timeUnit' is required`)
	}
	if timeUnitType := reflect.TypeOf(value); timeUnitType.Kind() != reflect.String {
		return errors.New(`invalid type : 'timeUnit' should be a string`)
	}
	timeUnit = value.(string)

	//Type {CALENDAR, FLEXI, ROLLING_WINDOW}
	value, ok = quotaBucketMap["quotaType"]
	if !ok {
		return errors.New(`missing field: 'quotaType' is required`)
	}
	if quotaTypeType := reflect.TypeOf(value); quotaTypeType.Kind() != reflect.String {
		return errors.New(`invalid type : 'quotaType' should be a string`)
	}
	quotaType = value.(string)

	value, ok = quotaBucketMap["preciseAtSecondsLevel"]
	if !ok {
		return errors.New(`missing field: 'preciseAtSecondsLevel' is required`)
	}
	if preciseAtSecondsLevelType := reflect.TypeOf(value); preciseAtSecondsLevelType.Kind() != reflect.Bool {
		return errors.New(`invalid type : 'preciseAtSecondsLevel' should be boolean`)
	}
	preciseAtSecondsLevel = value.(bool)

	value, ok = quotaBucketMap["startTime"]
	if !ok {
		return errors.New(`missing field: 'startTime' is required`)
	}
	//from input when its read its float, need to then convert to int.
	if startTimeType := reflect.TypeOf(value); startTimeType.Kind() != reflect.Float64 {
		return errors.New(`invalid type : 'startTime' should be UNIX timestamp`)
	}
	startTimeFloat := value.(float64)
	startTime = int64(startTimeFloat)

	value, ok = quotaBucketMap["maxCount"]
	if !ok {
		return errors.New(`missing field: 'maxCount' is required`)
	}
	//from input when its read its float, need to then convert to int.
	if maxCountType := reflect.TypeOf(value); maxCountType.Kind() != reflect.Float64 {
		return errors.New(`invalid type : 'maxCount' should be a number`)
	}
	maxCountFloat := value.(float64)
	maxCount = int64(maxCountFloat)

	value, ok = quotaBucketMap["bucketType"]
	if !ok {
		return errors.New(`missing field: 'bucketType' is required`)
	}
	if bucketTypeType := reflect.TypeOf(value); bucketTypeType.Kind() != reflect.String {
		return errors.New(`invalid type : 'bucketType' should be a string`)
	}
	bucketType = value.(string)

	value, ok = quotaBucketMap["weight"]
	if !ok {
		return errors.New(`missing field: 'weight' is required`)
	}
	//from input when its read its float, need to then convert to int.
	if weightType := reflect.TypeOf(value); weightType.Kind() != reflect.Float64 {
		return errors.New(`invalid type : 'maxCount' should be a number`)
	}
	weightFloat := value.(float64)
	weight = int64(weightFloat)


	newQBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType, weight)
	if err != nil {
		return errors.New("error creating newquotaBucket: " + err.Error())

	}

	qBucket.quotaBucketData = newQBucket.quotaBucketData

	if err := qBucket.Validate(); err != nil {
		return errors.New("failed in Validating the quotaBucket: " + err.Error())
	}

	return nil

}

func  (qBucketResults *QuotaBucketResults)  ToAPIResponse() (map[string]interface{}) {
	resultsMap := make(map[string]interface{})
	resultsMap["edgeOrgID"] = qBucketResults.ID
	resultsMap["id"] = qBucketResults.ID
	resultsMap["exceededTokens"] = qBucketResults.exceededTokens
	resultsMap["allowedTokens"] = qBucketResults.allowedTokens
	resultsMap["MaxCount"] = qBucketResults.MaxCount
	resultsMap["startedAt"] = qBucketResults.startedAt
	resultsMap["expiresAt"] = qBucketResults.expiresAt


	return resultsMap
}