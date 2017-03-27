package quotaBucket

import (
	"errors"
	"fmt"
	"reflect"
)

func (qBucket *QuotaBucket) FromAPIRequest(quotaBucketMap map[string]interface{}) error {
	var edgeOrgID, id, timeUnit, quotaType, bucketType string
	var interval, maxCount int
	var startTime int64
	var preciseAtSecondsLevel bool

	fmt.Println("quotaBucketMap: ", quotaBucketMap)

	value, ok := quotaBucketMap["edgeOrgID"]
	if !ok {
		return errors.New(`missing field: 'edgeOrgID' is required`)
	}
	if edgeOrgIDType := reflect.TypeOf(value); edgeOrgIDType.Kind() != reflect.String {
		return errors.New(`invalid type : 'edgeOrgID' should be a string`)
	}
	edgeOrgID = value.(string)
	//fmt.Println("edgeOrgID: ", edgeOrgID)

	value, ok = quotaBucketMap["id"]
	if !ok {
		return errors.New(`missing field: 'id' is required`)
	}
	if idType := reflect.TypeOf(value); idType.Kind() != reflect.String {
		return errors.New(`invalid type : 'id' should be a string`)
	}
	id = value.(string)
	//fmt.Println("id: ", id)

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
	//fmt.Println("interval: ", interval)

	//TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	value, ok = quotaBucketMap["timeUnit"]
	if !ok {
		return errors.New(`missing field: 'timeUnit' is required`)
	}
	if timeUnitType := reflect.TypeOf(value); timeUnitType.Kind() != reflect.String {
		return errors.New(`invalid type : 'timeUnit' should be a string`)
	}
	timeUnit = value.(string)
	//fmt.Println("timeUnit: ", timeUnit)

	//Type {CALENDAR, FLEXI, ROLLING_WINDOW}
	value, ok = quotaBucketMap["quotaType"]
	if !ok {
		return errors.New(`missing field: 'quotaType' is required`)
	}
	if quotaTypeType := reflect.TypeOf(value); quotaTypeType.Kind() != reflect.String {
		return errors.New(`invalid type : 'quotaType' should be a string`)
	}
	quotaType = value.(string)
	//fmt.Println("quotaType: ", quotaType)

	value, ok = quotaBucketMap["preciseAtSecondsLevel"]
	if !ok {
		return errors.New(`missing field: 'preciseAtSecondsLevel' is required`)
	}
	if preciseAtSecondsLevelType := reflect.TypeOf(value); preciseAtSecondsLevelType.Kind() != reflect.Bool {
		return errors.New(`invalid type : 'preciseAtSecondsLevel' should be boolean`)
	}
	preciseAtSecondsLevel = value.(bool)
	//fmt.Println("preciseAtSecondsLevel: ", preciseAtSecondsLevel)

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
	//fmt.Println("startTime: ", startTime)

	value, ok = quotaBucketMap["maxCount"]
	if !ok {
		return errors.New(`missing field: 'maxCount' is required`)
	}
	//from input when its read its float, need to then convert to int.
	if maxCountType := reflect.TypeOf(value); maxCountType.Kind() != reflect.Float64 {
		return errors.New(`invalid type : 'maxCount' should be a number`)
	}
	maxCountFloat := value.(float64)
	maxCount = int(maxCountFloat)
	//fmt.Println("maxCount: ", maxCount)

	value, ok = quotaBucketMap["bucketType"]
	if !ok {
		return errors.New(`missing field: 'bucketType' is required`)
	}
	if bucketTypeType := reflect.TypeOf(value); bucketTypeType.Kind() != reflect.String {
		return errors.New(`invalid type : 'bucketType' should be a string`)
	}
	bucketType = value.(string)
	//fmt.Println("bucketType: ", bucketType)

	newQBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
	if err != nil {
		return errors.New("error creating newquotaBucket: " + err.Error())

	}

	qBucket.quotaBucketData = newQBucket.quotaBucketData

	if err := qBucket.Validate(); err != nil {
		return errors.New("failed in Validating the quotaBucket: " + err.Error())
	}

	return nil

}
