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

	quotaPeriod := QuotaPeriod{}
	value, ok = quotaBucketMap["period"]
	//if period is not sent in the request, it is calculated based in the startTime, quotaType and interval.
	if ok {
		var inStartInt, startInt, endInt int64

		isPeriodMap := reflect.TypeOf(value)
		if isPeriodMap.Kind() != reflect.Map {
			return errors.New(`invalid type : 'period' should be a Map`)
		}
		periodMap := value.(map[string]interface{})

		inStartTimeValue, ok := periodMap["inputStartTime"]
		if !ok {
			//set period.inputStart time from qBucket.startTime
			inStartInt = startTime
		} else {
			if inStartType := reflect.TypeOf(inStartTimeValue); inStartType.Kind() != reflect.Float64 {
				return errors.New(`invalid type : 'inputStartTime' in 'period' should be UNIX timestamp`)
			}
			inStartFloat := inStartTimeValue.(float64)
			inStartInt = int64(inStartFloat)
			if startTime != inStartInt {
				return errors.New(`invalid value : 'inputStartTime' in 'period' should be same as 'startTime'' in request`)
			}
		}

		startTimeValue, ok := periodMap["startTime"]
		if !ok {
			return errors.New(`missing field : 'startTime' in 'period' cannot be empty`)
		}
		if periodStartType := reflect.TypeOf(startTimeValue); periodStartType.Kind() != reflect.Float64 {
			return errors.New(`invalid type : 'startTime' in 'period' should be UNIX timestamp`)
		}
		periodStartFloat := startTimeValue.(float64)
		startInt = int64(periodStartFloat)

		periodEndValue, ok := periodMap["endTime"]
		if !ok {
			return errors.New(`missing field : 'endTime' in 'period' cannot be empty`)
		}
		if periodEndType := reflect.TypeOf(periodEndValue); periodEndType.Kind() != reflect.Float64 {
			return errors.New(`invalid type : 'endTime' in 'period' should be UNIX timestamp`)
		}
		periodEndFloat := periodEndValue.(float64)
		endInt = int64(periodEndFloat)

		quotaPeriod = NewQuotaPeriod(inStartInt, startInt, endInt)
	}

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

	newQBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, quotaPeriod, startTime, maxCount, bucketType)
	qBucket.quotaBucketData = newQBucket.quotaBucketData

	if err := qBucket.Validate(); err != nil {
		return errors.New("failed in Validating the quotaBucket: " + err.Error())
	}

	return nil

}
