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

package quotaBucket

import (
	"errors"
	"github.com/apid/apidQuota/constants"
	"reflect"
	"time"
)

const (
	reqEdgeOrgID = "edgeOrgID"
	reqID        = "id"
	reqMaxCount  = "maxCount"
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
	var preciseAtSecondsLevel, distributed bool
	newQBucket := &QuotaBucket{}
	var err error

	value, ok := quotaBucketMap[reqEdgeOrgID]
	if !ok {
		return errors.New(`missing field: 'edgeOrgID' is required`)
	}
	if edgeOrgIDType := reflect.TypeOf(value); edgeOrgIDType.Kind() != reflect.String {
		return errors.New(`invalid type : 'edgeOrgID' should be a string`)
	}
	edgeOrgID = value.(string)

	value, ok = quotaBucketMap[reqID]
	if !ok {
		return errors.New(`missing field: 'id' is required`)
	}
	if idType := reflect.TypeOf(value); idType.Kind() != reflect.String {
		return errors.New(`invalid type : 'id' should be a string`)
	}
	id = value.(string)

	//build cacheKey - to retrieve from or add to quotaCache
	cacheKey = edgeOrgID + constants.CacheKeyDelimiter + id

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

	//QuotaType {CALENDAR, FLEXI, ROLLING_WINDOW}
	value, ok = quotaBucketMap["type"]
	if !ok {
		return errors.New(`missing field: 'type' is required`)
	}
	if quotaTypeType := reflect.TypeOf(value); quotaTypeType.Kind() != reflect.String {
		return errors.New(`invalid type : 'type' should be a string`)
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

	value, ok = quotaBucketMap["startTimestamp"]
	if !ok { //todo: in the current cps code startTime is optional for QuotaBucket. should we make startTime optional to NewQuotaBucket?
		startTime = time.Now().UTC().Unix()
	} else {
		//	//from input when its read its float, need to then convert to int.
		if startTimeType := reflect.TypeOf(value); startTimeType.Kind() != reflect.Float64 {
			return errors.New(`invalid type : 'startTime' should be UNIX timestamp`)
		}
		startTimeFloat := value.(float64)
		startTime = int64(startTimeFloat)
	}

	value, ok = quotaBucketMap[reqMaxCount]
	if !ok {
		return errors.New(`missing field: 'maxCount' is required`)
	}
	//from input when its read its float, need to then convert to int.
	if maxCountType := reflect.TypeOf(value); maxCountType.Kind() != reflect.Float64 {
		return errors.New(`invalid type : 'maxCount' should be a number`)
	}
	maxCountFloat := value.(float64)
	maxCount = int64(maxCountFloat)

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

	value, ok = quotaBucketMap["distributed"]
	if !ok {
		return errors.New(`missing field: 'distributed' is required`)
	}
	if preciseAtSecondsLevelType := reflect.TypeOf(value); preciseAtSecondsLevelType.Kind() != reflect.Bool {
		return errors.New(`invalid type : 'distributed' should be boolean`)
	}
	distributed = value.(bool)

	//if distributed check for sync or async Quota
	if distributed {
		value, ok = quotaBucketMap["synchronous"]
		if !ok {
			return errors.New(`missing field: 'synchronous' is required`)
		}
		if synchronousType := reflect.TypeOf(value); synchronousType.Kind() != reflect.Bool {
			return errors.New(`invalid type : 'synchronous' should be boolean`)
		}
		synchronous := value.(bool)

		// for async retrieve syncTimeSec or syncMessageCount
		if !synchronous {
			syncTimeValue, syncTimeOK := quotaBucketMap["syncTimeInSec"]
			syncMsgCountValue, syncMsgCountOK := quotaBucketMap["syncMessageCount"]

			if syncTimeOK && syncMsgCountOK {
				return errors.New(`either syncTimeInSec or syncMessageCount should be present but not both.`)
			}

			if !syncTimeOK && !syncMsgCountOK {
				return errors.New(`either syncTimeInSec or syncMessageCount should be present. both cant be empty.`)
			}

			if syncTimeOK {
				if syncTimeType := reflect.TypeOf(syncTimeValue); syncTimeType.Kind() != reflect.Float64 {
					return errors.New(`invalid type : 'syncTimeInSec' should be a number`)
				}
				syncTimeFloat := syncTimeValue.(float64)
				syncTimeInt := int64(syncTimeFloat)

				//try to retrieve from cache
				newQBucket, ok = getFromCache(cacheKey, weight)

				if !ok {
					newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel,
						startTime, maxCount, weight, distributed, synchronous, syncTimeInt, -1)
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
					return errors.New(`invalid type : 'syncTimeInSec' should be a number`)
				}
				syncMsgCountFloat := syncMsgCountValue.(float64)
				syncMsgCountInt := int64(syncMsgCountFloat)
				//try to retrieve from cache
				newQBucket, ok = getFromCache(cacheKey, weight)

				if !ok {
					newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel,
						startTime, maxCount, weight, distributed, synchronous, -1, syncMsgCountInt)
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
		}

		//try to retrieve from cache
		newQBucket, ok = getFromCache(cacheKey, weight)

		if !ok {
			//for synchronous quotaBucket
			newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel,
				startTime, maxCount, weight, distributed, synchronous, -1, -1)
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

	//for non distributed quotaBucket
	//retrieveFromCache.
	newQBucket, ok = getFromCache(cacheKey, weight)

	if !ok {
		//for non distributed quotaBucket
		newQBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel,
			startTime, maxCount, weight, distributed, false, -1, -1)
		if err != nil {
			return errors.New("error creating quotaBucket: " + err.Error())

		}

		qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

		if err := qBucketRequest.Validate(); err != nil {
			return errors.New("error validating quotaBucket: " + err.Error())
		}

		addToCache(qBucketRequest)
	}
	qBucketRequest.quotaBucketData = newQBucket.quotaBucketData

	return nil

}

func (qBucketResults *QuotaBucketResults) ToAPIResponse() map[string]interface{} {
	resultsMap := make(map[string]interface{})
	resultsMap[reqEdgeOrgID] = qBucketResults.EdgeOrgID
	resultsMap[reqID] = qBucketResults.ID
	resultsMap[reqMaxCount] = qBucketResults.MaxCount
	resultsMap["exceeded"] = qBucketResults.exceeded
	resultsMap["remainingCount"] = qBucketResults.remainingCount
	resultsMap["startTimestamp"] = qBucketResults.startTimestamp
	resultsMap["expiresTimestamp"] = qBucketResults.expiresTimestamp

	return resultsMap
}
