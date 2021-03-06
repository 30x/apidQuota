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
	"github.com/apid/apidQuota/services"
	"github.com/apid/apidQuota/constants"
)

type QuotaBucketType interface {
	resetCount(bucket *QuotaBucket) error
	incrementQuotaCount(qBucket *QuotaBucket) (*QuotaBucketResults, error)
}

type SynchronousQuotaBucketType struct{}

func (sQuotaBucket SynchronousQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//do nothing.
	return nil
}

func (sQuotaBucket SynchronousQuotaBucketType) incrementQuotaCount(q *QuotaBucket) (*QuotaBucketResults, error) {
	period, err := q.GetPeriod()
	if err != nil {
		return nil, errors.New("error getting period: " + err.Error())
	}
	maxCount := q.GetMaxCount()
	exceeded := false
	remainingCount := int64(0)

	weight := q.GetWeight()

	//first retrieve the count from counter service.
	currentCount, err := services.GetCount(q.GetEdgeOrgID(), q.GetID(), period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
	if err != nil {
		return nil, err
	}

	if period.IsCurrentPeriod(q) {
		if currentCount < maxCount {
			allowed := maxCount - currentCount
			if allowed >= weight {
				if weight != 0 {
					currentCount, err = services.IncrementAndGetCount(q.GetEdgeOrgID(), q.GetID(), weight, period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
					if err != nil {
						return nil, err
					}
				}
				remainingCount = maxCount - (currentCount)

			} else {
				if weight != 0 {
					exceeded = true
				}
				remainingCount = maxCount - currentCount
			}
		} else {
			exceeded = true
			remainingCount = maxCount - currentCount
		}
	}

	if remainingCount < 0 {
		remainingCount = int64(0)
	}

	results := &QuotaBucketResults{
		EdgeOrgID:        q.GetEdgeOrgID(),
		ID:               q.GetID(),
		exceeded:         exceeded,
		remainingCount:   remainingCount,
		MaxCount:         maxCount,
		startTimestamp:   period.GetPeriodStartTime().Unix(),
		expiresTimestamp: period.GetPeriodEndTime().Unix(),
	}

	return results, nil
}

type AsynchronousQuotaBucketType struct {
}

func (quotaBucketType AsynchronousQuotaBucketType) resetCount(q *QuotaBucket) error {
	//yet to implement
	return nil
}

func (quotaBucketType AsynchronousQuotaBucketType) incrementQuotaCount(q *QuotaBucket) (*QuotaBucketResults, error) {
	period, err := q.GetPeriod()
	if err != nil {
		return nil, errors.New("error getting period: " + err.Error())
	}
	aSyncBucket := q.GetAsyncQuotaBucket()
	if aSyncBucket == nil {
		return nil, errors.New(constants.AsyncQuotaBucketEmpty + " : aSyncQuotaBucket to increment cannot be empty.")
	}
	currentCount, err := aSyncBucket.getCount(q, period)
	if err != nil {
		return nil, err
	}

	maxCount := q.GetMaxCount()
	exceeded := false
	remainingCount := int64(0)
	weight := q.GetWeight()

	if period.IsCurrentPeriod(q) {

		if currentCount < maxCount {
			diffCount := (currentCount + weight) - maxCount
			if diffCount > 0 {
				exceeded = true
				remainingCount = maxCount - currentCount

			} else {
				aSyncBucket.addToCounter(weight)
				aSyncBucket.addToAsyncLocalMessageCount(weight)
				remainingCount = maxCount - (currentCount + weight)

			}

			asyncMessageCount, err := aSyncBucket.getAsyncSyncMessageCount()
			if err != nil {
				return nil, err
			}

			asyncLocalMsgCount,err := aSyncBucket.getAsyncLocalMessageCount()
			if err != nil {
				return nil, err
			}

			if asyncMessageCount > 0 &&
				asyncLocalMsgCount >= asyncMessageCount {
				err = internalRefresh(q, period)
				if err != nil {
					return nil, err
				}
			}
		} else {
			exceeded = true
			remainingCount = maxCount - currentCount
		}
	}
	if remainingCount < 0 {
		remainingCount = int64(0)
	}

	results := &QuotaBucketResults{
		EdgeOrgID:        q.GetEdgeOrgID(),
		ID:               q.GetID(),
		exceeded:         exceeded,
		remainingCount:   remainingCount,
		MaxCount:         maxCount,
		startTimestamp:   period.GetPeriodStartTime().Unix(),
		expiresTimestamp: period.GetPeriodEndTime().Unix(),
	}

	return results, nil
}

func internalRefresh(q *QuotaBucket, period *quotaPeriod) error {
	var err error
	aSyncBucket := q.GetAsyncQuotaBucket()
	if aSyncBucket == nil {
		return errors.New(constants.AsyncQuotaBucketEmpty)
	}

	weight := int64(0)
	countFromCounterService := int64(0)
	globalCount,err := aSyncBucket.getAsyncGlobalCount()
	if err != nil {
		return err
	}

	maxCount := q.GetMaxCount()
	for _, counterEle := range *aSyncBucket.asyncCounter {
		weight += counterEle

		//delete from asyncCounter
		temp := *aSyncBucket.asyncCounter
		temp = temp[1:]
		aSyncBucket.asyncCounter = &temp

		if (weight + globalCount) >= maxCount {
			//clear asyncCounter
			for range *aSyncBucket.asyncCounter {
				//delete all elements from asyncCounter
				temp := *aSyncBucket.asyncCounter
				temp = temp[1:]
				aSyncBucket.asyncCounter = &temp
			}
		}
	}

	countFromCounterService, err = services.IncrementAndGetCount(q.GetEdgeOrgID(), q.GetID(), weight, period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
	if err != nil {
		return err
	}
	aSyncBucket.asyncGLobalCount = countFromCounterService

	aSyncBucket.asyncLocalMessageCount -= weight
	return nil
}

type NonDistributedQuotaBucketType struct{}

func (sQuotaBucket NonDistributedQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//yet to implement
	return errors.New("methog not implemented")
}
func (sQuotaBucket NonDistributedQuotaBucketType) incrementQuotaCount(qBucket *QuotaBucket) (*QuotaBucketResults, error) {

	return nil, errors.New("methog not implemented")
}

func GetQuotaBucketHandler(qBucket *QuotaBucket) (QuotaBucketType, error) {

	if !qBucket.IsDistrubuted() {
		quotaBucketType := &NonDistributedQuotaBucketType{}
		return quotaBucketType, nil
	} else {
		if qBucket.IsSynchronous() {
			quotaBucketType := &SynchronousQuotaBucketType{}
			return quotaBucketType, nil
		}
		quotaBucketType := &AsynchronousQuotaBucketType{}
		return quotaBucketType, nil

	}

	return nil, errors.New("ignoring: unrecognized quota type")

}
