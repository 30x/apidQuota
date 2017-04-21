package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/services"
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

			if aSyncBucket.syncMessageCount > 0 &&
				aSyncBucket.asyncLocalMessageCount >= aSyncBucket.syncMessageCount {
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
	weight := int64(0)
	countFromCounterService := int64(0)
	globalCount := aSyncBucket.asyncGLobalCount
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
