package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/services"
)

type QuotaBucketType interface {
	resetCount(bucket *QuotaBucket) error
	incrementQuotaCount(qBucket *QuotaBucket) (*QuotaBucketResults, error)
	resetQuotaForCurrentPeriod(qBucket *QuotaBucket) (*QuotaBucketResults, error)
}

type SynchronousQuotaBucketType struct{}

func (sQuotaBucket SynchronousQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//do nothing.
	return nil
}
func (sQuotaBucket SynchronousQuotaBucketType) resetQuotaForCurrentPeriod(q *QuotaBucket) (*QuotaBucketResults, error) {

	weight := q.GetWeight()
	weightToReset := -weight
	period, err := q.GetPeriod()
	if err != nil {
		return nil, errors.New("error getting period: " + err.Error())
	}
	currentCount, err := services.IncrementAndGetCount(q.GetEdgeOrgID(), q.GetID(), weightToReset, period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
	exceededCount := currentCount > q.GetMaxCount()
	results := &QuotaBucketResults{
		EdgeOrgID:      q.GetEdgeOrgID(),
		ID:             q.GetID(),
		exceededTokens: exceededCount,
		currentTokens:  currentCount,
		MaxCount:       q.GetMaxCount(),
		startedAt:      period.GetPeriodStartTime().Unix(),
		expiresAt:      period.GetPeriodEndTime().Unix(),
	}
	return results, nil

}

func (sQuotaBucket SynchronousQuotaBucketType) incrementQuotaCount(q *QuotaBucket) (*QuotaBucketResults, error) {

	maxCount := q.GetMaxCount()
	exceededCount := false
	allowedCount := int64(0)
	weight := q.GetWeight()
	period, err := q.GetPeriod()
	if err != nil {
		return nil, errors.New("error getting period: " + err.Error())
	}

	//first retrieve the count from counter service.
	currentCount, err := services.GetCount(q.GetEdgeOrgID(), q.GetID(), period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
	if err != nil {
		return nil, err
	}

	if period.IsCurrentPeriod(q) {
		if currentCount < maxCount {
			allowed := maxCount - currentCount
			if allowed > weight {
				if weight != 0 {
					currentCount, err = services.IncrementAndGetCount(q.GetEdgeOrgID(), q.GetID(), weight, period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
					if err != nil {
						return nil, err
					}
				}
				allowedCount = currentCount
			} else {
				if weight != 0 {

					exceededCount = true
				}
				allowedCount = currentCount + weight
			}
		} else {

			exceededCount = true
			allowedCount = currentCount
		}
	}

	results := &QuotaBucketResults{
		EdgeOrgID:      q.GetEdgeOrgID(),
		ID:             q.GetID(),
		exceededTokens: exceededCount,
		currentTokens:  allowedCount,
		MaxCount:       maxCount,
		startedAt:      period.GetPeriodStartTime().Unix(),
		expiresAt:      period.GetPeriodEndTime().Unix(),
	}

	return results, nil
}

type AsynchronousQuotaBucketType struct {
	initialized      bool
	globalCount      int64
	syncMessageCount int64
	syncTimeInSec    int64
}

func (quotaBucketType AsynchronousQuotaBucketType) resetCount(q *QuotaBucket) error {
	//yet to implement
	return nil
}

func (quotaBucketType AsynchronousQuotaBucketType) incrementQuotaCount(q *QuotaBucket) (*QuotaBucketResults, error) {
	//getCount()
	return nil, nil
}

func (quotaBucketType AsynchronousQuotaBucketType) resetQuotaForCurrentPeriod(q *QuotaBucket) (*QuotaBucketResults, error) {
	return nil, nil
}

type NonDistributedQuotaBucketType struct{}

func (sQuotaBucket NonDistributedQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//yet to implement
	return nil
}
func (sQuotaBucket NonDistributedQuotaBucketType) incrementQuotaCount(qBucket *QuotaBucket) (*QuotaBucketResults, error) {

	return nil, nil
}
func (sQuotaBucket NonDistributedQuotaBucketType) resetQuotaForCurrentPeriod(q *QuotaBucket) (*QuotaBucketResults, error) {
	return nil, nil
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
