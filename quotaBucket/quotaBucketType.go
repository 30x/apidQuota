package quotaBucket

import (
	"errors"
	"strings"
)

const (
	QuotaBucketTypeSynchronous    = "synchronous"
	QuotaBucketTypeAsynchronous   = "asynchronous"
	QuotaBucketTypeNonDistributed = "nonDistributed"
	//todo: Add other bucketTypes
)

type QuotaBucketType interface {
	resetCount(bucket *QuotaBucket) error
}

type SynchronousQuotaBucketType struct{}

func (sQuotaBucket SynchronousQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//do nothing.
	return nil
}

type AsynchronousQuotaBucketType struct{}

func (sQuotaBucket AsynchronousQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//yet to implement
	return nil
}

type NonDistributedQuotaBucketType struct{}

func (sQuotaBucket NonDistributedQuotaBucketType) resetCount(qBucket *QuotaBucket) error {
	//yet to implement
	return nil
}

func GetQuotaBucketHandler(qBucket string) (QuotaBucketType, error) {
	var quotaBucketType QuotaBucketType
	qBucketType := strings.ToLower(strings.TrimSpace(qBucket))
	switch qBucketType {
	case QuotaBucketTypeSynchronous:
		quotaBucketType = &SynchronousQuotaBucketType{}
		return quotaBucketType, nil
	case QuotaBucketTypeAsynchronous:
		quotaBucketType = &AsynchronousQuotaBucketType{}
		return quotaBucketType, nil
	case QuotaBucketTypeNonDistributed:
		quotaBucketType = &NonDistributedQuotaBucketType{}
		return quotaBucketType, nil
	default:
		return nil, errors.New("Ignoring unrecognized quota type in request: " + qBucket)

	}
}
