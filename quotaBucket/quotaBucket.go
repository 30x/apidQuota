package quotaBucket

import (
	"errors"
	"fmt"
	"time"
)

const (
	TimeUnitSECOND = "second"
	TimeUnitMINUTE = "minute"
	TimeUnitHOUR   = "hour"
	TimeUnitDAY    = "day"
	TimeUnitWEEK   = "week"
	TimeUnitMONTH  = "month"
)

type QuotaPeriod struct {
	inputStartTime time.Time
	startTime      time.Time
	endTime        time.Time
}

func NewQuotaPeriod(inputStartTime int64, startTime int64, endTime int64) QuotaPeriod {
	pInStartTime := time.Unix(inputStartTime, 0)
	pStartTime := time.Unix(startTime, 0)
	pEndTime := time.Unix(endTime, 0)

	period := &QuotaPeriod{inputStartTime: pInStartTime,
		startTime: pStartTime,
		endTime:   pEndTime,
	}
	return *period

}

type quotaBucketData struct {
	EdgeOrgID             string
	ID                    string
	Interval              int
	TimeUnit              string //TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	QuotaType             string //Type {CALENDAR, FLEXI, ROLLING_WINDOW}
	PreciseAtSecondsLevel bool
	Period                QuotaPeriod
	StartTime             time.Time
	MaxCount              int
	BucketType            string // SyncDistributed, AsyncDistributed, NonDistributed
}

type QuotaBucket struct {
	quotaBucketData
}

func NewQuotaBucket(edgeOrgID string, id string, interval int,
	timeUnit string, quotaType string, preciseAtSecondsLevel bool, period QuotaPeriod,
	startTime int64, maxCount int, bucketType string) (*QuotaBucket, error) {

	fromUNIXTime := time.Unix(startTime, 0)

	quotaBucketDataStruct := &quotaBucketData{
		EdgeOrgID:             edgeOrgID,
		ID:                    id,
		Interval:              interval,
		TimeUnit:              timeUnit,
		QuotaType:             quotaType,
		PreciseAtSecondsLevel: preciseAtSecondsLevel,
		Period:                period,
		StartTime:             fromUNIXTime,
		MaxCount:              maxCount,
		BucketType:            bucketType,
	}

	quotaBucket := &QuotaBucket{
		quotaBucketData: *quotaBucketDataStruct,
	}

	return quotaBucket, nil

}

func (q *QuotaBucket) GetEdgeOrgID() string {
	return q.quotaBucketData.EdgeOrgID
}

func (q *QuotaBucket) GetID() string {
	return q.quotaBucketData.ID
}

func (q *QuotaBucket) GetInterval() int {
	return q.quotaBucketData.Interval
}

func (q *QuotaBucket) GetStartTime() time.Time {
	return q.quotaBucketData.StartTime
}

func (q *QuotaBucket) GetQuotaType() string {
	return q.quotaBucketData.QuotaType
}

func (q *QuotaBucket) GetPeriod() (*QuotaPeriod, error) {
	if q.quotaBucketData.QuotaType == QuotaTypeRollingWindow {
		qRWType := RollingWindowQuotaDescriptorType{}
		err := qRWType.SetCurrentPeriod(q)
		if err != nil {
			return nil, err
		}
	}
	return &q.quotaBucketData.Period, nil
}

func (q *QuotaBucket) GetMaxCount() int {
	return q.quotaBucketData.MaxCount
}

func (q *QuotaBucket) GetBucketType() string {
	return q.quotaBucketData.BucketType
}

func (q *quotaBucketData) SetPeriod(startTime time.Time, endTime time.Time) {
	q.Period = QuotaPeriod{inputStartTime: q.StartTime,
		startTime: startTime,
		endTime:   endTime,
	}
}

func (q *QuotaBucket) setCurrentPeriod() error {

	qDescriptorType, err := GetQuotaTypeHandler(q.GetQuotaType())
	if err != nil {
		return err
	}
	return qDescriptorType.SetCurrentPeriod(q)

}

func (period *QuotaPeriod) IsCurrentPeriod(qBucket *QuotaBucket) bool {
	if qBucket != nil && qBucket.GetBucketType() != "" {
		if qBucket.GetBucketType() == QuotaTypeRollingWindow {
			return (period.inputStartTime.Equal(time.Now()) || period.inputStartTime.Before(time.Now()))
		}
		return ((period.inputStartTime.Equal(time.Now()) || period.inputStartTime.Before(time.Now())) &&
			period.startTime.String() != "" && period.endTime.String() != "" &&
			period.startTime.Before(time.Now()) && period.startTime.Equal(time.Now()) &&
			period.endTime.Before(time.Now()) && period.startTime.Equal(time.Now()))
	}
	return false
}

func (q *QuotaBucket) GetQuotaCount() (int, error) {
	err := q.setCurrentPeriod()
	if err != nil {
		return 0, errors.New("error setCurrentPeriod(): " + err.Error())
	}
	period, err := q.GetPeriod()
	if err != nil {
		return 0, errors.New("error getting period: " + err.Error())
	}
	fmt.Println("period set: ", period)

	//todo API call to counter service using Period start end and keyspace.

	return 10, nil
}

func (q *QuotaBucket) IncrementQuota() (int, error) {
	//todo
	return 0, nil
}
