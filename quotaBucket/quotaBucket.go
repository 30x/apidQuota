package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/services"
	"strings"
	"time"
)

const (
	//add to acceptedTimeUnitList in init() if case any other new timeUnit is added
	TimeUnitSECOND = "second"
	TimeUnitMINUTE = "minute"
	TimeUnitHOUR   = "hour"
	TimeUnitDAY    = "day"
	TimeUnitWEEK   = "week"
	TimeUnitMONTH  = "month"

	//add to acceptedBucketTypeList in init() if case any other new bucketType is added
	QuotaBucketTypeSynchronous    = "synchronous"
	QuotaBucketTypeAsynchronous   = "asynchronous"
	QuotaBucketTypeNonDistributed = "nonDistributed"
	//todo: Add other accepted bucketTypes

	//errors
	InvalidQuotaTimeUnitType   = "invalidQuotaTimeUnitType"
	InvalidQuotaDescriptorType = "invalidQuotaTimeUnitType"
	InvalidQuotaBucketType     = "invalidQuotaBucketType"
	InvalidQuotaPeriod         = "invalidQuotaPeriod"
)

var (
	acceptedTimeUnitList   map[string]bool
	acceptedBucketTypeList map[string]bool
)

func init() {

	acceptedTimeUnitList = map[string]bool{TimeUnitSECOND: true,
		TimeUnitMINUTE: true, TimeUnitHOUR: true,
		TimeUnitDAY: true, TimeUnitWEEK: true, TimeUnitMONTH: true}
	acceptedBucketTypeList = map[string]bool{QuotaBucketTypeSynchronous: true,
		QuotaBucketTypeAsynchronous: true, QuotaBucketTypeNonDistributed: true} //todo: add other accpeted bucketTypes

}

type quotaPeriodData struct {
	inputStartTime time.Time
	startTime      time.Time
	endTime        time.Time
}

type QuotaPeriod struct {
	quotaPeriodData
}

func NewQuotaPeriod(inputStartTime int64, startTime int64, endTime int64) QuotaPeriod {
	pInStartTime := time.Unix(inputStartTime, 0)
	pStartTime := time.Unix(startTime, 0)
	pEndTime := time.Unix(endTime, 0)

	periodData := quotaPeriodData{
		inputStartTime: pInStartTime,
		startTime:      pStartTime,
		endTime:        pEndTime,
	}

	period := QuotaPeriod{
		quotaPeriodData: periodData,
	}

	return period

}

func (qp *QuotaPeriod) GetPeriodInputStartTime() time.Time {
	return qp.quotaPeriodData.inputStartTime
}

func (qp *QuotaPeriod) GetPeriodStartTime() time.Time {
	return qp.quotaPeriodData.startTime
}

func (qp *QuotaPeriod) GetPeriodEndTime() time.Time {
	return qp.quotaPeriodData.endTime
}

func (qp *QuotaPeriod) Validate() (bool, error) {
	if qp.startTime.Before(qp.endTime) {
		return true, nil
	}
	return false, errors.New(InvalidQuotaPeriod + " : startTime in the period must be before endTime")

}

type quotaBucketData struct {
	EdgeOrgID             string
	ID                    string
	Interval              int
	TimeUnit              string //TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	QuotaDescriptorType   string //Type {CALENDAR, FLEXI, ROLLING_WINDOW}
	PreciseAtSecondsLevel bool
	Period                QuotaPeriod
	StartTime             time.Time
	MaxCount              int64
	BucketType            string // SyncDistributed, AsyncDistributed, NonDistributed
	Weight                int64
}

type QuotaBucket struct {
	quotaBucketData
}

func NewQuotaBucket(edgeOrgID string, id string, interval int,
	timeUnit string, quotaType string, preciseAtSecondsLevel bool,
	startTime int64, maxCount int64, bucketType string, weight int64) (*QuotaBucket, error) {

	fromUNIXTime := time.Unix(startTime, 0)

	quotaBucketDataStruct := quotaBucketData{
		EdgeOrgID:             edgeOrgID,
		ID:                    id,
		Interval:              interval,
		TimeUnit:              timeUnit,
		QuotaDescriptorType:   quotaType,
		PreciseAtSecondsLevel: preciseAtSecondsLevel,
		StartTime:             fromUNIXTime,
		MaxCount:              maxCount,
		BucketType:            bucketType,
		Weight:                weight,
	}

	quotaBucket := &QuotaBucket{
		quotaBucketData: quotaBucketDataStruct,
	}

	err := quotaBucket.setCurrentPeriod()
	if err != nil {
		return nil, err
	}
	return quotaBucket, nil

}

func (q *QuotaBucket) Validate() error {

	//check valid quotaTimeUnit
	if ok := IsValidTimeUnit(strings.ToLower(q.GetTimeUnit())); !ok {
		return errors.New(InvalidQuotaTimeUnitType)
	}

	//check valid quotaBucketType
	if ok := IsValidQuotaBucketType(strings.ToLower(q.GetBucketType())); !ok {
		return errors.New(InvalidQuotaBucketType)
	}

	//check if the period is valid
	period, err := q.GetQuotaBucketPeriod()
	if err != nil {
		return err
	}
	if ok, err := period.Validate(); !ok {
		return errors.New("invalid Period: " + err.Error())
	}

	return nil
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

func (q *QuotaBucket) GetTimeUnit() string {
	return q.quotaBucketData.TimeUnit
}

func (q *QuotaBucket) GetStartTime() time.Time {
	return q.quotaBucketData.StartTime
}

func (q *QuotaBucket) GetQuotaDescriptorType() string {
	return q.quotaBucketData.QuotaDescriptorType
}

func (q *QuotaBucket) GetIsPreciseAtSecondsLevel() bool {
	return q.quotaBucketData.PreciseAtSecondsLevel
}

func (q *QuotaBucket) GetMaxCount() int64 {
	return q.quotaBucketData.MaxCount
}

func (q *QuotaBucket) GetBucketType() string {
	return q.quotaBucketData.BucketType
}

func (q *QuotaBucket) GetWeight() int64 {
	return q.quotaBucketData.Weight
}


//Calls setCurrentPeriod if DescriptorType is 'rollingWindow' or period.endTime is before now().
// It is required to setPeriod while incrementing the count.
func (q *QuotaBucket) GetPeriod() (*QuotaPeriod, error) {
	if q.quotaBucketData.QuotaDescriptorType == QuotaTypeRollingWindow {
		qRWType := RollingWindowQuotaDescriptorType{}
		err := qRWType.SetCurrentPeriod(q)
		if err != nil {
			return nil, err
		}
	}

	period, err := q.GetQuotaBucketPeriod()
	if err != nil {
		return nil, err
	}

	//setCurrentPeriod if endTime > time.now()
	if period == nil || period.endTime.Before(time.Now().UTC()) || period.endTime.Equal(time.Now().UTC()) {
		if err := q.setCurrentPeriod(); err != nil {
			return nil, err
		}
	}

	return &q.quotaBucketData.Period, nil
}

//setCurrentPeriod only for rolling window else just return the value of QuotaPeriod.
func (q *QuotaBucket) GetQuotaBucketPeriod() (*QuotaPeriod, error) {
	if q.quotaBucketData.QuotaDescriptorType == QuotaTypeRollingWindow {
		qRWType := RollingWindowQuotaDescriptorType{}
		err := qRWType.SetCurrentPeriod(q)
		if err != nil {
			return nil, err
		}
	}
	return &q.quotaBucketData.Period, nil
}

func (q *QuotaBucket) SetPeriod(startTime time.Time, endTime time.Time) {
	periodData := quotaPeriodData{
		inputStartTime: q.GetStartTime(),
		startTime:      startTime,
		endTime:        endTime,
	}

	period := QuotaPeriod{
		quotaPeriodData: periodData,
	}

	q.quotaBucketData.Period = period
}

func (q *QuotaBucket) setCurrentPeriod() error {

	qDescriptorType, err := GetQuotaDescriptorTypeHandler(q.GetQuotaDescriptorType())
	if err != nil {
		return err
	}
	return qDescriptorType.SetCurrentPeriod(q)

}

func (period *QuotaPeriod) IsCurrentPeriod(qBucket *QuotaBucket) bool {
	if qBucket != nil && qBucket.GetBucketType() != "" {
		if qBucket.GetQuotaDescriptorType() == QuotaTypeRollingWindow {
			return (period.inputStartTime.Equal(time.Now().UTC()) || period.inputStartTime.Before(time.Now().UTC()))
		}

		return (period.inputStartTime.Equal(time.Now().UTC()) || period.inputStartTime.Before(time.Now().UTC())) &&
			period.startTime.String() != "" &&
			period.endTime.String() != "" &&
			period.startTime.Before(period.endTime) &&
			(period.startTime.Equal(time.Now().UTC()) || period.startTime.Before(time.Now().UTC())) &&
			(period.endTime.Equal(time.Now().UTC()) || period.endTime.After(time.Now().UTC()))
	}
	return false
}

func (q *QuotaBucket) IncrementQuotaLimit() (*QuotaBucketResults, error) {
	maxCount := q.GetMaxCount()
	exceededCount := false
	allowedCount := int64(0)
	weight := q.GetWeight()
	period, err := q.GetPeriod()
	if err != nil {
		return nil, errors.New("error getting period: " + err.Error())
	}

	//first retrieve the count from counter service.
	currentCount, err := services.IncrementAndGetCount(q.GetEdgeOrgID(), q.GetID(), 0, period.GetPeriodStartTime().Unix(), period.GetPeriodEndTime().Unix())
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

func IsValidTimeUnit(timeUnit string) bool {
	if _, ok := acceptedTimeUnitList[timeUnit]; ok {
		return true
	}
	return false
}

func IsValidQuotaBucketType(bucketType string) bool {
	if _, ok := acceptedBucketTypeList[bucketType]; ok {
		return true
	}
	return false
}
