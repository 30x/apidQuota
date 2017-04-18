package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/constants"
	"github.com/30x/apidQuota/globalVariables"
	"strings"
	"time"
)

var (
	acceptedTimeUnitList map[string]bool
	acceptedTypeList     map[string]bool
)

func init() {

	acceptedTimeUnitList = map[string]bool{constants.TimeUnitSECOND: true,
		constants.TimeUnitMINUTE: true, constants.TimeUnitHOUR: true,
		constants.TimeUnitDAY: true, constants.TimeUnitWEEK: true, constants.TimeUnitMONTH: true}
	acceptedTypeList = map[string]bool{constants.QuotaTypeCalendar: true,
		constants.QuotaTypeRollingWindow: true}

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
	return false, errors.New(constants.InvalidQuotaPeriod + " : startTime in the period must be before endTime")

}

type quotaBucketData struct {
	EdgeOrgID             string
	ID                    string
	Interval              int
	TimeUnit              string //TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	QuotaType             string //QuotaType {CALENDAR, FLEXI, ROLLING_WINDOW}
	PreciseAtSecondsLevel bool
	Period                QuotaPeriod
	StartTime             time.Time
	MaxCount              int64
	Weight                int64
	Distributed           bool
	Synchronous           bool
	SyncTimeInSec         int64
	SyncMessageCount      int64
	AsyncMessageCounter int64
	QTicker *time.Ticker

}

type QuotaBucket struct {
	quotaBucketData
}

func NewQuotaBucket(edgeOrgID string, id string, interval int,
	timeUnit string, quotaType string, preciseAtSecondsLevel bool,
	startTime int64, maxCount int64, weight int64, distributed bool,
	synchronous bool, syncTimeInSec int64, syncMessageCount int64) (*QuotaBucket, error) {

	fromUNIXTime := time.Unix(startTime, 0)

	quotaBucketDataStruct := quotaBucketData{
		EdgeOrgID:             edgeOrgID,
		ID:                    id,
		Interval:              interval,
		TimeUnit:              timeUnit,
		QuotaType:             quotaType,
		PreciseAtSecondsLevel: preciseAtSecondsLevel,
		StartTime:             fromUNIXTime,
		MaxCount:              maxCount,
		Weight:                weight,
		Distributed:           distributed,
		Synchronous:           synchronous,
		SyncTimeInSec:         syncTimeInSec,
		SyncMessageCount:      syncMessageCount,
		AsyncMessageCounter: int64(-1),
		QTicker: &time.Ticker{},
	}

	quotaBucket := &QuotaBucket{
		quotaBucketData: quotaBucketDataStruct,
	}

	err := quotaBucket.setCurrentPeriod()
	if err != nil {
		return nil, err
	}

	//for async SetAsyncMessageCounter to 0 and also start the scheduler
	if distributed && !synchronous{
		quotaBucket.SetAsyncMessageCounter(0)
		quotaBucket.quotaBucketData.QTicker =  time.NewTicker(time.Second)
		go func() {
			count := 0
			for t := range quotaBucket.quotaBucketData.QTicker.C {
				globalVariables.Log.Debug("t: : ", t.String())
				if count > 10 {
					quotaBucket.getTicker().Stop()
				}
				count += 1
			}
		}()
	}

	return quotaBucket, nil

}

func (q *QuotaBucket) Validate() error {

	//check valid quotaTimeUnit
	if ok := IsValidTimeUnit(strings.ToLower(q.GetTimeUnit())); !ok {
		return errors.New(constants.InvalidQuotaTimeUnitType)
	}

	if ok := IsValidType(strings.ToLower(q.GetType())); !ok {
		return errors.New(constants.InvalidQuotaBucketType)
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

func (q *QuotaBucket) GetType() string {
	return q.quotaBucketData.QuotaType
}

func (q *QuotaBucket) GetIsPreciseAtSecondsLevel() bool {
	return q.quotaBucketData.PreciseAtSecondsLevel
}

func (q *QuotaBucket) GetMaxCount() int64 {
	return q.quotaBucketData.MaxCount
}

func (q *QuotaBucket) GetWeight() int64 {
	return q.quotaBucketData.Weight
}

func (q *QuotaBucket) IsDistrubuted() bool {
	return q.quotaBucketData.Distributed
}

func (q *QuotaBucket) IsSynchronous() bool {
	return q.quotaBucketData.Synchronous
}

func (qbucket *QuotaBucket) SetAsyncMessageCounter(count int64) {
	qbucket.quotaBucketData.AsyncMessageCounter = count
}

func (q *QuotaBucket) getTicker() *time.Ticker {
	return q.quotaBucketData.QTicker
}
//Calls setCurrentPeriod if DescriptorType is 'rollingWindow' or period.endTime is before now().
// It is required to setPeriod while incrementing the count.
func (q *QuotaBucket) GetPeriod() (*QuotaPeriod, error) {
	if q.quotaBucketData.QuotaType == constants.QuotaTypeRollingWindow {
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
	if q.quotaBucketData.QuotaType == constants.QuotaTypeRollingWindow {
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

	qDescriptorType, err := GetQuotaTypeHandler(q.GetType())
	if err != nil {
		return err
	}
	return qDescriptorType.SetCurrentPeriod(q)

}

func (period *QuotaPeriod) IsCurrentPeriod(qBucket *QuotaBucket) bool {
	if qBucket != nil && qBucket.GetType() != "" {
		if qBucket.GetType() == constants.QuotaTypeRollingWindow {
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

func (q *QuotaBucket) ResetQuotaLimit() (*QuotaBucketResults, error) {
	bucketType, err := GetQuotaBucketHandler(q)
	if err != nil {
		return nil, errors.New("error getting quotaBucketHandler: " + err.Error())
	}

	return bucketType.resetQuotaForCurrentPeriod(q)

}

func (q *QuotaBucket) IncrementQuotaLimit() (*QuotaBucketResults, error) {

	qBucketHandler, err := GetQuotaBucketHandler(q)
	if err != nil {
		return nil, errors.New("error getting quotaBucketHandler: " + err.Error())
	}

	return qBucketHandler.incrementQuotaCount(q)

}

func IsValidTimeUnit(timeUnit string) bool {
	if _, ok := acceptedTimeUnitList[timeUnit]; ok {
		return true
	}
	return false
}

func IsValidType(qtype string) bool {
	if _, ok := acceptedTypeList[qtype]; ok {
		return true
	}
	return false
}
