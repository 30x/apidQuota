package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/constants"
	"github.com/30x/apidQuota/globalVariables"
	"github.com/30x/apidQuota/services"
	"strings"
	"sync/atomic"
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

type quotaPeriod struct {
	inputStartTime time.Time
	startTime      time.Time
	endTime        time.Time
}

func (qp *quotaPeriod) GetPeriodInputStartTime() time.Time {

	return qp.inputStartTime
}

func (qp *quotaPeriod) GetPeriodStartTime() time.Time {

	return qp.startTime
}

func (qp *quotaPeriod) GetPeriodEndTime() time.Time {

	return qp.endTime
}

func (qp *quotaPeriod) Validate() (bool, error) {

	if qp.startTime.Before(qp.endTime) {
		return true, nil
	}
	return false, errors.New(constants.InvalidQuotaPeriod + " : startTime in the period must be before endTime")

}

type aSyncQuotaBucket struct {
	syncTimeInSec          int64 // sync time in seconds.
	syncMessageCount       int64 //set to -1 if the aSyncQuotaBucket should syncTimeInSec
	asyncLocalMessageCount int64
	asyncCounter           *[]int64
	asyncGLobalCount       int64
	initialized            bool
	qTicker                *time.Ticker
}

func (qAsync *aSyncQuotaBucket) getAsyncSyncTime() (int64, error) {

	if qAsync != nil {
		return qAsync.syncTimeInSec, nil
	}
	return 0, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) getAsyncSyncMessageCount() (int64, error) {

	if qAsync != nil {
		return qAsync.syncMessageCount, nil
	}
	return 0, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) getAsyncLocalMessageCount() (int64, error) {

	if qAsync != nil {
		return qAsync.asyncLocalMessageCount, nil
	}
	return 0, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) addToAsyncLocalMessageCount(count int64) error {

	if qAsync != nil {
		atomic.AddInt64(&qAsync.asyncLocalMessageCount, count)
	}
	return errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) getAsyncGlobalCount() (int64, error) {

	if qAsync != nil {
		return qAsync.asyncGLobalCount, nil
	}
	return 0, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) getAsyncIsInitialized() (bool, error) {

	if qAsync != nil {
		return qAsync.initialized, nil
	}
	return false, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) getAsyncQTicker() (*time.Ticker, error) {

	if qAsync != nil {
		return qAsync.qTicker, nil
	}
	return nil, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (qAsync *aSyncQuotaBucket) getAsyncCounter() (*[]int64, error) {

	if qAsync != nil {
		return qAsync.asyncCounter, nil
	}
	return nil, errors.New(constants.AsyncQuotaBucketEmpty)
}

func (aSyncbucket *aSyncQuotaBucket) addToCounter(weight int64) error {

	if aSyncbucket == nil {
		return errors.New(constants.AsyncQuotaBucketEmpty)
	}

	*aSyncbucket.asyncCounter = append(*aSyncbucket.asyncCounter, weight)
	return nil
}

func (aSyncbucket *aSyncQuotaBucket) getCount(q *QuotaBucket, period *quotaPeriod) (int64, error) {

	var gcount int64
	var err error
	if !aSyncbucket.initialized {
		gcount, err = services.IncrementAndGetCount(q.GetEdgeOrgID(), q.GetID(), 0, period.startTime.Unix(), period.endTime.Unix())
		if err != nil {
			return 0, err
		}
		aSyncbucket.asyncGLobalCount = gcount
		aSyncbucket.initialized = true
	}

	return aSyncbucket.asyncGLobalCount + aSyncbucket.asyncLocalMessageCount, nil
}

func (quotaBucketType *aSyncQuotaBucket) incrementQuotaCount(q *QuotaBucket) (*QuotaBucketResults, error) {
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

			asyncLocalMsgCount, err := aSyncBucket.getAsyncLocalMessageCount()
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
	globalCount, err := aSyncBucket.getAsyncGlobalCount()
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

type quotaBucketData struct {
	EdgeOrgID         string
	ID                string
	Interval          int
	TimeUnit          string //TimeUnit {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH}
	QuotaType         string //QuotaType {CALENDAR, FLEXI, ROLLING_WINDOW}
	StartTime         time.Time
	MaxCount          int64
	Weight            int64
	AsyncQuotaDetails *aSyncQuotaBucket
}

type QuotaBucket struct {
	quotaBucketData
}

func NewQuotaBucket(edgeOrgID string, id string, interval int,
	timeUnit string, quotaType string,
	startTime int64, maxCount int64, weight int64, syncTimeInSec int64, syncMessageCount int64) (*QuotaBucket, error) {

	fromUNIXTime := time.Unix(startTime, 0)
	quotaBucketDataStruct := quotaBucketData{
		EdgeOrgID:         edgeOrgID,
		ID:                id,
		Interval:          interval,
		TimeUnit:          timeUnit,
		QuotaType:         quotaType,
		StartTime:         fromUNIXTime,
		MaxCount:          maxCount,
		Weight:            weight,
		AsyncQuotaDetails: nil,
	}

	quotaBucket := &QuotaBucket{
		quotaBucketData: quotaBucketDataStruct,
	}

	var quotaTicker int64
	//ensure just one of syncTimeInSec and syncMessageCount is set.
	if syncTimeInSec > -1 && syncMessageCount > -1 {
		return nil, errors.New("both syncTimeInSec and syncMessageCount canot be set. only one of them should be set.")
	}
	//set default syncTime for AsyncQuotaBucket.
	//for aSyncQuotaBucket with 'syncMessageCount' the ticker is invoked with DefaultQuotaSyncTime
	quotaTicker = constants.DefaultQuotaSyncTime

	if syncTimeInSec > 0 { //if sync with counter service periodically
		quotaTicker = syncTimeInSec
	}

	counter := make([]int64, 0)
	newAsyncQuotaDetails := &aSyncQuotaBucket{
		syncTimeInSec:          syncTimeInSec,
		syncMessageCount:       syncMessageCount,
		asyncCounter:           &counter,
		asyncGLobalCount:       constants.DefaultCount,
		asyncLocalMessageCount: constants.DefaultCount,
		initialized:            false,
		qTicker:                time.NewTicker(time.Duration(time.Second.Nanoseconds() * quotaTicker)),
	}

	quotaBucket.setAsyncQuotaBucket(newAsyncQuotaDetails)
	go func() {
		aSyncBucket := quotaBucket.GetAsyncQuotaBucket()
		if aSyncBucket != nil {
			exitCount := int64(0)
			qticker, _ := aSyncBucket.getAsyncQTicker()
			for t := range qticker.C {
				globalVariables.Log.Debug("t: ", t.String())
				if len(*aSyncBucket.asyncCounter) == 0 {
					exitCount += 1
				}
				period, err := quotaBucket.GetPeriod()
				if err != nil {
					globalVariables.Log.Error("error getting period for: ", err.Error(), "for quotaBucket: ", quotaBucket)
					qticker.Stop()
					continue
				}
				//sync with counterService.
				err = internalRefresh(quotaBucket, period)
				if err != nil {
					globalVariables.Log.Error("error during internalRefresh: ", err.Error(), "for quotaBucket: ", quotaBucket)
					qticker.Stop()
					continue
				}

				if exitCount > 3 {
					removeFromCache(quotaBucket.GetEdgeOrgID()+
						constants.CacheKeyDelimiter+quotaBucket.GetID(),
						quotaCache[quotaBucket.GetEdgeOrgID()+constants.CacheKeyDelimiter+quotaBucket.GetID()])
					qticker.Stop()
				}
			}
		} else {
			globalVariables.Log.Error("aSyncBucketDetails are empty for the given quotaBucket: ", quotaBucket)
		}
	}()

	return quotaBucket, nil

}

func (q *QuotaBucket) Validate() error {

	//check valid quotaTimeUnit
	if ok := IsValidTimeUnit(strings.ToLower(q.GetTimeUnit())); !ok {
		return errors.New(constants.InvalidQuotaTimeUnitType)
	}

	if ok := IsValidType(strings.ToLower(q.GetType())); !ok {
		return errors.New(constants.InvalidQuotaType)
	}

	//check if the period is valid
	period, err := q.GetPeriod()
	if err != nil {
		return errors.New("error retireving Period for the quota Bucket" + err.Error())
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

//QuotaType {CALENDAR, FLEXI, ROLLING_WINDOW}
func (q *QuotaBucket) GetType() string {
	return q.quotaBucketData.QuotaType
}

func (q *QuotaBucket) GetMaxCount() int64 {
	return q.quotaBucketData.MaxCount
}

func (q *QuotaBucket) GetWeight() int64 {
	return q.quotaBucketData.Weight
}

//setCurrentPeriod only for rolling window else just return the value of QuotaPeriod.
func (q *QuotaBucket) GetPeriod() (*quotaPeriod, error) {

	qDescriptorType, err := GetQuotaTypeHandler(q.GetType())
	if err != nil {
		return nil, err
	}
	return qDescriptorType.GetCurrentPeriod(q)

}

func (period *quotaPeriod) IsCurrentPeriod(qBucket *QuotaBucket) bool {
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

func (q *QuotaBucket) setAsyncQuotaBucket(aSyncbucket *aSyncQuotaBucket) {
	q.quotaBucketData.AsyncQuotaDetails = aSyncbucket
}

func (q *QuotaBucket) GetAsyncQuotaBucket() *aSyncQuotaBucket {
	return q.quotaBucketData.AsyncQuotaDetails
}

func (q *QuotaBucket) IncrementQuotaLimit() (*QuotaBucketResults, error) {

	return q.GetAsyncQuotaBucket().incrementQuotaCount(q)

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
