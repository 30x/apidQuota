package quotaBucket

import (
	"errors"
	"github.com/30x/apidQuota/constants"
	"strings"
	"time"
)

type QuotaDescriptorType interface {
	GetCurrentPeriod(bucket *QuotaBucket) (*quotaPeriod, error)
}

func GetQuotaTypeHandler(qType string) (QuotaDescriptorType, error) {
	var qDescriptor QuotaDescriptorType
	quotaType := strings.ToLower(strings.TrimSpace(qType))
	switch quotaType {
	case constants.QuotaTypeCalendar:
		qDescriptor = &CalendarQuotaDescriptorType{}
		return qDescriptor, nil
	case constants.QuotaTypeRollingWindow:
		qDescriptor = &RollingWindowQuotaDescriptorType{}
		return qDescriptor, nil
	default:
		return nil, errors.New(constants.InvalidQuotaType + " Quota type: " + qType + " in the request is not supported")

	}
}

type CalendarQuotaDescriptorType struct{}

func (c *CalendarQuotaDescriptorType) GetCurrentPeriod(qbucket *QuotaBucket) (*quotaPeriod, error) {

	var currentStart, currentEnd time.Time
	now := time.Now().UTC()
	timeUnit := strings.ToLower(strings.TrimSpace(qbucket.TimeUnit))
	switch timeUnit {
	case constants.TimeUnitSECOND:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC)
		secInDuration := time.Duration(int64(qbucket.Interval) * time.Second.Nanoseconds())
		currentEnd = currentStart.Add(secInDuration)
		break
	case constants.TimeUnitMINUTE:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)
		minInDuration := time.Duration(int64(qbucket.Interval) * time.Minute.Nanoseconds())
		currentEnd = currentStart.Add(minInDuration)
		break
	case constants.TimeUnitHOUR:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
		hoursInDuration := time.Duration(int64(qbucket.Interval) * time.Hour.Nanoseconds())
		currentEnd = currentStart.Add(hoursInDuration)

		break
	case constants.TimeUnitDAY:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		currentEnd = currentStart.AddDate(0, 0, 1*qbucket.Interval)
		break
	case constants.TimeUnitWEEK:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		for currentStart.Weekday() != time.Monday {
			currentStart = currentStart.AddDate(0, 0, -1)
		}
		currentEnd = currentStart.AddDate(0, 0, 7*qbucket.Interval)
		break
	case constants.TimeUnitMONTH:
		currentStart = time.Date(now.Year(), now.Month(), 0, 0, 0, 0, 0, time.UTC)
		currentEnd = currentStart.AddDate(0, qbucket.Interval, 0)
		break
	default:
		return nil, errors.New(constants.InvalidQuotaTimeUnitType + " : ignoring unrecognized timeUnit : " + timeUnit)

	}

	return &quotaPeriod{
		inputStartTime: qbucket.GetStartTime(),
		startTime:      currentStart,
		endTime:        currentEnd,
	}, nil
}

type RollingWindowQuotaDescriptorType struct{}

func (c *RollingWindowQuotaDescriptorType) GetCurrentPeriod(qbucket *QuotaBucket) (*quotaPeriod, error) {

	//yet to implement
	var currentStart, currentEnd time.Time
	currentEnd = time.Now().UTC()
	interval, err := GetIntervalDurtation(qbucket)
	if err != nil {
		return nil, errors.New("error in SetCurrentPeriod: " + err.Error())
	}
	currentStart = currentEnd.Add(-interval)
	return &quotaPeriod{
		inputStartTime: qbucket.GetStartTime(),
		startTime:      currentStart,
		endTime:        currentEnd,
	}, nil
}
func GetIntervalDurtation(qb *QuotaBucket) (time.Duration, error) {

	timeUnit := strings.ToLower(strings.TrimSpace(qb.TimeUnit))
	switch timeUnit {
	case constants.TimeUnitSECOND:
		return time.Duration(int64(qb.Interval) * time.Second.Nanoseconds()), nil
	case constants.TimeUnitMINUTE:
		return time.Duration(int64(qb.Interval) * time.Minute.Nanoseconds()), nil
	case constants.TimeUnitHOUR:
		return time.Duration(int64(qb.Interval) * time.Hour.Nanoseconds()), nil
	case constants.TimeUnitDAY:
		return time.Duration(int64(qb.Interval*24) * time.Hour.Nanoseconds()), nil
	case constants.TimeUnitWEEK:
		return time.Duration(int64(qb.Interval*24*7) * time.Hour.Nanoseconds()), nil
	case constants.TimeUnitMONTH:
		now := time.Now().UTC()
		var currentStart, currentEnd time.Time
		quotaType := strings.ToLower(strings.TrimSpace(qb.QuotaType))
		switch quotaType {
		case constants.QuotaTypeCalendar:
			currentStart = time.Date(now.Year(), now.Month(), 0, 0, 0, 0, 0, time.UTC)
			currentEnd = currentStart.AddDate(0, qb.Interval, 0)
			return currentEnd.Sub(currentStart), nil
		case constants.QuotaTypeRollingWindow:
			currentEnd = now
			currentStart = currentEnd.AddDate(0, (-1)*qb.Interval, 0)
			return currentEnd.Sub(currentStart), nil
		default:
			return time.Duration(0), errors.New(constants.InvalidQuotaBucketType + " : ignoring unrecognized quotaType : " + quotaType)

		}
	default:
		return time.Duration(0), errors.New(constants.InvalidQuotaTimeUnitType + " : ignoring unrecognized timeUnit : " + timeUnit)

	}

}
