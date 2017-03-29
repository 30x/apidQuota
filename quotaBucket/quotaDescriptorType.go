package quotaBucket

import (
	"errors"
	"strings"
	"time"
)

const (
	QuotaTypeCalendar      = "calendar"      // after start time
	QuotaTypeFlexi         = "flexi"         //after first request
	QuotaTypeRollingWindow = "rollingwindow" // in the past "window" time
)

type QuotaDescriptorType interface {
	SetCurrentPeriod(bucket *QuotaBucket) error
}

func GetQuotaDescriptorTypeHandler(qType string) (QuotaDescriptorType, error) {
	var qDescriptor QuotaDescriptorType
	quotaType := strings.ToLower(strings.TrimSpace(qType))
	switch quotaType {
	case QuotaTypeCalendar:
		qDescriptor = &CalendarQuotaDescriptorType{}
		return qDescriptor, nil
	case QuotaTypeRollingWindow:
		qDescriptor = &RollingWindowQuotaDescriptorType{}
		return qDescriptor, nil
	default:
		return nil, errors.New(InvalidQuotaDescriptorType + " Quota type " + qType + " in the request is not supported")

	}
}

type CalendarQuotaDescriptorType struct{}

func (c CalendarQuotaDescriptorType) SetCurrentPeriod(qbucket *QuotaBucket) error {
	startTime := qbucket.GetStartTime()
	currentPeriod, err := qbucket.GetQuotaBucketPeriod()
	if err != nil {
		return err
	}
	if startTime.Before(time.Now().UTC()) || startTime.Equal(time.Now().UTC()) {
		if currentPeriod != nil {
			if currentPeriod.IsCurrentPeriod(qbucket) {
				return nil
			}
		} else {
			if currentPeriod.IsCurrentPeriod(qbucket) {
				return nil
			} else {
				qBucketHandler, err := GetQuotaBucketHandler(qbucket.BucketType)
				if err != nil {
					return errors.New("error getting QuotaBucketType: " + err.Error())
				}
				qBucketHandler.resetCount(qbucket)
			}
		}
	}

	var currentStart, currentEnd time.Time
	now := time.Now().UTC()
	timeUnit := strings.ToLower(strings.TrimSpace(qbucket.TimeUnit))
	switch timeUnit {
	case TimeUnitSECOND:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC)
		secInDuration := time.Duration(int64(qbucket.Interval) * time.Second.Nanoseconds())
		currentEnd = currentStart.Add(secInDuration)
		break
	case TimeUnitMINUTE:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)
		minInDuration := time.Duration(int64(qbucket.Interval) * time.Minute.Nanoseconds())
		currentEnd = currentStart.Add(minInDuration)
		break
	case TimeUnitHOUR:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
		hoursInDuration := time.Duration(int64(qbucket.Interval) * time.Hour.Nanoseconds())
		currentEnd = currentStart.Add(hoursInDuration)

		break
	case TimeUnitDAY:
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		currentEnd = currentStart.AddDate(0, 0, 1*qbucket.Interval)
		break
	case TimeUnitWEEK:
		//todo
		currentStart = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		for currentStart.Weekday() != time.Monday {
			currentStart = currentStart.AddDate(0, 0, -1)
		}
		currentEnd = currentStart.AddDate(0, 0, 7*qbucket.Interval)
		break
	case TimeUnitMONTH:
		currentStart = time.Date(now.Year(), now.Month(), 0, 0, 0, 0, 0, time.UTC)
		currentEnd = currentStart.AddDate(0, qbucket.Interval, 0)
		break
	default:
		return errors.New(InvalidQuotaTimeUnitType + " : ignoring unrecognized timeUnit : " + timeUnit)

	}

	qbucket.SetPeriod(currentStart, currentEnd)
	return nil
}

type RollingWindowQuotaDescriptorType struct{}

func (c RollingWindowQuotaDescriptorType) SetCurrentPeriod(qbucket *QuotaBucket) error {

	//yet to implement
	var currentStart, currentEnd time.Time
	currentEnd = time.Now().UTC()
	interval, err := GetIntervalDurtation(qbucket)
	if err != nil {
		return errors.New("error in SetCurrentPeriod: " + err.Error())
	}
	currentStart = currentEnd.Add(-interval)
	qbucket.SetPeriod(currentStart, currentEnd)

	return nil
}
func GetIntervalDurtation(qb *QuotaBucket) (time.Duration, error) {

	timeUnit := strings.ToLower(strings.TrimSpace(qb.TimeUnit))
	switch timeUnit {
	case TimeUnitSECOND:
		return time.Duration(int64(qb.Interval) * time.Second.Nanoseconds()), nil
	case TimeUnitMINUTE:
		return time.Duration(int64(qb.Interval) * time.Minute.Nanoseconds()), nil
	case TimeUnitHOUR:
		return time.Duration(int64(qb.Interval) * time.Hour.Nanoseconds()), nil
	case TimeUnitDAY:
		return time.Duration(int64(qb.Interval*24) * time.Hour.Nanoseconds()), nil
	case TimeUnitWEEK:
		return time.Duration(int64(qb.Interval*24*7) * time.Hour.Nanoseconds()), nil
	case TimeUnitMONTH:
		now := time.Now().UTC()
		var currentStart, currentEnd time.Time
		quotaType := strings.ToLower(strings.TrimSpace(qb.QuotaDescriptorType))
		switch quotaType {
		case QuotaTypeCalendar:
			currentStart = time.Date(now.Year(), now.Month(), 0, 0, 0, 0, 0, time.UTC)
			currentEnd = currentStart.AddDate(0, qb.Interval, 0)
			return currentEnd.Sub(currentStart), nil
		case QuotaTypeRollingWindow:
			currentEnd = now
			currentStart = currentEnd.AddDate(0, (-1)*qb.Interval, 0)
			return currentEnd.Sub(currentStart), nil
		default:
			return time.Duration(0), errors.New(InvalidQuotaDescriptorType + " : ignoring unrecognized quotaType : " + quotaType)

		}
	default:
		return time.Duration(0), errors.New(InvalidQuotaTimeUnitType + " : ignoring unrecognized timeUnit : " + timeUnit)

	}

}
