package quotaBucket

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	QuotaTypeCalendar      = "calendar"      // after start time
	QuotaTypeFlexi         = "flexi"         //after first request
	QuotaTypeRollingWindow = "rollingWindow" // in the past "window" time
)

type QuotaDescriptorType interface {
	SetCurrentPeriod(bucket *QuotaBucket) error
}

func GetQuotaTypeHandler(qType string) (QuotaDescriptorType, error) {
	var qDescriptor QuotaDescriptorType
	quotaType := strings.ToLower(strings.TrimSpace(qType))
	switch quotaType {
	case QuotaTypeCalendar:
		qDescriptor = &CanlendarQuotaDescriporType{}
		return qDescriptor, nil
	case QuotaTypeFlexi:
		qDescriptor = &FlexiQuotaDescriptorType{}
		return qDescriptor, nil
	case QuotaTypeRollingWindow:
		qDescriptor = &RollingWindowQuotaDescriptorType{}
		return qDescriptor, nil
	default:
		return nil, errors.New("Ignoring unrecognized quota type in request: " + qType)

	}
}

type CanlendarQuotaDescriporType struct{}

func (c CanlendarQuotaDescriporType) SetCurrentPeriod(qbucket *QuotaBucket) error {
	var err error
	startTime := qbucket.GetStartTime()
	currentPeriod, err := qbucket.GetPeriod()
	if err != nil {
		return err
	}

	if startTime.Before(time.Now()) || startTime.Equal(time.Now()) {
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
	now := time.Now()
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
		//currentStart = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		//currentEnd = currentStart.AddDate(0, 0, 7*qbucket.Interval)
		break
	case TimeUnitMONTH:
		currentStart = time.Date(now.Year(), now.Month(), 0, 0, 0, 0, 0, time.UTC)
		currentEnd = currentStart.AddDate(0, 1*qbucket.Interval, 0)
		break
	}

	qbucket.SetPeriod(currentStart, currentEnd)
	fmt.Println("inside calendat set period: ", qbucket.quotaBucketData.Period)
	return nil
}

type FlexiQuotaDescriptorType struct{}

func (c FlexiQuotaDescriptorType) SetCurrentPeriod(qbucket *QuotaBucket) error {
	//yet to implement
	return nil
}

type RollingWindowQuotaDescriptorType struct{}

func (c RollingWindowQuotaDescriptorType) SetCurrentPeriod(qbucket *QuotaBucket) error {
	//yet to implement
	return nil
}
