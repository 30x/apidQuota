package quotaBucket_test

import (
	. "github.com/30x/apidQuota/quotaBucket"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strings"
	"time"
)

var _ = Describe("Test QuotaPeriod", func() {
	It("Valid NewQuotaPeriod", func() {
		//startTime before endTime
		period := NewQuotaPeriod(1492324596, 1490047028, 1492725428)
		isValid, err := period.Validate()

		if !isValid || err != nil {
			Fail("expected isValid: true and error: nil for NewQuotaPeriod")
		}
	})

	It("Invalid NewQuotaPeriod", func() {
		//startTime after endTime
		period := NewQuotaPeriod(1492324596, 1492725428, 1490047028)
		isValid, err := period.Validate()
		if err == nil || isValid {
			Fail("Expected isValid: false and error: <notNil> for invalid NewQuotaPeriod. startTime should be before endTime")
		}
	})

})

var _ = Describe("Test AcceptedQuotaTimeUnitTypes", func() {
	It("testTimeUnit", func() {
		if !IsValidTimeUnit("second") {
			Fail("second is a valid TimeUnit")
		}
		if !IsValidTimeUnit("minute") {
			Fail("minute is a valid TimeUnit")
		}
		if !IsValidTimeUnit("hour") {
			Fail("hour is a valid TimeUnit")
		}
		if !IsValidTimeUnit("day") {
			Fail("day is a valid TimeUnit")
		}
		if !IsValidTimeUnit("week") {
			Fail("week is a valid TimeUnit")
		}
		if !IsValidTimeUnit("month") {
			Fail("month is a valid TimeUnit")
		}

		//invalid type
		if IsValidTimeUnit("invalidType") {
			Fail("invalidType is a invalid TimeUnit")
		}
	})
})

var _ = Describe("Test AcceptedQuotaBucketTypes", func() {
	It("testTimeUnit", func() {
		if !IsValidQuotaBucketType("synchronous") {
			Fail("synchronous is a valid quotaBucket")
		}
		if !IsValidQuotaBucketType("asynchronous") {
			Fail("asynchronous is a valid quotaBucket")
		}
		if !IsValidQuotaBucketType("nonDistributed") {
			Fail("nonDistributed is a valid quotaBucket")
		}

		//invalid type
		if IsValidQuotaBucketType("invalidType") {
			Fail("invalidType is a invalid quotaBucket")
		}
	})
})

var _ = Describe("QuotaBucket", func() {
	It("Create with NewQuotaBucket", func() {
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		period := NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime := int64(1492324596)

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())


		//also check if all the fields are set as expected
		sTime := time.Unix(startTime, 0)
		Expect(sTime).To(Equal(quotaBucket.GetStartTime()))
		Expect(edgeOrgID).To(Equal(quotaBucket.GetEdgeOrgID()))
		Expect(id).To(Equal(quotaBucket.GetID()))
		Expect(timeUnit).To(Equal(quotaBucket.GetTimeUnit()))
		Expect(quotaType).To(Equal(quotaBucket.GetQuotaType()))
		Expect(bucketType).To(Equal(quotaBucket.GetBucketType()))
		Expect(interval).To(Equal(quotaBucket.GetInterval()))
		Expect(maxCount).To(Equal(quotaBucket.GetMaxCount()))
		Expect(preciseAtSecondsLevel).To(Equal(quotaBucket.GetPreciseAtSecondsLevel()))
		getPeriod, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		Expect(period).To(Equal(*getPeriod))

	})

	It("Test invalid quotaPeriod", func() {
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		period := NewQuotaPeriod(1492324596, 1492725428, 1490047028)
		startTime := int64(1492324596)

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		if err == nil {
			Fail("error expected but got <nil>")
		}
		if !strings.Contains(err.Error(), InvalidQuotaPeriod) {
			Fail("expected: " + InvalidQuotaPeriod + " in the error but got: " + err.Error())
		}

	})

	It("Test invalid timeUnitType", func() {
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "invalidTimeUnit"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		period := NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime := int64(1492324596)

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		if err == nil {
			Fail("error expected but got <nil>")
		}
		if err.Error() != InvalidQuotaTimeUnitType {
			Fail("expected: " + InvalidQuotaBucketType + "but got: " + err.Error())
		}

	})

	It("Test invalid quotaBucketType", func() {
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "invalidQuotaBucket"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		period := NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime := time.Now().AddDate(0,-1,0).Unix()

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		if err == nil {
			Fail("error expected but got <nil>")
		}
		if err.Error() != InvalidQuotaBucketType {
			Fail("expected: " + InvalidQuotaBucketType + "but got: " + err.Error())
		}

	})

})




var _ = Describe("IsCurrentPeriod", func(){
	It("Test RollingType Window Valid TestCase", func() {

		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "rollingwindow"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		//InputStart time is before now
		period := NewQuotaPeriod(time.Now().AddDate(0, -1,0).Unix(),
			time.Now().AddDate(0,0, -1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime := time.Now().AddDate(0,-1,0).Unix()

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}

		//InputStart time is now
		period = NewQuotaPeriod(time.Now().Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime = time.Now().Unix()
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())


		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}
	})

	It("Test RollingType Window InValid TestCase", func() {

		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "rollingwindow"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		//InputStart time is after now.
		period := NewQuotaPeriod(time.Now().AddDate(0,1,0).Unix(),
			time.Now().AddDate(0,1,0).Unix(), time.Now().AddDate(0,0,1).Unix())
		startTime := time.Now().AddDate(0,1,0).Unix()

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())


		if ok := period.IsCurrentPeriod(quotaBucket); ok{
			Fail("Exprected true, returned: false")
		}

		//endTime before startTime in period
		startTime = time.Now().AddDate(0,-1,0).Unix()
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		quotaBucket.SetPeriod(time.Now(), time.Now().AddDate(0,1,0))
		if ok := period.IsCurrentPeriod(quotaBucket); ok{
			Fail("Exprected false, returned: true")
		}
	})


	It("Test NonRollingType Window Valid TestCases", func() {

		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true


		//InputStart time is before now
		period := NewQuotaPeriod(time.Now().AddDate(0, -1,0).Unix(),
			time.Now().AddDate(0,0, -1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime := time.Now().AddDate(0,-1,0).Unix()

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}


		//InputStart time is now
		period = NewQuotaPeriod(time.Now().Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime = time.Now().Unix()
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}

		//start Time in period is before now
		startTime = time.Now().Unix()
		period = NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}

		//start Time in period is now
		startTime = time.Now().Unix()
		period = NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().Unix(),
			time.Now().AddDate(0,1,0).Unix())
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}

		//end Time in period is now // cant set end time to now and tes.. by the time it evaluates isCurrentPeriod the period.endTime will be before time.now()
		//fmt.Println("entTIme is now : ")
		//startTime = time.Now().Unix()
		//period = NewQuotaPeriod(time.Now().AddDate(0,-1,-1).Unix(),
		//	time.Now().AddDate(0,0,-1).Unix(),
		//	time.Now().Unix())
		//quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		//err = quotaBucket.Validate()
		//if err != nil {
		//	Fail("no error expected but got error: " + err.Error())
		//}
		//
		//period.IsCurrentPeriod(quotaBucket)
		//if ok := period.IsCurrentPeriod(quotaBucket); !ok{
		//	Fail("Exprected true, returned: false")
		//}

		//end Time in period is after now
		startTime = time.Now().Unix()
		period = NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,0,-1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}

		//start time in period is before end time
		startTime = time.Now().Unix()
		period = NewQuotaPeriod(time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,-1,0).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok{
			Fail("Exprected true, returned: false")
		}

	})

	It("Test Non RollingType Window InValid TestCase", func() {

		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true

		//InputStart time is after now.
		period := NewQuotaPeriod(time.Now().AddDate(0,1,0).Unix(),
			time.Now().AddDate(0,1,0).Unix(), time.Now().AddDate(1,0,1).Unix())
		startTime := time.Now().AddDate(0,1,0).Unix()

		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		if ok := period.IsCurrentPeriod(quotaBucket); ok{
			Fail("Exprected true, returned: false")
		}


		//endTime is before start time
		startTime = time.Now().AddDate(0,-1,0).Unix()
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now(), time.Now().AddDate(0,-1,0))

		if ok := period.IsCurrentPeriod(quotaBucket); ok{
			Fail("Exprected true, returned: false")
		}

		//start time in period after now
		quotaBucket.SetPeriod(time.Now().AddDate(0,1,0), time.Now().AddDate(1,1,0))

		if ok := period.IsCurrentPeriod(quotaBucket); ok{
			Fail("Exprected true, returned: false")
		}

		//end time in period is before now
		quotaBucket.SetPeriod(time.Now().AddDate(-1,-1,0), time.Now().AddDate(0,-1,0))

		if ok := period.IsCurrentPeriod(quotaBucket); ok{
			Fail("Exprected true, returned: false")
		}

	})
})


var _ = Describe("Test GetPeriod and setCurrentPeriod", func() {
	It("Valid GetPeriod", func() {
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "rollingwindow"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		//InputStart time is before now
		period := NewQuotaPeriod(time.Now().AddDate(0, -1,0).Unix(),
			time.Now().AddDate(0,0, -1).Unix(),
			time.Now().AddDate(0,1,0).Unix())
		startTime := time.Now().AddDate(0,-1,0).Unix()
		quotaBucket := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		err := quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())
		qPeriod, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())

		// check if the rolling window was set properly
		Expect(qPeriod.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !qPeriod.GetPeriodEndTime().After(qPeriod.GetPeriodStartTime()){
			Fail("Rolling Window was not set as expected")
		}
		intervalDuration := qPeriod.GetPeriodEndTime().Sub(qPeriod.GetPeriodStartTime())
		expectedDuration, err := GetIntervalDurtation(quotaBucket)
		Expect(intervalDuration).Should(Equal(expectedDuration))


		//for non rolling Type window do not setCurrentPeriod as endTime is > time.now.
		quotaType = "calendar"
		pstartTime := time.Now().AddDate(0,-1,0)
		pendTime := time.Now().AddDate(0,1,0)
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		quotaBucket.SetPeriod(pstartTime, pendTime)
		qPeriod, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(qPeriod.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !qPeriod.GetPeriodEndTime().After(qPeriod.GetPeriodStartTime()){
			Fail("Rolling Window was not set as expected")
		}

		//for non rolling Type window setCurrentPeriod as endTime is < time.now.
		quotaType = "calendar"
		pstartTime = time.Now().AddDate(0,-1,0)
		pendTime = time.Now().AddDate(0,-1,0)
		quotaBucket = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, period, startTime, maxCount, bucketType)
		quotaBucket.SetPeriod(pstartTime, pendTime)
		qPeriod, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(qPeriod.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !qPeriod.GetPeriodEndTime().After(qPeriod.GetPeriodStartTime()){
			Fail("Rolling Window was not set as expected")
		}
		intervalDuration = qPeriod.GetPeriodEndTime().Sub(qPeriod.GetPeriodStartTime())
		expectedDuration, err = GetIntervalDurtation(quotaBucket)
		Expect(intervalDuration).Should(Equal(expectedDuration))



	})

})

