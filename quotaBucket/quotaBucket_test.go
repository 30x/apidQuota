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
		period := NewQuotaPeriod(time.Now().UTC().AddDate(0, -1, 0).Unix(),
			time.Now().UTC().AddDate(0, 0, -1).Unix(),
			time.Now().UTC().AddDate(0, 1, 0).Unix())
		isValid, err := period.Validate()

		if !isValid {
			Fail("expected isValid true but got false")
		}
		if err != nil {
			Fail("expected error <nil> but got " + err.Error())
		}
	})

	It("Invalid NewQuotaPeriod", func() {
		//startTime after endTime
		period := NewQuotaPeriod(time.Now().UTC().AddDate(0, -1, 0).Unix(),
			time.Now().UTC().AddDate(0, 1, 0).Unix(),
			time.Now().UTC().AddDate(0, 0, -1).Unix())
		isValid, err := period.Validate()
		if isValid {
			Fail("Expected isValid false but got true")
		}

		if err == nil {
			Fail(" Expected error but got <nil>")
		}
	})
})

var _ = Describe("Test AcceptedQuotaTimeUnitTypes", func() {
	It("testTimeUnit", func() {
		if !IsValidTimeUnit("second") {
			Fail("Expected true: second is a valid TimeUnit")
		}
		if !IsValidTimeUnit("minute") {
			Fail("Expected true: minute is a valid TimeUnit")
		}
		if !IsValidTimeUnit("hour") {
			Fail("Expected true: hour is a valid TimeUnit")
		}
		if !IsValidTimeUnit("day") {
			Fail("Expected true: day is a valid TimeUnit")
		}
		if !IsValidTimeUnit("week") {
			Fail("Expected true: week is a valid TimeUnit")
		}
		if !IsValidTimeUnit("month") {
			Fail("Expected true: month is a valid TimeUnit")
		}

		//invalid type
		if IsValidTimeUnit("invalidType") {
			Fail("Expected false: invalidType is not a valid TimeUnit")
		}
	})
})

var _ = Describe("Test AcceptedQuotaBucketTypes", func() {
	It("testTimeUnit", func() {
		if !IsValidQuotaBucketType("synchronous") {
			Fail("Expected true: synchronous is a valid quotaBucket")
		}
		if !IsValidQuotaBucketType("asynchronous") {
			Fail("Expected true: asynchronous is a valid quotaBucket")
		}
		if !IsValidQuotaBucketType("nonDistributed") {
			Fail("Expected true: nonDistributed is a valid quotaBucket")
		}

		//invalid type
		if IsValidQuotaBucketType("invalidType") {
			Fail("Expected false: invalidType is a invalid quotaBucket")
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
		startTime := time.Now().UTC().AddDate(0, -1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
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
		Expect(getPeriod.GetPeriodInputStartTime().String()).ShouldNot(BeEmpty())
		Expect(getPeriod.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(getPeriod.GetPeriodEndTime().String()).ShouldNot(BeEmpty())

	})

	//end before start
	It("Test invalid quotaPeriod", func() {
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true
		startTime := time.Now().UTC().AddDate(0, -1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC().UTC().AddDate(0, 1, 0), time.Now().AddDate(0, 0, -1))
		err = quotaBucket.Validate()
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
		startTime := time.Now().UTC().AddDate(0, -1, 0).Unix()
		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).To(HaveOccurred())

		if !strings.Contains(err.Error(), InvalidQuotaTimeUnitType) {
			Fail("expected: " + InvalidQuotaTimeUnitType + "but got: " + err.Error())
		}
		if quotaBucket != nil {
			Fail("quotaBucket returned should be nil.")
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
		startTime := time.Now().UTC().AddDate(0, -1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		if err == nil {
			Fail("error expected but got <nil>")
		}
		if !strings.Contains(err.Error(), InvalidQuotaBucketType) {
			Fail("expected: " + InvalidQuotaBucketType + " in the error message but got: " + err.Error())
		}
	})

})

var _ = Describe("IsCurrentPeriod", func() {
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
		startTime := time.Now().UTC().AddDate(0, -1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err := quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected")
		}
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
			Fail("Exprected true, returned: false")
		}

		//InputStart time is now
		startTime = time.Now().UTC().Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected")
		}
		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
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
		startTime := time.Now().UTC().AddDate(0, 1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC().AddDate(0, -1, 0), time.Now().UTC().AddDate(0, 0, 1))
		period, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		if ok := period.IsCurrentPeriod(quotaBucket); ok {
			Fail("Exprected true, returned: false")
		}

		//endTime before startTime in period
		startTime = time.Now().UTC().AddDate(0, -1, 0).Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC(), time.Now().UTC().AddDate(0, -1, 0))
		if ok := period.IsCurrentPeriod(quotaBucket); ok {
			Fail("Exprected false, returned: true")
		}
	})

	It("Test calendarType Window Valid TestCases", func() {

		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "hour"
		quotaType := "calendar"
		bucketType := "synchronous"
		interval := 1
		maxCount := 10
		preciseAtSecondsLevel := true

		//InputStart time is before now
		startTime := time.Now().UTC().UTC().AddDate(-1, -1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err := quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected but returned " + err.Error())
		}
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
			Fail("Exprected true, returned: false")
		}

		//InputStart time is now
		startTime = time.Now().UTC().Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected but returned " + err.Error())
		}
		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
			Fail("Exprected true, returned: false")
		}

		//start Time in period is before now
		startTime = time.Now().UTC().Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC().AddDate(0, 0, -1),
			time.Now().UTC().AddDate(0, 1, 0))
		period, err = quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected but returned " + err.Error())
		}
		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
			Fail("Exprected true, returned: false")
		}

		//start Time in period is now
		startTime = time.Now().UTC().Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())
		quotaBucket.SetPeriod(time.Now().UTC(),
			time.Now().UTC().AddDate(0, 1, 0))
		period, err = quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected but returned " + err.Error())
		}
		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
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
		startTime = time.Now().UTC().Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC().AddDate(0, 0, -1),
			time.Now().UTC().AddDate(0, 1, 0))
		period, err = quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected but returned " + err.Error())
		}

		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
			Fail("Exprected true, returned: false")
		}

		//start time in period is before end time
		startTime = time.Now().UTC().Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())
		quotaBucket.SetPeriod(time.Now().UTC().AddDate(0, 0, -1),
			time.Now().UTC().AddDate(0, 1, 0))
		period, err = quotaBucket.GetPeriod()
		if err != nil {
			Fail("no error expected but returned " + err.Error())
		}
		period.IsCurrentPeriod(quotaBucket)
		if ok := period.IsCurrentPeriod(quotaBucket); !ok {
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
		period := NewQuotaPeriod(time.Now().UTC().AddDate(0, 1, 0).Unix(),
			time.Now().UTC().AddDate(0, 1, 0).Unix(), time.Now().AddDate(1, 0, 1).Unix())
		startTime := time.Now().UTC().AddDate(0, 1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		if ok := period.IsCurrentPeriod(quotaBucket); ok {
			Fail("Exprected true, returned: false")
		}

		//endTime is before start time
		startTime = time.Now().UTC().AddDate(0, -1, 0).Unix()
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC(), time.Now().UTC().AddDate(0, -1, 0))

		if ok := period.IsCurrentPeriod(quotaBucket); ok {
			Fail("Exprected true, returned: false")
		}

		//start time in period after now
		quotaBucket.SetPeriod(time.Now().AddDate(0, 1, 0), time.Now().AddDate(1, 1, 0))

		if ok := period.IsCurrentPeriod(quotaBucket); ok {
			Fail("Exprected true, returned: false")
		}

		//end time in period is before now
		quotaBucket.SetPeriod(time.Now().UTC().AddDate(-1, -1, 0), time.Now().UTC().AddDate(0, -1, 0))

		if ok := period.IsCurrentPeriod(quotaBucket); ok {
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
		startTime := time.Now().UTC().AddDate(0, -1, 0).Unix()
		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(time.Now().UTC().AddDate(0, 0, -1), time.Now().UTC().AddDate(0, 1, 0))
		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())
		qPeriod, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())

		// check if the rolling window was set properly
		Expect(qPeriod.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !qPeriod.GetPeriodEndTime().After(qPeriod.GetPeriodStartTime()) {
			Fail("Rolling Window was not set as expected")
		}
		intervalDuration := qPeriod.GetPeriodEndTime().Sub(qPeriod.GetPeriodStartTime())
		expectedDuration, err := GetIntervalDurtation(quotaBucket)
		Expect(intervalDuration).Should(Equal(expectedDuration))

		//for non rolling Type window do not setCurrentPeriod as endTime is > time.now.
		quotaType = "calendar"
		pstartTime := time.Now().UTC().AddDate(0, -1, 0)
		pendTime := time.Now().UTC().AddDate(0, 1, 0)
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(pstartTime, pendTime)
		qPeriod, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(qPeriod.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !qPeriod.GetPeriodEndTime().After(qPeriod.GetPeriodStartTime()) {
			Fail("Rolling Window was not set as expected")
		}

		//for non rolling Type window setCurrentPeriod as endTime is < time.now.
		quotaType = "calendar"
		pstartTime = time.Now().UTC().AddDate(0, -1, 0)
		pendTime = time.Now().UTC().AddDate(0, -1, 0)
		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit, quotaType, preciseAtSecondsLevel, startTime, maxCount, bucketType)
		Expect(err).NotTo(HaveOccurred())

		quotaBucket.SetPeriod(pstartTime, pendTime)
		qPeriod, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(qPeriod.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !qPeriod.GetPeriodEndTime().After(qPeriod.GetPeriodStartTime()) {
			Fail("period for Non Rolling Window Type was not set as expected")
		}
		intervalDuration = qPeriod.GetPeriodEndTime().Sub(qPeriod.GetPeriodStartTime())
		expectedDuration, err = GetIntervalDurtation(quotaBucket)
		Expect(intervalDuration).Should(Equal(expectedDuration))
	})
})