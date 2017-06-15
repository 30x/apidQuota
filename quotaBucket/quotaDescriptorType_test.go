// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quotaBucket_test

import (
	"github.com/30x/apidQuota/constants"
	. "github.com/30x/apidQuota/quotaBucket"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"reflect"
	"strings"
	"time"
)

var _ = Describe("Check Descriptor Type ", func() {
	It("test Calendar Type descriptor", func() {
		descriptorType, err := GetQuotaTypeHandler("calendar")
		Expect(err).NotTo(HaveOccurred())
		if reflect.TypeOf(descriptorType) != reflect.TypeOf(&CalendarQuotaDescriptorType{}) {
			Fail("Excepted CalendarQuotaDescriptorType, but got: " + reflect.TypeOf(descriptorType).String())
		}
	})

	It("test RollingWindow Type descriptor", func() {
		descriptorType, err := GetQuotaTypeHandler("rollingwindow")
		Expect(err).NotTo(HaveOccurred())
		if reflect.TypeOf(descriptorType) != reflect.TypeOf(&RollingWindowQuotaDescriptorType{}) {
			Fail("Excepted RollingWindowQuotaDescriptorType, but got: " + reflect.TypeOf(descriptorType).String())
		}
	})

	It("test invalid Type descriptor", func() {
		_, err := GetQuotaTypeHandler("invalidDescriptorType")
		Expect(err).To(HaveOccurred())
		if !strings.Contains(err.Error(), constants.InvalidQuotaType) {
			Fail("Excepted error to contain: " + constants.InvalidQuotaType + " but got: " + err.Error())
		}
	})
})

var _ = Describe("QuotaDescriptorType", func() {
	It("Valid testcases for CalendarType", func() {

		// test set period for timeUnit=second
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "second"
		quotaType := "calendar"
		interval := 1
		maxCount := int64(10)
		weight := int64(1)
		distributed := true
		synchronous := true
		syncTimeInSec := int64(-1)
		syncMessageCount := int64(-1)
		preciseAtSecondsLevel := true
		startTime := time.Now().UTC().UTC().AddDate(0, -1, 0).Unix()

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration := period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Second))

		// test set period for timeUnit=minute
		timeUnit = "minute"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Minute))

		// test set period for timeUnit=hour
		timeUnit = "hour"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Hour))

		// test set period for timeUnit=day
		timeUnit = "day"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(24 * time.Hour))

		// test set period for timeUnit=week
		timeUnit = "week"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(7 * 24 * time.Hour))

		// test set period for timeUnit=month
		timeUnit = "month"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		addMonthToStartDate := period.GetPeriodStartTime().AddDate(0, interval*1, 0)
		actualDuration := addMonthToStartDate.Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(actualDuration))

	})

	It("inValid testcases for CalendarType", func() {

		// test set period for timeUnit=second
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "second"
		quotaType := "calendar"
		interval := 1
		maxCount := int64(10)
		weight := int64(1)
		preciseAtSecondsLevel := true
		startTime := time.Now().UTC().UTC().AddDate(0, -1, 0).Unix()
		distributed := true
		synchronous := true
		syncTimeInSec := int64(-1)
		syncMessageCount := int64(-1)

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Calendar Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration := period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Second))

		// test set period for timeUnit=month
		timeUnit = "invalidTimeUnit"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		err = quotaBucket.Validate()
		Expect(err).To(HaveOccurred())
		if ok := strings.Contains(err.Error(), constants.InvalidQuotaTimeUnitType); !ok {
			Fail("expected error to contain " + constants.InvalidQuotaTimeUnitType + " but got different error message: " + err.Error())
		}

	})

	It("Valid testcases for RollingWindow Type", func() {

		// test set period for timeUnit=second
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "second"
		quotaType := "rollingWindow"
		interval := 1
		maxCount := int64(10)
		weight := int64(1)
		preciseAtSecondsLevel := true
		startTime := time.Now().UTC().UTC().AddDate(0, -1, 0).Unix()
		distributed := true
		synchronous := true
		syncTimeInSec := int64(-1)
		syncMessageCount := int64(-1)

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration := period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Second))

		// test set period for timeUnit=minute
		timeUnit = "minute"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Minute))

		// test set period for timeUnit=hour
		timeUnit = "hour"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Hour))

		// test set period for timeUnit=day
		timeUnit = "day"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(24 * time.Hour))

		// test set period for timeUnit=week
		timeUnit = "week"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(7 * 24 * time.Hour))

		// test set period for timeUnit=month
		timeUnit = "month"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err = quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration = period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		subMonthToEndtDate := period.GetPeriodEndTime().AddDate(0, -interval*1, 0)
		actualDuration := period.GetPeriodEndTime().Sub(subMonthToEndtDate)
		Expect(intervalDuration).Should(Equal(actualDuration))

	})

	It("inValid testcases for RollingWindow Type", func() {

		// test set period for timeUnit=second
		edgeOrgID := "sampleOrg"
		id := "sampleID"
		timeUnit := "second"
		quotaType := "rollingwindow"
		interval := 1
		maxCount := int64(10)
		weight := int64(1)
		preciseAtSecondsLevel := true
		startTime := time.Now().UTC().UTC().AddDate(0, -1, 0).Unix()
		distributed := true
		synchronous := true
		syncTimeInSec := int64(-1)
		syncMessageCount := int64(-1)

		quotaBucket, err := NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		Expect(err).NotTo(HaveOccurred())

		err = quotaBucket.Validate()
		Expect(err).NotTo(HaveOccurred())

		period, err := quotaBucket.GetPeriod()
		Expect(err).NotTo(HaveOccurred())
		// check if the calendar window was set properly
		Expect(period.GetPeriodInputStartTime()).Should(Equal(quotaBucket.GetStartTime()))
		if !period.GetPeriodEndTime().After(period.GetPeriodStartTime()) {
			Fail("period for Rolling Window Type was not set as expected")
		}
		Expect(period.GetPeriodStartTime().String()).ShouldNot(BeEmpty())
		Expect(period.GetPeriodEndTime().String()).ShouldNot(BeEmpty())
		intervalDuration := period.GetPeriodEndTime().Sub(period.GetPeriodStartTime())
		Expect(intervalDuration).Should(Equal(time.Second))

		// test set period for timeUnit=month
		timeUnit = "invalidTimeUnit"

		quotaBucket, err = NewQuotaBucket(edgeOrgID, id, interval, timeUnit,
			quotaType, preciseAtSecondsLevel, startTime, maxCount,
			weight, distributed, synchronous, syncTimeInSec, syncMessageCount)
		err = quotaBucket.Validate()
		Expect(err).To(HaveOccurred())
		if ok := strings.Contains(err.Error(), constants.InvalidQuotaTimeUnitType); !ok {
			Fail("expected error to contain " + constants.InvalidQuotaTimeUnitType + " but got different error message: " + err.Error())
		}

	})
})
