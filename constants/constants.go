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

package constants

import "time"

const (
	//config variables.
	ApigeeSyncBearerToken = "apigeesync_bearer_token"
	ConfigCounterServiceBasePath = "apidquota_counterService_base_path"

	//add to acceptedTimeUnitList in init() if case any other new timeUnit is added
	TimeUnitSECOND = "second"
	TimeUnitMINUTE = "minute"
	TimeUnitHOUR   = "hour"
	TimeUnitDAY    = "day"
	TimeUnitWEEK   = "week"
	TimeUnitMONTH  = "month"

	//errors
	InvalidQuotaTimeUnitType = "invalidQuotaTimeUnitType"
	InvalidQuotaType         = "invalidQuotaType"
	InvalidQuotaPeriod       = "invalidQuotaPeriod"
	AsyncQuotaBucketEmpty = "AsyncDetails_for_quotaBucket_are_empty"

	QuotaTypeCalendar      = "calendar"      // after start time
	QuotaTypeRollingWindow = "rollingwindow" // in the past "window" time

	CacheKeyDelimiter    = "|"
	CacheTTL             = time.Minute * 1
	DefaultQuotaSyncTime = 300 //in seconds
	DefaultCount         = 0

	UnableToParseBody           = "unable_to_parse_body"
	UnMarshalJSONError          = "unmarshal_json_error"
	ErrorConvertReqBodyToEntity = "error_convert_reqBody_to_entity"
	ConfigQuotaBasePath         = "quota_base_path"
	ErrorCheckingQuotaLimit     = "error_checking_quota_limit"
	QuotaBasePathDefault        = "/quota"

	URLCounterServiceNotSet      = "url_counter_service_not_set"
	URLCounterServiceInvalid     = "url_counter_service_invalid"
	MarshalJSONError             = "marshal_JSON_error"
)
