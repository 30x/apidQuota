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

package apidQuota

import (
	"github.com/30x/apid-core"
	"github.com/30x/apidQuota/constants"
	"github.com/30x/apidQuota/globalVariables"
	"reflect"
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	globalVariables.Log = services.Log().ForModule("apidQuota")
	globalVariables.Log.Debug("start init for apidQuota")

	setConfig(services)
	InitAPI(services)

	return pluginData, nil
}

func setConfig(services apid.Services) {
	// set configuration
	globalVariables.Config = services.Config()
	// set plugin config defaults
	globalVariables.Config.SetDefault(constants.ConfigQuotaBasePath, constants.QuotaBasePathDefault)

	counterServiceBasePath := globalVariables.Config.Get(constants.ConfigCounterServiceBasePath)
	if counterServiceBasePath != nil {
		if reflect.TypeOf(counterServiceBasePath).Kind() != reflect.String {
			globalVariables.Log.Fatal("value of: " + constants.ConfigCounterServiceBasePath + " in the config should be string")
		}
		globalVariables.CounterServiceURL = counterServiceBasePath.(string)
	}

}
