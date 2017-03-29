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
