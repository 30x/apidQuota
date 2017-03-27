package apidQuota

import (
	"github.com/30x/apid-core"
	"github.com/30x/apidQuota/constants"
	"github.com/30x/apidQuota/globalVariables"
)

func init() {
	apid.RegisterPlugin(initPlugin)
	initCounterService()
}

// set config for counter service.
func initCounterService() {
	//counterBasePath := globalVariables.Config.Get(constants.ConfigCounterServiceBasePath)
	//fmt.Println("counterBasePath: ", counterBasePath , "//")
	/*if counterBasePath != nil {
		if reflect.TypeOf(counterBasePath).Kind() != reflect.String{
			globalVariables.Log.Fatal("value of: " + constants.ConfigCounterServiceBasePath + " in the config should be string")
		}
		globalVariables.CounterServiceURL = counterBasePath.(string)
	}*/
	globalVariables.CounterServiceURL = "http://54.86.114.219:8989/increment"
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	globalVariables.Log = services.Log().ForModule("apidQuota")
	globalVariables.Log.Debug("start init")

	setConfig(services)
	InitAPI(services)

	return pluginData, nil
}

func setConfig(services apid.Services) {
	// set configuration
	globalVariables.Config = services.Config()
	// set plugin config defaults
	globalVariables.Config.SetDefault(constants.ConfigQuotaBasePath, constants.QuotaBasePathDefault)

}
