package apidQuota

import (
	"github.com/30x/apid-core"
)

var (
	Log    apid.LogService
	Config apid.ConfigService
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	Log = services.Log().ForModule("apidQuota")
	Log.Debug("start init")

	setConfig(services)
	InitAPI(services)

	return pluginData, nil
}

func setConfig(services apid.Services) {
	// set configuration
	Config = services.Config()
	// set plugin config defaults
	Config.SetDefault(ConfigQuotaBasePath, quotaBasePathDefault)

}
