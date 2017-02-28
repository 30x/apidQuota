package apidQuota

import (
	"github.com/30x/apid-core"
)

var(
	log apid.LogService

)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	log = services.Log().ForModule("apidQuota")
	log.Debug("start init")

	return pluginData, nil
}
