package quotaBucket

import (
	"github.com/30x/apidQuota/constants"
	"time"
	"sync"
)

var quotaCachelock = sync.RWMutex{}

type quotaBucketCache struct {
	qBucket    *QuotaBucket
	expiryTime int64
}

var quotaCache map[string]quotaBucketCache

func init() {
	quotaCache = make(map[string]quotaBucketCache)
}

func getFromCache(cacheKey string) (*QuotaBucket,bool) {
	quotaCachelock.Lock()
	defer quotaCachelock.Unlock()
	qBucketCache, ok := quotaCache[cacheKey]
	if !ok {
		return nil,false
	}

	isExpired := time.Unix(qBucketCache.expiryTime, 0).Before(time.Now().UTC())
	if isExpired {
		removeFromCache(cacheKey, qBucketCache)
		return nil, false
	}

	// update expiry time every time you access.
	ttl := time.Now().UTC().Add(constants.CacheTTL).Unix()
	qBucketCache.expiryTime = ttl
	quotaCache[cacheKey] = qBucketCache

	return qBucketCache.qBucket, true

}

func removeFromCache(cacheKey string, qBucketCache quotaBucketCache) {
	//for async Stop the scheduler.
	if qBucketCache.qBucket.Distributed && !qBucketCache.qBucket.IsSynchronous(){
		qBucketCache.qBucket.getTicker().Stop()
	}

	quotaCachelock.Lock()
	delete(quotaCache,cacheKey)
	quotaCachelock.Unlock()
}

func addToCache(qBucketToAdd *QuotaBucket) {
	cacheKey := qBucketToAdd.GetEdgeOrgID() + constants.CacheKeyDelimiter + qBucketToAdd.GetID()
	ttl := time.Now().UTC().Add(constants.CacheTTL).Unix()

	qCacheData := quotaBucketCache{
		qBucket:    qBucketToAdd,
		expiryTime: ttl,
	}

	quotaCachelock.Lock()
	quotaCache[cacheKey] = qCacheData
	quotaCachelock.Unlock()
}
