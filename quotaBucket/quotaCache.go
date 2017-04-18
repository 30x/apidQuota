package quotaBucket

import (
	"github.com/30x/apidQuota/constants"
	"time"
	"sync"
	"fmt"
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
	qBucketCache, ok := quotaCache[cacheKey]
	quotaCachelock.Unlock()
	if !ok {
		fmt.Println("not in cache. add to cache.")
		return nil,false
	}

	isExpired := time.Unix(qBucketCache.expiryTime, 0).Before(time.Now().UTC())
	if isExpired {
		fmt.Println("quotaBucket expired: remove from cache and return false.")
		removeFromCache(cacheKey, qBucketCache)
		return nil, false
	}

	return qBucketCache.qBucket, true

}

func removeFromCache(cacheKey string, qBucketCache quotaBucketCache) {
	fmt.Println("inside remove from cache")
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

	fmt.Println("qbucket in cache: ", qBucketToAdd.getTicker())
	quotaCachelock.Lock()
	quotaCache[cacheKey] = qCacheData
	quotaCachelock.Unlock()
	fmt.Println("duration: " ,time.Unix(qCacheData.expiryTime,0).String())
	fmt.Println("now: ", time.Now().UTC())
}
