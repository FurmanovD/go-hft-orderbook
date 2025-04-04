package hftorderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/FurmanovD/go-kit/db/redislock"
)

const (
	DefaultInitialCacheSize = 10000

	DefaultGlobalLockTimeout = 5 * time.Second
	DefaultRecheckLoopPeriod = 100 * time.Millisecond

	LockKeyOrderBookGlobal = "ob"
	LockKeyBidLimit        = "bidLimit"
	LockKeyAskLimit        = "askLimit"

	orderKeyPrefixBidLimit = "order:bidLimit:"
	orderKeyPrefixAskLimit = "order:askLimit:"
)

var (
	OrderKeyFn = func(id int, bidOrAsk bool) string {
		prefix := orderKeyPrefixAskLimit
		if bidOrAsk {
			prefix = orderKeyPrefixBidLimit
		}
		return fmt.Sprintf("%s%d", prefix, id)
	}
)

type RedisOrder struct {
	ID       int
	Volume   float64
	BidOrAsk bool
	Price    float64
}

// OrderbookDistributed implements distributed order book with Redis caching
type OrderbookDistributed struct {
	ob Orderbook

	redisClient redisClient
	// distributed globalLockerOB
	globalLockerOB redislock.RedisLock
	lockTTL        time.Duration

	mu *sync.RWMutex // Protects concurrent access to the whole orderbook

	muBidLimit        *sync.RWMutex  // Protects concurrent access to Bid side
	orderByIDBidLimit map[int]*Order // Tracks in-memory Bids

	muAskLimit        *sync.RWMutex  // Protects concurrent access to Ask side
	orderByIDAskLimit map[int]*Order // Tracks in-memory Asks

	syncStart    sync.Once
	syncStopChan chan struct{}
	wg           sync.WaitGroup
}

type clock interface {
	Now() time.Time
}

func NewOrderbookDistributed(
	redisClient redisClient,
	clock clock,
) *OrderbookDistributed {
	obd := &OrderbookDistributed{
		ob:             NewOrderbook(),
		redisClient:    redisClient,
		globalLockerOB: redislock.NewRedisLocker(redisClient, LockKeyOrderBookGlobal, clock),
		lockTTL:        10 * time.Second,

		mu: &sync.RWMutex{},

		muBidLimit:        &sync.RWMutex{},
		orderByIDBidLimit: make(map[int]*Order, DefaultInitialCacheSize),

		muAskLimit:        &sync.RWMutex{},
		orderByIDAskLimit: make(map[int]*Order, DefaultInitialCacheSize),

		syncStopChan: make(chan struct{}),
	}

	return obd
}

// Stop halts background synchronization
func (obd *OrderbookDistributed) Stop() {
	close(obd.syncStopChan)
	obd.wg.Wait()
}

// StartBackgroundSync runs periodic Redis synchronization
func (obd *OrderbookDistributed) StartBackgroundSync(ctx context.Context, interval time.Duration) {
	obd.syncStart.Do(func() {
		obd.wg.Add(1)

		go func() {
			defer obd.wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					obd.SyncOrdersFromRedis(ctx)
				case <-obd.syncStopChan:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	})
}

// SyncOrdersFromRedis synchronizes orders from Redis to memory.
// Also might be used as a an inefficient, but more robust rollback mechanism.
func (obd *OrderbookDistributed) SyncOrdersFromRedis(ctx context.Context) error {
	obd.Lock(ctx)
	defer obd.Unlock(ctx)

	wg := &sync.WaitGroup{}

	wg.Add(2)
	var errs []error
	go func() {
		defer wg.Done()
		if err := obd.syncOrdersFromRedis(ctx, orderKeyPrefixBidLimit, obd.orderByIDBidLimit); err != nil {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := obd.syncOrdersFromRedis(ctx, orderKeyPrefixAskLimit, obd.orderByIDAskLimit); err != nil {
			errs = append(errs, err)
		}
	}()

	wg.Wait()

	return errors.Join(errs...)
}

// TODO(DF) the issue with this method is in the current LimitOrder implementation.
// It contains the orders in the queue in order of the time they added
// to the orderbook. This is not the case here. To save the correct order,
// we need to also save the each order's timestamp in micro or nanoseconds.
func (obd *OrderbookDistributed) syncOrdersFromRedis(ctx context.Context, keyPrefix string, orderMap map[int]*Order) error {
	// Get all order keys
	keys, err := obd.redisClient.Keys(ctx, fmt.Sprintf("%s:*", keyPrefix)).Result()
	if err != nil {
		return fmt.Errorf("failed to get order keys with prefix %s: %w", keyPrefix, err)
	}

	toDeleteFromCache := make(map[int]*Order, len(orderMap))
	for orderID, order := range orderMap {
		toDeleteFromCache[orderID] = order
	}

	for _, key := range keys {
		var orderID int
		if _, err := fmt.Sscanf(key, keyPrefix+"%d", &orderID); err != nil {
			return fmt.Errorf("failed to parse int order ID from %s: %w", key, err)
		}

		if _, exists := orderMap[orderID]; exists {
			delete(toDeleteFromCache, orderID)
			continue
		}

		val, err := obd.redisClient.Get(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("failed to get order %d from Redis: %w", orderID, err)
		}

		var redisOrder RedisOrder
		if err := json.Unmarshal([]byte(val), &redisOrder); err != nil {
			return fmt.Errorf("failed to unmarshal order %d from Redis[%s]: %w", orderID, val, err)
		}

		o := &Order{
			Id:       redisOrder.ID,
			Volume:   redisOrder.Volume,
			BidOrAsk: redisOrder.BidOrAsk,
		}

		// Add to local order book
		obd.ob.Add(redisOrder.Price, o)

		// Add to the cacheByID
		orderMap[redisOrder.ID] = o
	}

	// remove extra orders from the local cache
	for _, order := range toDeleteFromCache {
		obd.ob.Cancel(order)
		delete(orderMap, order.Id)
	}

	return nil
}

func (obd *OrderbookDistributed) Add(ctx context.Context, price float64, o *Order) error {
	var lock *sync.RWMutex
	var orderMap map[int]*Order

	if o.BidOrAsk {
		lock = obd.muBidLimit
		orderMap = obd.orderByIDBidLimit
	} else {
		lock = obd.muAskLimit
		orderMap = obd.orderByIDAskLimit
	}

	lock.Lock()
	defer lock.Unlock()

	return obd.add(ctx, orderMap, price, o)
}

func (obd *OrderbookDistributed) add(ctx context.Context, orderMap map[int]*Order, price float64, o *Order) error {
	if _, exists := orderMap[o.Id]; exists {
		return nil
	}

	// Persist to Redis
	jsonData, err := json.Marshal(RedisOrder{
		ID:       o.Id,
		Volume:   o.Volume,
		BidOrAsk: o.BidOrAsk,
		Price:    price,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal redis order: %w", err)
	}

	res := obd.redisClient.Set(
		ctx,
		OrderKeyFn(o.Id, o.BidOrAsk),
		jsonData,
		0,
	)

	if res != nil && res.Err() != nil {
		return fmt.Errorf("failed to persist redis order: %w", res.Err())
	}

	// Add to unsafe order book
	obd.ob.Add(price, o)

	orderMap[o.Id] = o

	return nil
}

func (obd *OrderbookDistributed) Cancel(ctx context.Context, price float64, o *Order) {
	var lock *sync.RWMutex
	var orderMap map[int]*Order

	if o.BidOrAsk {
		lock = obd.muBidLimit
		orderMap = obd.orderByIDBidLimit
	} else {
		lock = obd.muAskLimit
		orderMap = obd.orderByIDAskLimit
	}

	lock.Lock()
	defer lock.Unlock()

	obd.cancel(ctx, orderMap, price, o)
}

func (obd *OrderbookDistributed) cancel(ctx context.Context, orderMap map[int]*Order, price float64, o *Order) error {
	cmdRes := obd.redisClient.Del(ctx, OrderKeyFn(o.Id, o.BidOrAsk))
	if cmdRes != nil && cmdRes.Err() != nil {
		// TODO(DF) handle NotFound error properly
		return fmt.Errorf("failed to delete redis order: %w", cmdRes.Err())
	}

	if _, exists := orderMap[o.Id]; !exists {
		return nil
	}

	// Remove from unsafe order book
	obd.ob.Cancel(o)

	delete(orderMap, o.Id)

	return nil
}

// Remaining methods delegate to embedded Orderbook with proper locking

func (obd *OrderbookDistributed) ClearBidLimit(price float64) {
	obd.muBidLimit.Lock()
	defer obd.muBidLimit.Unlock()

	// TODO(DF) implement properly, going by all the orders and deleting them:
	// * from the mapByIDs
	// * from the redis
	// NOTE: is not required right now as it's not used yet
	obd.ob.ClearBidLimit(price)
}

func (obd *OrderbookDistributed) ClearAskLimit(price float64) {
	obd.muAskLimit.Lock()
	defer obd.muAskLimit.Unlock()

	// TODO(DF) implement properly, going by all the orders and deleting them:
	// * from the mapByIDs
	// * from the redis
	// NOTE: is not required right now as it's not used yet
	obd.ob.ClearAskLimit(price)
}

func (obd *OrderbookDistributed) DeleteBidLimit(price float64) {
	obd.muBidLimit.Lock()
	defer obd.muBidLimit.Unlock()

	// TODO(DF) implement properly, going by all the orders and deleting them:
	// * from the mapByIDs
	// * from the redis
	// NOTE: is not required right now as it's not used yet
	obd.ob.DeleteBidLimit(price)
}

func (obd *OrderbookDistributed) DeleteAskLimit(price float64) {
	obd.muAskLimit.Lock()
	defer obd.muAskLimit.Unlock()

	// TODO(DF) implement properly, going by all the orders and deleting them:
	// * from the mapByIDs
	// * from the redis
	// NOTE: is not required right now as it's not used yet
	obd.ob.DeleteAskLimit(price)
}

func (obd *OrderbookDistributed) GetVolumeAtBidLimit(price float64) float64 {
	obd.muBidLimit.RLock()
	defer obd.muBidLimit.RUnlock()

	return obd.ob.GetVolumeAtBidLimit(price)
}

func (obd *OrderbookDistributed) GetVolumeAtAskLimit(price float64) float64 {
	obd.muAskLimit.Lock()
	defer obd.muAskLimit.Unlock()

	return obd.ob.GetVolumeAtAskLimit(price)
}

func (obd *OrderbookDistributed) GetBestBid(ctx context.Context) float64 {
	obd.RLockBid(ctx)
	defer obd.RUnlockBid(ctx)
	return obd.ob.GetBestBid()
}

func (obd *OrderbookDistributed) GetBestOffer(ctx context.Context) float64 {
	obd.RLockAsk(ctx)
	defer obd.RUnlockAsk(ctx)
	return obd.ob.GetBestOffer()
}

func (obd *OrderbookDistributed) BLength(ctx context.Context) int {
	obd.RLockBid(ctx)
	defer obd.RUnlockBid(ctx)
	return obd.ob.BLength()
}

func (obd *OrderbookDistributed) ALength(ctx context.Context) int {
	obd.RLockAsk(ctx)
	defer obd.RUnlockAsk(ctx)
	return obd.ob.ALength()
}

func (obd *OrderbookDistributed) Lock(ctx context.Context) error {
	obd.mu.Lock()
	obd.muBidLimit.Lock()
	obd.muAskLimit.Lock()

	if err := obd.globalLockerOB.ObtainLock(ctx, obd.lockTTL, DefaultGlobalLockTimeout, DefaultRecheckLoopPeriod); err != nil {
		obd.muAskLimit.Unlock()
		obd.muBidLimit.Unlock()
		obd.mu.Unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (obd *OrderbookDistributed) Unlock(ctx context.Context) error {
	if err := obd.globalLockerOB.Unlock(ctx); err != nil {
		return err
	}

	obd.muAskLimit.Unlock()
	obd.muBidLimit.Unlock()
	obd.mu.Unlock()

	return nil
}

func (obd *OrderbookDistributed) LockBid(ctx context.Context) {
	obd.muBidLimit.Lock()
}

func (obd *OrderbookDistributed) UnlockBid(ctx context.Context) {
	obd.muBidLimit.Unlock()
}

func (obd *OrderbookDistributed) RLockBid(ctx context.Context) {
	obd.muBidLimit.RLock()
}

func (obd *OrderbookDistributed) RUnlockBid(ctx context.Context) {
	obd.muBidLimit.RUnlock()
}

func (obd *OrderbookDistributed) LockAsk(ctx context.Context) {
	obd.muAskLimit.Lock()
}

func (obd *OrderbookDistributed) UnlockAsk(ctx context.Context) {
	obd.muAskLimit.Unlock()
}

func (obd *OrderbookDistributed) RLockAsk(ctx context.Context) {
	obd.muAskLimit.RLock()
}

func (obd *OrderbookDistributed) RUnlockAsk(ctx context.Context) {
	obd.muAskLimit.RUnlock()
}

func (obd *OrderbookDistributed) GetBidLimit(price float64) *LimitOrder {
	obd.muBidLimit.RLock()
	defer obd.muBidLimit.RUnlock()

	return obd.ob.GetBidLimit(price)
}

func (obd *OrderbookDistributed) GetAskLimit(price float64) *LimitOrder {
	obd.muAskLimit.RLock()
	defer obd.muAskLimit.RUnlock()

	return obd.ob.GetAskLimit(price)
}
