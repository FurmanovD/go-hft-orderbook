//go:generate mockery --with-expecter --name=redisClient --testonly --inpackage --filename=redisclient_mock.go
//go:generate mockery --with-expecter --name=redisPipeliner --testonly --inpackage --filename=redispipeliner_mock.go
//go:generate mockery --with-expecter --name=orderbookDistributed --testonly --inpackage --filename=orderbookdistributed_mock.go
//go:generate mockery --with-expecter --name=clock --testonly --inpackage --filename=clock_mock.go
package hftorderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/FurmanovD/go-kit/db/redislock"
	"github.com/go-redis/redis/v8"
)

const (
	LockTTLOrderExecution = 10 * time.Second
	OrderExecutionTimeout = 10 * time.Second
)

var (
	LockKeyOrderFn = func(id int) string {
		return fmt.Sprintf("execution-order:%d", id)
	}
)

type redisPipeliner interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Exec(ctx context.Context) ([]redis.Cmder, error)
}

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	// the SetNX is present to satisfy the redisLock instances creation
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	// next methods are executor-engine specific
	Pipeline() redisPipeliner
}

type orderbookDistributed interface {
	Lock(context.Context) error
	Unlock(context.Context) error

	Add(context.Context, float64, *Order) error
	Cancel(context.Context, float64, *Order) error

	GetAskLimit(float64) *LimitOrder
	GetBidLimit(float64) *LimitOrder

	GetBestOffer(context.Context) float64
	GetBestBid(context.Context) float64
}

type ExecutionEngine struct {
	obd       orderbookDistributed
	redis     redisClient
	inputChan chan *Order
	stopChan  chan struct{}
	wg        sync.WaitGroup
	tradeChan chan TradeEvent
	// TODO: possibly add the Error channel
	clock clock
}

type TradeEvent struct {
	TakerOrderID int
	MakerOrderID int
	Price        float64
	Volume       float64
	Timestamp    time.Time
}

type transactionStep struct {
	undo       func(context.Context) error
	redisState []byte
}

type orderSnapshot struct {
	order     *Order
	redisData []byte
}

func NewExecutionEngine(
	ob orderbookDistributed,
	redis redisClient,
	workers int,
	c clock,
) *ExecutionEngine {
	ee := &ExecutionEngine{
		obd:       ob,
		redis:     redis,
		inputChan: make(chan *Order, 100000), // the size should be configurable
		stopChan:  make(chan struct{}),
		tradeChan: make(chan TradeEvent, 100000), // the size should be configurable
		clock:     c,
	}

	ee.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go ee.processOrders()
	}

	return ee
}

// TODO(DF) the Order.LimitOrder is expected to be NOT nil to store the limit price.
// This limitation is required to avoid huge refactoring of the original order book package.
func (ee *ExecutionEngine) SubmitOrder(o *Order) error {
	if o.Limit == nil {
		return fmt.Errorf("order %d has no limit price", o.Id)
	}

	ee.inputChan <- o

	return nil
}

func (ee *ExecutionEngine) processOrders() {
	defer ee.wg.Done()

	for {
		select {
		case order := <-ee.inputChan:
			// TODO(DF) pass to the error channel
			_ = ee.processOrder(order)
		case <-ee.stopChan:
			return
		}
	}
}

func (ee *ExecutionEngine) processOrder(o *Order) error {
	// TODO(DF) pass ctx with the order?
	ctx, cancel := context.WithTimeout(context.Background(), OrderExecutionTimeout)
	defer cancel()

	// Lock the whole order book globally until the order is executed/added
	if err := ee.obd.Lock(ctx); err != nil {
		return fmt.Errorf("failed to lock order book: %+w", err)
	}
	defer func() {
		if err := ee.obd.Unlock(ctx); err != nil {
			log.Printf("failed to unlock order book: %v", err)
		}
	}()

	tx := ee.createTransaction(ctx, o)

	if err := tx.orderLock.Lock(ctx, LockTTLOrderExecution); err != nil {
		return fmt.Errorf("failed to lock order %d to execute: %+w", tx.order.Id, err)
	}

	defer func() {
		if err := tx.orderLock.Unlock(ctx); err != nil {
			log.Printf("failed to unlock order %d execution lock: %v", tx.order.Id, err)
		}
	}()

	if err := ee.executeTransaction(ctx, tx); err != nil {
		ee.rollbackTransaction(ctx, tx)
		return fmt.Errorf("failed to execute order %d: %+w", tx.order.Id, err)
	}

	if err := ee.commitTransaction(ctx, tx); err != nil {
		ee.rollbackTransaction(ctx, tx)
		return fmt.Errorf("failed to commit order %d: %+w", tx.order.Id, err)
	}

	return nil
}

type transaction struct {
	orderLock     redislock.RedisLock
	order         *Order
	snapshots     map[int]*orderSnapshot
	steps         []transactionStep
	matchedOrders []*Order
	ctx           context.Context
}

func (ee *ExecutionEngine) createTransaction(ctx context.Context, o *Order) *transaction {
	return &transaction{
		orderLock: redislock.NewRedisLocker(ee.redis, LockKeyOrderFn(o.Id), ee.clock),
		order:     o,
		snapshots: make(map[int]*orderSnapshot),
		ctx:       ctx,
	}
}

func (ee *ExecutionEngine) executeTransaction(ctx context.Context, tx *transaction) error {
	if err := ee.prepareOrderAddition(tx); err != nil {
		return err
	}

	return ee.matchOrders(tx)
}

func (ee *ExecutionEngine) prepareOrderAddition(tx *transaction) error {
	// Capture initial state
	snapshot, err := ee.createOrderSnapshot(tx.ctx, tx.order)
	if err != nil {
		return err
	}
	tx.snapshots[tx.order.Id] = snapshot

	// Add order to book
	if err := ee.obd.Add(tx.ctx, tx.order.Limit.Price, tx.order); err != nil {
		return err
	}

	// Record undo step
	tx.steps = append(tx.steps, transactionStep{
		undo: func(ctx context.Context) error {
			// TODO(DF) send error to the error channel
			_ = ee.obd.Cancel(ctx, tx.order.Limit.Price, tx.order)
			return nil
		},
		redisState: snapshot.redisData,
	})

	return nil
}

func (ee *ExecutionEngine) createOrderSnapshot(ctx context.Context, o *Order) (*orderSnapshot, error) {
	key := OrderKeyFn(o.Id, o.BidOrAsk)
	redisData, err := ee.redis.Get(ctx, key).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	return &orderSnapshot{
		order:     &Order{Id: o.Id, Volume: o.Volume, BidOrAsk: o.BidOrAsk},
		redisData: redisData,
	}, nil
}

func (ee *ExecutionEngine) matchOrders(tx *transaction) error {
	for {
		bestPrice := ee.getBestMatchPrice(tx.ctx, tx.order.BidOrAsk, tx.order.Limit.Price)
		if bestPrice == 0 {
			break // stop when order book is empty
		}

		if err := ee.matchAtPrice(tx, bestPrice); err != nil {
			return err
		}

		if tx.order.Volume <= 0 {
			break // stop when order is fully executed
		}
	}
	return nil
}

func (ee *ExecutionEngine) matchAtPrice(tx *transaction, price float64) error {
	var limit *LimitOrder
	if tx.order.BidOrAsk {
		limit = ee.obd.GetAskLimit(price)
	} else {
		limit = ee.obd.GetBidLimit(price)
	}

	if limit == nil || limit.Size() == 0 {
		return nil
	}

	makerOrder := limit.orders.Dequeue()
	if makerOrder == nil {
		return nil
	}

	tx.matchedOrders = append(tx.matchedOrders, makerOrder)

	// Capture maker order state
	snapshot, err := ee.createOrderSnapshot(tx.ctx, makerOrder)
	if err != nil {
		return err
	}
	tx.snapshots[makerOrder.Id] = snapshot

	// Execute trade
	tradeVolume := min(tx.order.Volume, makerOrder.Volume)
	tx.order.Volume -= tradeVolume
	makerOrder.Volume -= tradeVolume

	// Record trade event
	// TODO(DF) reimplement to collect all the trade events in a slice and send them all
	// in a correct order when the transaction is commited.
	ee.tradeChan <- TradeEvent{
		TakerOrderID: tx.order.Id,
		MakerOrderID: makerOrder.Id,
		Price:        price,
		Volume:       tradeVolume,
		Timestamp:    ee.clock.Now().UTC(),
	}

	// Update or remove maker order
	if makerOrder.Volume > 0 {
		limit.orders.Enqueue(makerOrder)
		tx.steps = append(tx.steps, transactionStep{
			undo: func(ctx context.Context) error {
				makerOrder.Volume += tradeVolume
				return ee.obd.Add(ctx, price, makerOrder)
			},
		})
	} else {
		tx.steps = append(tx.steps, transactionStep{
			undo: func(ctx context.Context) error {
				makerOrder.Volume += tradeVolume
				return ee.obd.Add(ctx, price, makerOrder)
			},
			redisState: snapshot.redisData,
		})
	}

	return nil
}

func (ee *ExecutionEngine) rollbackTransaction(ctx context.Context, tx *transaction) {
	// Undo steps in reverse order
	for i := len(tx.steps) - 1; i >= 0; i-- {
		step := tx.steps[i]
		if err := step.undo(ctx); err != nil {
			// Log error but continue rolling back OR return error into the processing errors channel
			log.Printf("Failed to rollback transaction step #%d: %v", i, err)
		}

		if step.redisState != nil {
			key := OrderKeyFn(tx.order.Id, tx.order.BidOrAsk)
			if len(step.redisState) == 0 {
				ee.redis.Del(ctx, key)
			} else {
				ee.redis.Set(ctx, key, step.redisState, 0)
			}
		}
	}

	// Restore matched orders
	for _, order := range tx.matchedOrders {
		if snapshot, exists := tx.snapshots[order.Id]; exists {
			ee.restoreOrder(ctx, snapshot)
		}
	}

	// Restore original order
	if snapshot, exists := tx.snapshots[tx.order.Id]; exists {
		ee.restoreOrder(ctx, snapshot)
	}
}

func (ee *ExecutionEngine) commitTransaction(ctx context.Context, tx *transaction) error {
	// Persist all changes to Redis
	pipe := ee.redis.Pipeline()

	// Persist taker order if there is any unmatched volume left
	if tx.order.Volume > 0 {
		data, err := json.Marshal(RedisOrder{
			ID:       tx.order.Id,
			Volume:   tx.order.Volume,
			BidOrAsk: tx.order.BidOrAsk,
			Price:    tx.order.Limit.Price,
		})
		if err == nil {
			pipe.Set(ctx, OrderKeyFn(tx.order.Id, tx.order.BidOrAsk), data, 0)
		}
	} else {
		pipe.Del(ctx, OrderKeyFn(tx.order.Id, tx.order.BidOrAsk))
	}

	// Persist maker orders
	for _, order := range tx.matchedOrders {
		if order.Volume > 0 {
			data, err := json.Marshal(RedisOrder{
				ID:       order.Id,
				Volume:   order.Volume,
				BidOrAsk: order.BidOrAsk,
				Price:    order.Limit.Price,
			})
			if err == nil {
				pipe.Set(ctx, OrderKeyFn(order.Id, order.BidOrAsk), data, 0)
			}
		} else {
			pipe.Del(ctx, OrderKeyFn(order.Id, order.BidOrAsk))
		}
	}

	// TODO(DF) analyze the result as well
	_, err := pipe.Exec(ctx)
	return err
}

func (ee *ExecutionEngine) restoreOrder(ctx context.Context, snapshot *orderSnapshot) {
	if snapshot == nil {
		return
	}

	key := OrderKeyFn(snapshot.order.Id, snapshot.order.BidOrAsk)
	if len(snapshot.redisData) == 0 {
		ee.redis.Del(ctx, key)
	} else {
		ee.redis.Set(ctx, key, snapshot.redisData, 0)
	}
}

func (ee *ExecutionEngine) Stop() {
	close(ee.stopChan)
	ee.wg.Wait()
	close(ee.tradeChan)
}

func (ee *ExecutionEngine) getBestMatchPrice(ctx context.Context, bidOrAsk bool, limitPrice float64) float64 {
	if bidOrAsk {
		// For BUY orders (bids), check best ASK price
		bestOffer := ee.obd.GetBestOffer(ctx)
		if bestOffer <= limitPrice {
			return bestOffer
		}
	} else {
		// For SELL orders (asks), check best BID price
		bestBid := ee.obd.GetBestBid(ctx)
		if bestBid >= limitPrice {
			return bestBid
		}
	}
	return 0 // No valid match price
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
