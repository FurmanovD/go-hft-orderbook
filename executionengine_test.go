package hftorderbook

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestExecutionEngine_BasicOrderExecution(t *testing.T) {
	price := float64(100)

	orderMaker := &Order{
		Id:       1,
		Volume:   100,
		BidOrAsk: false,
		Limit:    &LimitOrder{Price: price},
	}
	orderMakerLimit := NewLimitOrder(price)
	orderMakerLimit.Enqueue(orderMaker)

	orderTaker := &Order{
		Id:       2,
		Volume:   50,
		BidOrAsk: true,
		Limit:    &LimitOrder{Price: price},
	}

	orderTakerAfterPartialExec := &RedisOrder{
		ID:       1,
		Volume:   50,
		BidOrAsk: false,
		Price:    price,
	}
	orderTakerJSON := MustMarshalJSON(orderTakerAfterPartialExec)

	obMock := newMockOrderbookDistributed(t)
	redisMock := newMockRedisClient(t)
	redisPipeliner := newMockRedisPipeliner(t)
	clockMock := newMockClock(t)

	// Setup order book expectations
	obMock.EXPECT().Lock(mock.Anything).Return(nil).Once()
	// order execution locker expectations
	redisMock.EXPECT().SetNX(mock.Anything, "lock-execution-order:2", 1, mock.Anything).Return(redisBoolCmd(true)).Once()

	obMock.EXPECT().GetAskLimit(price).Return(&orderMakerLimit).Once()

	obMock.EXPECT().GetBestOffer(mock.Anything).Return(100.0).Once()
	obMock.EXPECT().Add(mock.Anything, 100.0, mock.Anything).Return(nil).Once()

	// snapshot creation:
	redisMock.EXPECT().Get(mock.Anything, "order:bidLimit:2").Return(
		redis.NewStringResult(`{"ID":2,"Volume":50,"BidOrAsk":true,"Price":100}`, nil),
	).Once()

	redisMock.EXPECT().Get(mock.Anything, "order:askLimit:1").Return(
		redis.NewStringResult(`{"ID":1,"Volume":100,"BidOrAsk":false,"Price":100}`, nil),
	).Once()

	// save execution
	redisMock.EXPECT().Pipeline().Return(redisPipeliner).Once()

	// delete fully executed taker's order
	redisPipeliner.EXPECT().Del(mock.Anything, "order:bidLimit:2").Return(redisIntCmd(1)).Once()
	// update the volume of the maker's order
	redisPipeliner.EXPECT().Set(mock.Anything, "order:askLimit:1", orderTakerJSON, mock.Anything).Return(redisStatusCmd("")).Once()
	redisPipeliner.EXPECT().Exec(mock.Anything).Return([]redis.Cmder{}, nil).Once()

	// complete the order execution
	redisMock.EXPECT().Del(mock.Anything, "lock-execution-order:2").Return(redisIntCmd(1)).Once()

	obMock.EXPECT().Unlock(mock.Anything).Return(nil).Once()

	// Setup clock expectations
	clockMock.EXPECT().Now().Return(time.Now().UTC()).Once()

	engine := NewExecutionEngine(obMock, redisMock, 1, clockMock)
	defer engine.Stop()

	err := engine.SubmitOrder(orderTaker)
	require.NoError(t, err)

	select {
	case trade := <-engine.tradeChan:
		assert.Equal(t, 50.0, trade.Volume)
		assert.Equal(t, 100.0, trade.Price)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected trade event not received")
	}
}

func TestExecutionEngine_NoMatch(t *testing.T) {
	orderMaker := &Order{
		Id:       1,
		Volume:   100,
		BidOrAsk: false,
		Limit:    &LimitOrder{Price: 100},
	}
	orderMakerLimit := NewLimitOrder(100)
	orderMakerLimit.Enqueue(orderMaker)

	orderTaker := &Order{
		Id:       2,
		Volume:   50,
		BidOrAsk: true,
		Limit:    &LimitOrder{Price: 99},
	}

	obMock := newMockOrderbookDistributed(t)
	redisMock := newMockRedisClient(t)
	redisPipeliner := newMockRedisPipeliner(t)
	clockMock := newMockClock(t)

	// Setup order book expectations
	obMock.EXPECT().Lock(mock.Anything).Return(nil).Once()

	// order execution locker expectations
	redisMock.EXPECT().SetNX(mock.Anything, "lock-execution-order:2", 1, mock.Anything).Return(redisBoolCmd(true)).Once()

	// snapshot creation:
	redisMock.EXPECT().Get(mock.Anything, "order:bidLimit:2").Return(
		redis.NewStringResult(`{"ID":2,"Volume":50,"BidOrAsk":true,"Price":99}`, nil),
	).Once()

	// order book update
	obMock.EXPECT().Add(mock.Anything, 99.0, mock.Anything).Return(nil).Once()

	obMock.EXPECT().GetBestOffer(mock.Anything).Return(100.0).Once()

	redisMock.EXPECT().Pipeline().Return(redisPipeliner).Once()

	redisPipeliner.EXPECT().Set(mock.Anything, "order:bidLimit:2", []byte(`{"ID":2,"Volume":50,"BidOrAsk":true,"Price":99}`), mock.Anything).Return(redisStatusCmd("")).Once()
	redisPipeliner.EXPECT().Exec(mock.Anything).Return([]redis.Cmder{}, nil).Once()

	// complete the order execution
	redisMock.EXPECT().Del(mock.Anything, "lock-execution-order:2").Return(redisIntCmd(1)).Once()

	obMock.EXPECT().Unlock(mock.Anything).Return(nil).Once()

	engine := NewExecutionEngine(obMock, redisMock, 1, clockMock)
	defer engine.Stop()

	err := engine.SubmitOrder(orderTaker)
	require.NoError(t, err)

	select {
	case trade := <-engine.tradeChan:
		t.Fatalf("Unexpected trade event received: %+v", trade)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestExecutionEngine_RollbackOnPipelineError(t *testing.T) {
	price := float64(100)

	orderMaker := &Order{
		Id:       1,
		Volume:   100,
		BidOrAsk: false,
		Limit:    &LimitOrder{Price: price},
	}
	orderMakerLimit := NewLimitOrder(price)
	orderMakerLimit.Enqueue(orderMaker)

	orderTaker := &Order{
		Id:       2,
		Volume:   50,
		BidOrAsk: true,
		Limit:    &LimitOrder{Price: price},
	}

	obMock := newMockOrderbookDistributed(t)
	redisMock := newMockRedisClient(t)
	redisPipeliner := newMockRedisPipeliner(t)
	clockMock := newMockClock(t)

	// Setup order book expectations
	obMock.EXPECT().Lock(mock.Anything).Return(nil).Once()

	// order execution locker expectations
	redisMock.EXPECT().SetNX(mock.Anything, "lock-execution-order:2", 1, mock.Anything).Return(redisBoolCmd(true)).Once()

	// snapshot creation:
	redisMock.EXPECT().Get(mock.Anything, "order:bidLimit:2").Return(
		redis.NewStringResult(`{"ID":2,"Volume":50,"BidOrAsk":true,"Price":100}`, nil),
	).Once()

	redisMock.EXPECT().Get(mock.Anything, "order:askLimit:1").Return(
		redis.NewStringResult(`{"ID":1,"Volume":100,"BidOrAsk":false,"Price":100}`, nil),
	).Once()

	obMock.EXPECT().Add(mock.Anything, 100.0, mock.Anything).Return(nil).Once()

	obMock.EXPECT().GetBestOffer(mock.Anything).Return(100.0).Once()

	obMock.EXPECT().GetAskLimit(price).Return(&orderMakerLimit).Once()

	// Pipeline expectations
	redisMock.EXPECT().Pipeline().Return(redisPipeliner).Once()
	redisPipeliner.EXPECT().Del(mock.Anything, "order:bidLimit:2").Return(redisIntCmd(1)).Once()

	redisPipeliner.EXPECT().Set(mock.Anything, "order:askLimit:1", []byte(`{"ID":1,"Volume":50,"BidOrAsk":false,"Price":100}`), mock.Anything).Return(redisStatusCmd("")).Once()

	// Produce error
	redisPipeliner.EXPECT().Exec(mock.Anything).Return(nil, errors.New("pipeline failed")).Once()

	// Rollback expectations
	obMock.EXPECT().Add(mock.Anything, price, orderMaker).Return(nil).Once()
	obMock.EXPECT().Cancel(mock.Anything, 100.0, mock.Anything).Return(nil).Once()

	redisMock.EXPECT().Set(mock.Anything, "order:bidLimit:2", []byte(`{"ID":2,"Volume":50,"BidOrAsk":true,"Price":100}`), time.Duration(0)).Return(redis.NewStatusResult("", nil)).Once()
	redisMock.EXPECT().Set(mock.Anything, "order:askLimit:1", []byte(`{"ID":1,"Volume":100,"BidOrAsk":false,"Price":100}`), time.Duration(0)).Return(redis.NewStatusResult("", nil)).Once()

	// restore the original order
	redisMock.EXPECT().Set(mock.Anything, "order:bidLimit:2", []byte(`{"ID":2,"Volume":50,"BidOrAsk":true,"Price":100}`), time.Duration(0)).Return(redis.NewStatusResult("", nil)).Once()

	// unlock expectations
	redisMock.EXPECT().Del(mock.Anything, "lock-execution-order:2").Return(redisIntCmd(1)).Once()
	obMock.EXPECT().Unlock(mock.Anything).Return(nil).Once()

	// clock expectations
	clockMock.EXPECT().Now().Return(time.Now().UTC()).Once()

	engine := NewExecutionEngine(obMock, redisMock, 1, clockMock)
	defer engine.Stop()

	err := engine.SubmitOrder(orderTaker)
	require.NoError(t, err)
}

func TestExecutionEngine_HighFrequencyStressTest(t *testing.T) {
	const numOrders = 500

	orderMakerBid := &Order{
		Id:       1,
		Volume:   1_000_000,
		BidOrAsk: true,
		Limit:    &LimitOrder{Price: 100},
	}
	orderMakerBidLimit := NewLimitOrder(100)

	orderMakerAsk := &Order{
		Id:       1,
		Volume:   1_000_000,
		BidOrAsk: false,
		Limit:    &LimitOrder{Price: 200},
	}
	orderMakerAskLimit := NewLimitOrder(200)

	// Add orders at levels 100.0 and 200.0 with huge volumes to the order book
	orderMakerBidLimit.Enqueue(orderMakerBid)
	orderMakerAskLimit.Enqueue(orderMakerAsk)

	obMock := newMockOrderbookDistributed(t)
	redisMock := newMockRedisClient(t)
	redisPipeliner := newMockRedisPipeliner(t)
	clockMock := newMockClock(t)

	obMu := &sync.Mutex{}

	// Setup common expectations

	// Setup order book expectations
	obMock.EXPECT().Lock(mock.Anything).RunAndReturn(func(_ context.Context) error {
		obMu.Lock()
		return nil
	}).Times(numOrders)

	// order execution locker expectations
	redisMock.EXPECT().SetNX(mock.Anything, mock.Anything, 1, mock.Anything).Return(redisBoolCmd(true)).Times(numOrders)

	// snapshot creation:
	redisMock.EXPECT().Get(mock.Anything, mock.Anything).Return(
		redis.NewStringResult(``, nil),
	).Times(numOrders * 2)

	// only half of the orders
	obMock.EXPECT().GetBestBid(mock.Anything).Return(100.0).Times(numOrders / 2)
	obMock.EXPECT().GetBestOffer(mock.Anything).Return(200).Times(numOrders / 2)

	// another half of the orders
	obMock.EXPECT().GetBidLimit(100.0).Return(&orderMakerBidLimit).Times(numOrders / 2)
	obMock.EXPECT().GetAskLimit(200.0).Return(&orderMakerBidLimit).Times(numOrders / 2)

	obMock.EXPECT().Add(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(numOrders)

	redisMock.EXPECT().Pipeline().Return(redisPipeliner).Times(numOrders)

	// delete taker order
	redisPipeliner.EXPECT().Del(mock.Anything, mock.Anything).Return(redisIntCmd(1)).Times(numOrders)
	// update maker order
	redisPipeliner.EXPECT().Set(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(redisStatusCmd("")).Times(numOrders)
	// Execute
	redisPipeliner.EXPECT().Exec(mock.Anything).Return(nil, nil).Times(numOrders)

	// unlock expectations
	redisMock.EXPECT().Del(mock.Anything, mock.Anything).Return(redisIntCmd(1)).Times(numOrders)
	obMock.EXPECT().Unlock(mock.Anything).RunAndReturn(func(_ context.Context) error {
		obMu.Unlock()
		return nil
	}).Times(numOrders)

	// clock expectations
	clockMock.EXPECT().Now().Return(time.Now().UTC()).Times(numOrders)

	engine := NewExecutionEngine(obMock, redisMock, 100, clockMock)
	defer engine.Stop()

	var wg sync.WaitGroup
	errors := make(chan error, numOrders)

	wg.Add(numOrders)
	for i := 0; i < numOrders; i++ {
		go func(id int) {
			defer wg.Done()

			bidOrAsk := id%2 == 0
			price := float64(100) // ask at level 100.0
			if bidOrAsk {
				price = 200 // bid at level 200.0
			}

			err := engine.SubmitOrder(&Order{
				Id:       id,
				Volume:   10,
				BidOrAsk: bidOrAsk,
				Limit:    &LimitOrder{Price: price},
			})
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second) // Allow processing time
}

func redisBoolCmd(b bool) *redis.BoolCmd {
	res := redis.NewBoolCmd(context.Background())
	res.SetVal(b)
	return res
}

func redisIntCmd(n int64) *redis.IntCmd {
	res := redis.NewIntCmd(context.Background())
	res.SetVal(n)
	return res
}

func redisStatusCmd(s string) *redis.StatusCmd {
	res := redis.NewStatusCmd(context.Background())
	res.SetVal(s)
	return res
}
func MustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic("cannot marshal the object")
	}

	return data
}
