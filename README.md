[![Build Status](https://travis-ci.com/alexey-ernest/go-hft-orderbook.svg?branch=master)](https://travis-ci.com/alexey-ernest/go-hft-orderbook)

# go-hft-orderbook
Golang implementation of a Limit Order Book (LOB) for high frequency trading in crypto exchanges. Inspired by [this](https://web.archive.org/web/20110219163448/http://howtohft.wordpress.com/2011/02/15/how-to-build-a-fast-limit-order-book/) article.

## Operations

* Add – O(log M) for the first order at a limit, O(1) for all others
* Cancel – O(1)
* GetBestBid/Offer – O(1)
* GetVolumeAtLimit – O(1)

## Performance
* Random generated insertion with limited number of price levels (10K levels) on average MacBook Pro: ~200ns/op or ~5M op/s

## TODO
* Object pool (Done)
* Real data for benchmarks


## Radicle URN
rad:git:hwd1yregaqj5mrp5dgc3gyuu6exszg84zr71su8j1f7t6oe9czxee1zzyzr

# Order Execution Engine Component Overview

## 1. Core Classes
The high level UML representation of objects added is in `doc/ClassesAdded.drawio`

### 1.1 `ExecutionEngine`
**Responsibility**: Central order processing system with atomic execution guarantees

**Key Functionality**:
- Concurrent order processing with worker pool pattern
- Real-time trade matching using price-time priority
- Atomic transaction management with rollback capabilities
- Distributed locking for order execution
- Trade event streaming through channel interface

**Critical Methods**:
```go
- SubmitOrder(*Order) error // Order entry point
- processOrders()           // Worker goroutine handler
- executeTransaction()      // Transaction lifecycle management
- rollbackTransaction()     // State restoration mechanism
- commitTransaction()       // Redis persistence handler
```

### 1.2 `TradeEvent`
Purpose: Immutable record of completed trades for downstream systems
**Known issue**: all the events are sent at matching stage. Planned to reimplement.
```go
type TradeEvent struct {
    TakerOrderID int
    MakerOrderID int
    Price        float64
    Volume       float64
    Timestamp    time.Time
}
```

### 1.3 Transaction Management Classes
**`transaction`:**
```go
type transaction struct {
    orderLock     redislock.RedisLock
    order         *Order
    snapshots     map[int]*orderSnapshot
    steps         []transactionStep
    matchedOrders []*Order
    ctx           context.Context
}
```
**Responsibility**:
- Maintains execution context for single order processing
- Manages pre-execution state snapshots
- Tracks undo operations for rollback

**`transactionStep`:** Atomic Operation Unit of transaction
```go
type transactionStep struct {
    undo       func(context.Context) error
    redisState []byte
}
```
**Purpose**: Encapsulates reversible operation with Redis state restoration

### 1.4 State Preservation Classes
**`orderSnapshot`**
```go
type transactionStep struct {
type orderSnapshot struct {
    order     *Order
    redisData []byte
}
```
**Purpose**: Captures pre-transaction state for both memory and Redis

## 2. Distributed Systems Integration
### 2.1 Redis Interaction
**Key Features:**
- State persistence for crash recovery
- Transactional updates using pipelines
- Versioned state storage for rollbacks
- Distributed locking for concurrent access

### 2.2 OrderbookDistributed Interface
**Critical Operations:**
```go
type orderbookDistributed interface {
    Lock(context.Context) error
    Unlock(context.Context) error
    Add(context.Context, float64, *Order) error
    Cancel(context.Context, float64, *Order)
    // Price discovery methods
    GetAskLimit(float64) *LimitOrder
    GetBidLimit(float64) *LimitOrder
    GetBestOffer(context.Context) float64 
    GetBestBid(context.Context) float64
}
```

## 3. Testing
### 3.1 Test Scenarios
- Successful Execution - Validates complete order matching flow
- Successful Adding of a new order witn no match
- Rollback - Tests mid-transaction failure recovery
- Concurrency Stress - High-load locking/throughput verification

## 4. Known issues and trade offs and areas for improvement
### 1.1 Known issues
- Original orderbook does not save queued orders order. The concurrently safe orderbook and the execution engine was implemented assuming the operations of enqueue and syncing from persistent storage ignore this issue.

### 1.2 Areas for improvement
- Trading events should be collected during the order execution and sent only after the transaction is successfully commited
- A separate OrderStorage interface shoulc be implemented, the OrderbookDistributed should have it as a dependency and ExecutionEngine should only call OrderbookDisctributed to Get/Set/Update all the orders in the remote storage.
- The commit of the transaction should be implemented using the Redis native transaction feature. OrderbookDistributed should incapsulate methods of Tx creation and commitment. Also, it should use the transaction if it's received from the caller as a parameter to any operation that updated the remote storage.

