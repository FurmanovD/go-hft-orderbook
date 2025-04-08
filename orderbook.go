package hftorderbook

import (
	"fmt"
	"sync"
)

// maximum limits per orderbook side to pre-allocate memory
const MaxLimitsNum int = 10000

type Orderbook struct {
	Bids *redBlackBST
	Asks *redBlackBST

	bidLimitsCache map[float64]*LimitOrder
	askLimitsCache map[float64]*LimitOrder
	pool           *sync.Pool
}

func NewOrderbook() Orderbook {
	bids := NewRedBlackBST()
	asks := NewRedBlackBST()
	return Orderbook{
		Bids: &bids,
		Asks: &asks,

		bidLimitsCache: make(map[float64]*LimitOrder, MaxLimitsNum),
		askLimitsCache: make(map[float64]*LimitOrder, MaxLimitsNum),
		pool: &sync.Pool{
			New: func() interface{} {
				limit := NewLimitOrder(0.0)
				return &limit
			},
		},
	}
}

func (ob *Orderbook) Add(price float64, o *Order) {
	var limit *LimitOrder

	if o.BidOrAsk {
		limit = ob.bidLimitsCache[price]
	} else {
		limit = ob.askLimitsCache[price]
	}

	if limit == nil {
		// getting a new limit from pool
		limit = ob.pool.Get().(*LimitOrder)
		limit.Price = price

		// insert into the corresponding BST and cache
		if o.BidOrAsk {
			ob.Bids.Put(price, limit)
			ob.bidLimitsCache[price] = limit
		} else {
			ob.Asks.Put(price, limit)
			ob.askLimitsCache[price] = limit
		}
	}

	// add order to the limit
	limit.Enqueue(o)
}

func (ob *Orderbook) Cancel(o *Order) {
	limit := o.Limit
	limit.Delete(o)

	if limit.Size() == 0 {
		// remove the limit if there are no orders
		if o.BidOrAsk {
			ob.Bids.Delete(limit.Price)
			delete(ob.bidLimitsCache, limit.Price)
		} else {
			ob.Asks.Delete(limit.Price)
			delete(ob.askLimitsCache, limit.Price)
		}

		// put it back to the pool
		ob.pool.Put(limit)
	}
}

func (ob *Orderbook) ClearBidLimit(price float64) {
	ob.clearLimit(price, true)
}

func (ob *Orderbook) ClearAskLimit(price float64) {
	ob.clearLimit(price, false)
}

func (ob *Orderbook) clearLimit(price float64, bidOrAsk bool) {
	var limit *LimitOrder
	if bidOrAsk {
		limit = ob.bidLimitsCache[price]
	} else {
		limit = ob.askLimitsCache[price]
	}

	if limit == nil {
		panic(fmt.Sprintf("there is no such price limit %0.8f", price))
	}

	limit.Clear()
}

func (ob *Orderbook) DeleteBidLimit(price float64) {
	limit := ob.bidLimitsCache[price]
	if limit == nil {
		return
	}

	ob.deleteLimit(price, true)
	delete(ob.bidLimitsCache, price)

	// put limit back to the pool
	limit.Clear()
	ob.pool.Put(limit)

}

func (ob *Orderbook) DeleteAskLimit(price float64) {
	limit := ob.askLimitsCache[price]
	if limit == nil {
		return
	}

	ob.deleteLimit(price, false)
	delete(ob.askLimitsCache, price)

	// put limit back to the pool
	limit.Clear()
	ob.pool.Put(limit)
}

func (ob *Orderbook) deleteLimit(price float64, bidOrAsk bool) {
	if bidOrAsk {
		ob.Bids.Delete(price)
	} else {
		ob.Asks.Delete(price)
	}
}

func (ob *Orderbook) GetVolumeAtBidLimit(price float64) float64 {
	limit := ob.bidLimitsCache[price]
	if limit == nil {
		return 0
	}
	return limit.TotalVolume()
}

func (ob *Orderbook) GetVolumeAtAskLimit(price float64) float64 {
	limit := ob.askLimitsCache[price]
	if limit == nil {
		return 0
	}
	return limit.TotalVolume()
}

func (ob *Orderbook) GetBestBid() float64 {
	return ob.Bids.Max()
}

func (ob *Orderbook) GetBestOffer() float64 {
	return ob.Asks.Min()
}

func (ob *Orderbook) BLength() int {
	return len(ob.bidLimitsCache)
}

func (ob *Orderbook) ALength() int {
	return len(ob.askLimitsCache)
}

func (ob *Orderbook) GetBidLimit(price float64) *LimitOrder {
	return ob.bidLimitsCache[price]
}

func (ob *Orderbook) GetAskLimit(price float64) *LimitOrder {
	return ob.askLimitsCache[price]
}
