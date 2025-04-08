package hftorderbook

// Doubly linked orders queue
// TODO: this should be compared with ring buffer queue performance
type ordersQueue struct {
	head *Order
	tail *Order
	size int
}

func NewOrdersQueue() ordersQueue {
	return ordersQueue{}
}

func (oq *ordersQueue) Size() int {
	return oq.size
}

func (oq *ordersQueue) IsEmpty() bool {
	return oq.size == 0
}

func (oq *ordersQueue) Enqueue(o *Order) {
	tail := oq.tail
	oq.tail = o
	if tail != nil {
		tail.Next = o
		o.Prev = tail
	}
	if oq.head == nil {
		oq.head = o
	}
	oq.size++
}

func (oq *ordersQueue) Dequeue() *Order {
	if oq.size == 0 {
		return nil
	}

	head := oq.head
	if oq.tail == oq.head {
		oq.tail = nil
	}

	oq.head = oq.head.Next
	oq.size--
	return head
}

func (oq *ordersQueue) Delete(o *Order) {
	prev := o.Prev
	next := o.Next
	if prev != nil {
		prev.Next = next
	}
	if next != nil {
		next.Prev = prev
	}
	o.Next = nil
	o.Prev = nil

	oq.size--

	if oq.head == o {
		oq.head = next
	}
	if oq.tail == o {
		oq.tail = prev
	}
}
