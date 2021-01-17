package collections

import (
	"sync"
)

type BlockingQueue struct {
	q    *Queue
	cond *sync.Cond
}

func NewBlockingQueue() *BlockingQueue {
	bq := &BlockingQueue{}
	bq.q = NewQueue()
	bq.cond = sync.NewCond(&bq.q.lock)
	return bq
}

// 将元素插入到队列末尾
func (bq *BlockingQueue) Offer(value interface{}) {
	bq.cond.L.Lock()
	defer bq.cond.L.Unlock()

	element := &node{value: value}
	if bq.q.size == 0 {
		bq.q.first = element
		bq.q.last = element
	} else {
		bq.q.last.next = element
		bq.q.last = element
	}
	bq.q.size++

	bq.cond.Signal()
}

//获取队首元素，若成功，则返回队首元素；否则返回null
func (bq *BlockingQueue) Peek() (interface{}, bool) {
	return bq.q.Peek()
}

//移除并获取队首元素，若成功，则返回队首元素；否则返回null
func (bq *BlockingQueue) Poll() (interface{}, bool) {
	return bq.q.Poll()
}

func (bq *BlockingQueue) Size() int {
	return bq.q.Size()
}

func (bq *BlockingQueue) Clear() {
	bq.q.Clear()
}

// 移除并返回队列头部的元素, 如果队列为空，则阻塞
func (bq *BlockingQueue) Take() interface{} {
	bq.cond.L.Lock()
	defer bq.cond.L.Unlock()

	for {
		if bq.q.size == 0 {
			bq.cond.Wait() // 此处唤醒
		} else {
			break
		}
	}

	if bq.q.size == 1 {
		val := bq.q.first.value
		bq.q.first = nil
		bq.q.last = nil
		bq.q.size = 0
		return val
	}

	element := bq.q.first
	bq.q.first = bq.q.first.next
	val := element.value
	element = nil
	bq.q.size--
	return val
}
