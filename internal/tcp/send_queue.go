package tcp

import (
	"bytes"
	"context"
	"sync"

	"connect-tools/internal/records"
)

type sendQueue struct {
	mu     sync.Mutex
	items  []queuedSend
	head   int
	bytes  int
	limits QueueLimits
	closed bool
	ready  chan struct{}
}

func newSendQueue(limits QueueLimits) *sendQueue {
	return &sendQueue{limits: limits, ready: make(chan struct{}, 1)}
}

func (queue *sendQueue) Enqueue(payload []byte, actor records.Actor) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return ErrEndpointNotRunning
	}
	count := len(queue.items) - queue.head
	if count >= queue.limits.Messages || len(payload) > queue.limits.Bytes-queue.bytes {
		return ErrResourceExhausted
	}
	queue.items = append(queue.items, queuedSend{payload: bytes.Clone(payload), actor: actor})
	queue.bytes += len(payload)
	select {
	case queue.ready <- struct{}{}:
	default:
	}
	return nil
}

func (queue *sendQueue) Dequeue(ctx context.Context) (queuedSend, error) {
	for {
		queue.mu.Lock()
		if queue.head < len(queue.items) {
			item := queue.items[queue.head]
			queue.items[queue.head] = queuedSend{}
			queue.head++
			queue.bytes -= len(item.payload)
			if queue.head == len(queue.items) {
				queue.items = nil
				queue.head = 0
			} else if queue.head >= 64 && queue.head*2 >= len(queue.items) {
				queue.items = append([]queuedSend(nil), queue.items[queue.head:]...)
				queue.head = 0
			}
			if queue.head < len(queue.items) {
				select {
				case queue.ready <- struct{}{}:
				default:
				}
			}
			queue.mu.Unlock()
			return item, nil
		}
		if queue.closed {
			queue.mu.Unlock()
			return queuedSend{}, ErrEndpointNotRunning
		}
		queue.mu.Unlock()

		select {
		case <-ctx.Done():
			return queuedSend{}, ctx.Err()
		case <-queue.ready:
		}
	}
}

func (queue *sendQueue) Close() {
	queue.mu.Lock()
	queue.closed = true
	queue.items = nil
	queue.head = 0
	queue.bytes = 0
	queue.mu.Unlock()
	select {
	case queue.ready <- struct{}{}:
	default:
	}
}
