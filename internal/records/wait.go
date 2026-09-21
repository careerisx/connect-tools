package records

import (
	"bytes"
	"context"
	"fmt"
)

type waiter struct {
	match  Match
	result chan *Record
}

// Wait returns the first retained or newly appended record matching match.
// Historical lookup and waiter registration share the Append synchronization
// boundary, so a record cannot be lost between those operations.
func (store *Store) Wait(ctx context.Context, match Match) (Record, error) {
	if ctx == nil {
		return Record{}, fmt.Errorf("%w: context must not be nil", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if err := validateCriteria(match.Direction, match.Actor); err != nil {
		return Record{}, err
	}
	match = cloneMatch(match)

	store.mu.Lock()
	if err := store.validateInitializedLocked(); err != nil {
		store.mu.Unlock()
		return Record{}, err
	}
	for element := store.global.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*recordEntry)
		if match.Matches(entry.record) {
			record := &entry.record
			store.mu.Unlock()
			return cloneRecord(*record), nil
		}
	}
	if err := ctx.Err(); err != nil {
		store.mu.Unlock()
		return Record{}, err
	}
	if len(store.waiters) >= store.limits.MaxWaiters {
		store.mu.Unlock()
		return Record{}, fmt.Errorf("%w: maximum of %d record waiters reached", ErrResourceExhausted, store.limits.MaxWaiters)
	}
	id := store.allocateWaiterIDLocked()
	registered := &waiter{
		match:  match,
		result: make(chan *Record, 1),
	}
	store.waiters[id] = registered
	store.mu.Unlock()

	select {
	case record := <-registered.result:
		return cloneRecord(*record), nil
	case <-ctx.Done():
		store.mu.Lock()
		current, waiting := store.waiters[id]
		if waiting && current == registered {
			delete(store.waiters, id)
			store.mu.Unlock()
			return Record{}, ctx.Err()
		}
		store.mu.Unlock()
		// Append removed this waiter and committed its buffered result before the
		// cancellation cleanup acquired the lock, so the matching record wins.
		record := <-registered.result
		return cloneRecord(*record), nil
	}
}

func (store *Store) notifyWaitersLocked(record *Record) {
	for id, registered := range store.waiters {
		if !registered.match.Matches(*record) {
			continue
		}
		// The internal record is immutable after Append publishes it. Sharing its
		// address keeps wake-up bounded and nonblocking; Wait clones after leaving
		// the Store lock before exposing the result to its caller.
		registered.result <- record
		delete(store.waiters, id)
	}
}

func (store *Store) allocateWaiterIDLocked() uint64 {
	for {
		store.nextWaiterID++
		if store.nextWaiterID == 0 {
			store.nextWaiterID++
		}
		if _, exists := store.waiters[store.nextWaiterID]; !exists {
			return store.nextWaiterID
		}
	}
}

func cloneMatch(match Match) Match {
	cloned := match
	cloned.SessionIDs = append([]string(nil), match.SessionIDs...)
	cloned.Contains = bytes.Clone(match.Contains)
	if match.Direction != nil {
		direction := *match.Direction
		cloned.Direction = &direction
	}
	if match.Actor != nil {
		actor := *match.Actor
		cloned.Actor = &actor
	}
	return cloned
}
