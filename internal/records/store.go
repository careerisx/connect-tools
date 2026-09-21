package records

import (
	"container/list"
	"fmt"
	"math"
	"sync"
	"time"
)

type recordEntry struct {
	record         Record
	globalElement  *list.Element
	sessionElement *list.Element
}

// Store is a thread-safe, bounded, in-memory record store.
type Store struct {
	mu           sync.RWMutex
	limits       Limits
	global       list.List
	bySession    map[string]*list.List
	lastSequence uint64
	recordCount  uint64
	payloadBytes int64
	waiters      map[uint64]*waiter
	nextWaiterID uint64
	observer     Observer
}

// NewStore creates a Store with explicit non-zero limits.
func NewStore(limits Limits) (*Store, error) {
	return NewStoreWithObserver(limits, nil)
}

// NewStoreWithObserver creates a Store with one process-owned observer. The
// observer is immutable and is called after the store lock is released.
func NewStoreWithObserver(limits Limits, observer Observer) (*Store, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	return &Store{
		limits:    limits,
		bySession: make(map[string]*list.List),
		waiters:   make(map[uint64]*waiter),
		observer:  observer,
	}, nil
}

// NewDefaultStore creates a Store using DefaultLimits.
func NewDefaultStore() *Store {
	return NewDefaultStoreWithObserver(nil)
}

// NewDefaultStoreWithObserver creates a Store with default limits and an observer.
func NewDefaultStoreWithObserver(observer Observer) *Store {
	store, err := NewStoreWithObserver(DefaultLimits(), observer)
	if err != nil {
		panic(err)
	}
	return store
}

// Append assigns a monotonic sequence, stores a deep copy, applies retention,
// and wakes every matching waiter.
func (store *Store) Append(record Record) (Record, error) {
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	record = cloneRecord(record)
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	} else {
		record.Timestamp = record.Timestamp.UTC()
	}
	if record.Error != nil && !record.Error.Timestamp.IsZero() {
		record.Error.Timestamp = record.Error.Timestamp.UTC()
	}

	store.mu.Lock()

	if err := store.validateInitializedLocked(); err != nil {
		store.mu.Unlock()
		return Record{}, err
	}
	if store.lastSequence == math.MaxUint64 {
		store.mu.Unlock()
		return Record{}, fmt.Errorf("%w: record sequence space exhausted", ErrResourceExhausted)
	}
	store.lastSequence++
	record.Sequence = store.lastSequence

	entry := &recordEntry{record: record}
	entry.globalElement = store.global.PushBack(entry)
	sessionRecords := store.bySession[record.SessionID]
	if sessionRecords == nil {
		sessionRecords = list.New()
		store.bySession[record.SessionID] = sessionRecords
	}
	entry.sessionElement = sessionRecords.PushBack(entry)
	store.recordCount++
	store.payloadBytes += int64(len(record.Payload))

	var evicted ClearResult
	var evictedSessionIDs []string
	for sessionRecords.Len() > store.limits.PerSessionCount {
		removed := sessionRecords.Front().Value.(*recordEntry)
		evicted.Records++
		evicted.PayloadBytes += int64(len(removed.record.Payload))
		evictedSessionIDs = appendUnique(evictedSessionIDs, removed.record.SessionID)
		store.removeEntryLocked(removed)
	}
	for store.payloadBytes > store.limits.TotalPayloadBytes {
		oldest := store.global.Front()
		if oldest == nil {
			break
		}
		removed := oldest.Value.(*recordEntry)
		evicted.Records++
		evicted.PayloadBytes += int64(len(removed.record.Payload))
		evictedSessionIDs = appendUnique(evictedSessionIDs, removed.record.SessionID)
		store.removeEntryLocked(removed)
	}

	store.notifyWaitersLocked(&entry.record)
	snapshot := cloneRecord(record)
	store.mu.Unlock()
	store.notify(Event{Type: EventAppended, Record: &snapshot})
	if evicted.Records > 0 {
		store.notify(Event{
			Type:                 EventRetentionLimitReached,
			SessionIDs:           evictedSessionIDs,
			AffectedRecords:      evicted.Records,
			AffectedPayloadBytes: evicted.PayloadBytes,
		})
	}
	return snapshot, nil
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

// List returns retained matching records in ascending sequence order.
func (store *Store) List(filter Filter) (Page, error) {
	if err := validateCriteria(filter.Direction, filter.Actor); err != nil {
		return Page{}, err
	}
	limit := filter.Limit
	if limit == 0 {
		limit = DefaultPageLimit
	}
	if limit < 0 || limit > MaxPageLimit {
		return Page{}, fmt.Errorf("%w: page limit must be between 1 and %d", ErrInvalidArgument, MaxPageLimit)
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	if err := store.validateInitializedLocked(); err != nil {
		return Page{}, err
	}

	page := Page{
		Records:    make([]Record, 0, limit),
		NextCursor: filter.After,
	}
	for element := store.global.Front(); element != nil; element = element.Next() {
		record := element.Value.(*recordEntry).record
		if !matchesRecord(filter.SessionIDs, filter.Direction, filter.Actor, filter.Contains, filter.After, record) {
			continue
		}
		if len(page.Records) == limit {
			page.HasMore = true
			break
		}
		page.Records = append(page.Records, cloneRecord(record))
		page.NextCursor = record.Sequence
	}

	if !page.HasMore && store.lastSequence > page.NextCursor {
		// Every retained record has been examined, so advancing across non-matches
		// and already-evicted sequences cannot skip a future matching record.
		page.NextCursor = store.lastSequence
	}
	return page, nil
}

// Clear deletes all retained records matching filter without resetting sequence allocation.
func (store *Store) Clear(filter Filter) (ClearResult, error) {
	if err := validateCriteria(filter.Direction, filter.Actor); err != nil {
		return ClearResult{}, err
	}

	store.mu.Lock()
	if err := store.validateInitializedLocked(); err != nil {
		store.mu.Unlock()
		return ClearResult{}, err
	}

	var result ClearResult
	for element := store.global.Front(); element != nil; {
		next := element.Next()
		entry := element.Value.(*recordEntry)
		if matchesRecord(filter.SessionIDs, filter.Direction, filter.Actor, filter.Contains, filter.After, entry.record) {
			result.Records++
			result.PayloadBytes += int64(len(entry.record.Payload))
			store.removeEntryLocked(entry)
		}
		element = next
	}
	store.mu.Unlock()
	if result.Records > 0 {
		store.notify(Event{
			Type:                 EventCleared,
			SessionIDs:           append([]string(nil), filter.SessionIDs...),
			AffectedRecords:      result.Records,
			AffectedPayloadBytes: result.PayloadBytes,
		})
	}
	return result, nil
}

func (store *Store) notify(event Event) {
	if store.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	store.observer(cloneEvent(event))
}

// Metrics returns an exact snapshot of retained records, payload bytes, and waiters.
func (store *Store) Metrics() Metrics {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return Metrics{
		Records:      store.recordCount,
		PayloadBytes: store.payloadBytes,
		Waiters:      len(store.waiters),
	}
}

func (store *Store) removeEntryLocked(entry *recordEntry) {
	store.global.Remove(entry.globalElement)
	sessionRecords := store.bySession[entry.record.SessionID]
	if sessionRecords != nil {
		sessionRecords.Remove(entry.sessionElement)
		if sessionRecords.Len() == 0 {
			delete(store.bySession, entry.record.SessionID)
		}
	}
	store.recordCount--
	store.payloadBytes -= int64(len(entry.record.Payload))
}

func (store *Store) validateInitializedLocked() error {
	if err := validateLimits(store.limits); err != nil {
		return fmt.Errorf("%w: store is not initialized", ErrInvalidArgument)
	}
	if store.bySession == nil || store.waiters == nil {
		return fmt.Errorf("%w: store is not initialized", ErrInvalidArgument)
	}
	return nil
}
