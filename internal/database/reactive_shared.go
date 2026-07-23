package database

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	maxPendingReactiveBatches = 1024
	maxPendingReactiveChanges = 64 * 1024
	// The hub can be rebuilding a view while the dispatcher continues to hand it
	// ordered batches, so it needs its own byte boundary rather than relying on
	// the upstream dispatch queue.
	maxPendingReactiveBytes uint64 = 64 << 20
)

// reactiveHub owns one canonical view per effective collection/query pair.
// Commits enqueue one shared immutable ChangeBatch reference. Query state and
// subscriber delivery never run on the commit goroutine.
type reactiveHub struct {
	db *DB

	mu             sync.Mutex
	views          map[string]*sharedReactiveView
	byCollection   map[string]map[string]*sharedReactiveView
	orders         map[string]*reactiveCollectionOrder
	queue          []ChangeBatch
	pendingChanges int
	pendingBytes   uint64
	maxBatches     int
	maxChanges     int
	maxBytes       uint64
	prioritySeed   maphash.Seed
	resync         map[string]struct{}
	changed        chan struct{}
	done           chan struct{}
	nextID         uint64
	started        bool
	closed         bool
}

type reactiveDependencySignature struct {
	paths       []string
	hasOrdering bool
	hasWindow   bool
}

type sharedReactiveView struct {
	key          string
	collection   string
	query        QuerySpec
	dependencies reactiveDependencySignature
	state        atomic.Pointer[reactiveViewState]
	subscribers  map[uint64]*sharedQuerySubscriber // protected by hub.mu
}

// reactiveViewState is immutable after publication. Member documents and the
// output snapshot never alias public subscriber documents.
type reactiveViewState struct {
	token       uint64
	memberCount uint64
	memberBytes uint64
	byID        *reactiveIDNode
	ordered     *reactiveOrderNode
	snapshot    QuerySnapshot
}

type reactiveMember struct {
	document Document
	position uint64
	bytes    uint64
}

func reactiveMemberCanonicalBytes(member reactiveMember) (uint64, error) {
	if member.bytes != 0 {
		return member.bytes, nil
	}
	return canonicalDocumentSize(member.document)
}

// One order table is shared by every query on a collection. It preserves the
// insertion-order tie breaker without storing all nonmatching IDs per view.
type reactiveCollectionOrder struct {
	mu        sync.RWMutex
	token     uint64
	next      uint64
	positions map[DocumentID]uint64
}

type sharedQuerySubscriber struct {
	mu        sync.Mutex
	id        uint64
	snapshots chan QuerySnapshot
	initial   QuerySnapshot
	deltas    chan QueryDelta
	errors    chan error
	cancel    context.CancelFunc
	stop      func() bool
	lastToken uint64
	closed    bool
}

type reactiveDeliveryStatus uint8

const (
	reactiveDeliverySkipped reactiveDeliveryStatus = iota
	reactiveDeliverySent
	reactiveDeliverySlow
	reactiveDeliveryInvalid
)

func newReactiveHub(db *DB) *reactiveHub {
	return &reactiveHub{
		db: db, views: make(map[string]*sharedReactiveView),
		byCollection: make(map[string]map[string]*sharedReactiveView),
		orders:       make(map[string]*reactiveCollectionOrder),
		maxBatches:   maxPendingReactiveBatches,
		maxChanges:   maxPendingReactiveChanges,
		maxBytes:     maxPendingReactiveBytes,
		prioritySeed: maphash.MakeSeed(),
		resync:       make(map[string]struct{}),
		changed:      make(chan struct{}), done: make(chan struct{}),
	}
}

func (hub *reactiveHub) subscribe(ctx context.Context, collection string, query QuerySpec, canonical []byte, buffer int, cancel context.CancelFunc, deltas bool) (*sharedQuerySubscriber, error) {
	return hub.subscribeMode(ctx, collection, query, canonical, buffer, cancel, deltas, true)
}

// subscribeMode establishes a subscriber with a short database read-lock
// boundary. Cold  collections may build their first immutable view outside
// that boundary and then prove the snapshot token is still current before
// registration; all warm/retry paths retain the established lock-held logic.
func (hub *reactiveHub) subscribeMode(ctx context.Context, collection string, query QuerySpec, canonical []byte, buffer int, cancel context.CancelFunc, deltas, allowColdStorage bool) (*sharedQuerySubscriber, error) {
	if hub == nil || hub.db == nil {
		return nil, ErrClosed
	}
	key := collection + "\x00" + string(canonical)
	db := hub.db
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	currentToken := db.token

	hub.mu.Lock()
	view := hub.views[key]
	coldStorage := allowColdStorage && db.querySource != nil && len(hub.byCollection[collection]) == 0
	if view != nil {
		state := view.state.Load()
		if state != nil && state.token == currentToken {
			subscriber, err := hub.addSubscriberLocked(view, state.snapshot, buffer, cancel, deltas)
			hub.mu.Unlock()
			db.mu.RUnlock()
			if err != nil {
				return nil, err
			}
			db.metrics.sharedViewReuses.Add(1)
			hub.watchContext(ctx, key, view, subscriber)
			return subscriber, nil
		}
	}
	hub.mu.Unlock()
	if coldStorage {
		db.mu.RUnlock()
		return hub.subscribeColdStorage(ctx, collection, query, canonical, buffer, cancel, deltas)
	}

	order := hub.ensureOrder(collection)
	var refreshed *reactiveViewState
	var err error
	if db.querySource != nil {
		states, buildErr := buildStorageReactiveViewStates(db.querySource, currentToken, collection, []QuerySpec{query}, order, hub.prioritySeed, db.resourceLimits)
		if buildErr == nil {
			refreshed = states[0]
		}
		err = buildErr
	} else {
		data := db.collections[collection]
		if err = order.rebuild(data, currentToken); err == nil {
			refreshed, err = buildReactiveViewState(query, data, order, hub.prioritySeed, currentToken, db.resourceLimits)
		}
	}
	if err != nil {
		if errors.Is(err, ErrResourceLimit) {
			db.metrics.resourceLimitRejections.Add(1)
		}
		db.mu.RUnlock()
		return nil, err
	}
	var deliveries []*sharedQuerySubscriber
	var deliverySnapshot QuerySnapshot
	var deliveryPrevious *reactiveViewState
	created := false
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	view = hub.views[key]
	if view == nil {
		view = &sharedReactiveView{
			key: key, collection: collection, query: query,
			dependencies: dependencySignature(query), subscribers: make(map[uint64]*sharedQuerySubscriber),
		}
		view.state.Store(refreshed)
		hub.views[key] = view
		group := hub.byCollection[collection]
		if group == nil {
			group = make(map[string]*sharedReactiveView)
			hub.byCollection[collection] = group
		}
		group[key] = view
		db.metrics.sharedViews.Add(1)
		created = true
	} else {
		published, changed, previous := publishNewerViewState(view, refreshed)
		if published && changed {
			deliveries = subscribersOf(view)
			deliverySnapshot = refreshed.snapshot
			deliveryPrevious = previous
		}
		if published {
			db.metrics.queryRecomputes.Add(1)
			db.metrics.fullViewRecomputes.Add(1)
		}
	}
	state := view.state.Load()
	subscriber, err := hub.addSubscriberLocked(view, state.snapshot, buffer, cancel, deltas)
	hub.mu.Unlock()
	db.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if !created {
		db.metrics.sharedViewReuses.Add(1)
	}
	var deliveryDelta *sharedQueryDelta
	if len(deliveries) > 0 {
		deliveryDelta, err = buildSharedQueryDelta(deliveryPrevious.snapshot.Documents, deliverySnapshot.Documents, deliverySnapshot.Token)
		if err != nil {
			hub.failView(view, err)
			return nil, err
		}
		db.metrics.sharedDeltas.Add(1)
	}
	for _, existing := range deliveries {
		hub.deliver(key, view, existing, deliverySnapshot, deliveryDelta)
	}
	hub.watchContext(ctx, key, view, subscriber)
	return subscriber, nil
}

const maxColdReactiveSubscriptionAttempts = 3

// subscribeColdStorage avoids holding db.mu while a first view scans its
// immutable storage snapshot. A current-token check under db.mu before
// publication closes the snapshot/register gap; repeated write contention
// falls back to the original lock-held path rather than weakening ordering.
func (hub *reactiveHub) subscribeColdStorage(ctx context.Context, collection string, query QuerySpec, canonical []byte, buffer int, cancel context.CancelFunc, deltas bool) (*sharedQuerySubscriber, error) {
	if hub == nil || hub.db == nil || hub.db.querySource == nil {
		return nil, ErrClosed
	}
	db, key := hub.db, collection+"\x00"+string(canonical)
	for attempt := 0; attempt < maxColdReactiveSubscriptionAttempts; attempt++ {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return nil, ErrClosed
		}
		token := db.token
		db.mu.RUnlock()

		order := &reactiveCollectionOrder{positions: make(map[DocumentID]uint64)}
		states, err := buildStorageReactiveViewStates(db.querySource, token, collection, []QuerySpec{query}, order, hub.prioritySeed, db.resourceLimits)
		if err != nil {
			if errors.Is(err, ErrResourceLimit) {
				db.metrics.resourceLimitRejections.Add(1)
			}
			return nil, err
		}
		refreshed := states[0]

		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return nil, ErrClosed
		}
		if db.token != token {
			db.mu.RUnlock()
			continue
		}
		hub.mu.Lock()
		if hub.closed {
			hub.mu.Unlock()
			db.mu.RUnlock()
			return nil, ErrClosed
		}
		if existing := hub.views[key]; existing != nil {
			state := existing.state.Load()
			if state != nil && state.token == token {
				subscriber, err := hub.addSubscriberLocked(existing, state.snapshot, buffer, cancel, deltas)
				hub.mu.Unlock()
				db.mu.RUnlock()
				if err != nil {
					return nil, err
				}
				db.metrics.sharedViewReuses.Add(1)
				hub.watchContext(ctx, key, existing, subscriber)
				return subscriber, nil
			}
		}
		// Another query on this collection began while the cold snapshot was
		// scanning. Its shared order may be advancing, so retain the original
		// lock-held path instead of installing an independent order table.
		if len(hub.byCollection[collection]) != 0 {
			hub.mu.Unlock()
			db.mu.RUnlock()
			return hub.subscribeMode(ctx, collection, query, canonical, buffer, cancel, deltas, false)
		}
		view := &sharedReactiveView{
			key: key, collection: collection, query: query,
			dependencies: dependencySignature(query), subscribers: make(map[uint64]*sharedQuerySubscriber),
		}
		view.state.Store(refreshed)
		hub.orders[collection] = order
		hub.views[key] = view
		hub.byCollection[collection] = map[string]*sharedReactiveView{key: view}
		db.metrics.sharedViews.Add(1)
		subscriber, err := hub.addSubscriberLocked(view, refreshed.snapshot, buffer, cancel, deltas)
		hub.mu.Unlock()
		db.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		hub.watchContext(ctx, key, view, subscriber)
		return subscriber, nil
	}
	return hub.subscribeMode(ctx, collection, query, canonical, buffer, cancel, deltas, false)
}

func dependencySignature(query QuerySpec) reactiveDependencySignature {
	_, limited := query.Limit()
	return reactiveDependencySignature{
		paths: query.Paths(), hasOrdering: len(query.sort) > 0,
		hasWindow: query.skip > 0 || limited,
	}
}

func (hub *reactiveHub) ensureOrder(collection string) *reactiveCollectionOrder {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	order := hub.orders[collection]
	if order == nil {
		order = &reactiveCollectionOrder{positions: make(map[DocumentID]uint64)}
		hub.orders[collection] = order
	}
	return order
}

func (order *reactiveCollectionOrder) rebuild(data *collectionData, token uint64) error {
	order.mu.Lock()
	defer order.mu.Unlock()
	positions := make(map[DocumentID]uint64)
	next := order.next
	if data != nil {
		for _, id := range data.order {
			if _, exists := data.documents[id]; !exists {
				continue
			}
			position, exists := order.positions[id]
			if !exists {
				if next == ^uint64(0) {
					return ErrCorrupt
				}
				next++
				position = next
			}
			positions[id] = position
		}
	}
	order.positions, order.token, order.next = positions, token, next
	return nil
}

func (order *reactiveCollectionOrder) replace(token, next uint64, positions map[DocumentID]uint64) {
	order.mu.Lock()
	order.token, order.next, order.positions = token, next, positions
	order.mu.Unlock()
}

// buildStorageReactiveViewStates scans one immutable Primary snapshot for all
// views being rebuilt. Only matching documents are retained by each view; the
// collection-wide structure stores IDs and insertion positions, not decoded
// documents. Snapshot and iterator pins are released before publication.
func buildStorageReactiveViewStates(source querySnapshotSource, expectedToken uint64, collection string, queries []QuerySpec, order *reactiveCollectionOrder, seed maphash.Seed, limits ResourceLimits) ([]*reactiveViewState, error) {
	if source == nil || order == nil || len(queries) == 0 {
		return nil, ErrCorrupt
	}
	snapshot, err := source.openQuerySnapshot()
	if err != nil {
		return nil, err
	}
	if snapshot.Sequence() != expectedToken {
		_ = snapshot.Close()
		return nil, ErrCorrupt
	}
	iterator, err := snapshot.OpenCollectionIterator(collection)
	if err != nil {
		_ = snapshot.Close()
		return nil, err
	}
	finish := func(operationErr error) error {
		return errors.Join(operationErr, iterator.Close(), snapshot.Close())
	}
	positions := make(map[DocumentID]uint64)
	positionOwners := make(map[uint64]DocumentID)
	entries := make([][]reactiveTreeEntry, len(queries))
	memberCounts := make([]uint64, len(queries))
	memberBytes := make([]uint64, len(queries))
	var next uint64
	for iterator.Next() {
		record := iterator.Record()
		id, documentIDExists := record.Decoded.ID()
		if record.ID.IsZero() || record.Position == 0 || !documentIDExists || id != record.ID {
			return nil, finish(ErrCorrupt)
		}
		if _, duplicate := positions[id]; duplicate {
			return nil, finish(ErrCorrupt)
		}
		if _, duplicate := positionOwners[record.Position]; duplicate {
			return nil, finish(ErrCorrupt)
		}
		positions[id] = record.Position
		positionOwners[record.Position] = id
		if record.Position > next {
			next = record.Position
		}
		var documentBytes uint64
		documentBytesKnown := false
		for index, query := range queries {
			if query.Match(record.Decoded) {
				if !documentBytesKnown {
					documentBytes, err = canonicalDocumentSize(record.Decoded)
					if err != nil {
						return nil, finish(err)
					}
					documentBytesKnown = true
				}
				memberCounts[index], memberBytes[index], err = admitReactiveViewMember(limits, memberCounts[index], memberBytes[index], documentBytes)
				if err != nil {
					return nil, finish(err)
				}
				entries[index] = append(entries[index], reactiveTreeEntry{
					id: id, member: reactiveMember{document: record.Decoded, position: record.Position, bytes: documentBytes},
				})
			}
		}
	}
	if err := finish(iterator.Err()); err != nil {
		return nil, err
	}
	states := make([]*reactiveViewState, len(queries))
	for index, query := range queries {
		byID, ordered := buildReactiveTrees(seed, query, entries[index])
		states[index] = &reactiveViewState{
			token: expectedToken, memberCount: memberCounts[index], memberBytes: memberBytes[index], byID: byID, ordered: ordered,
			snapshot: QuerySnapshot{Token: expectedToken, Documents: materializeReactiveOrder(ordered, query.skip, query.limit)},
		}
	}
	order.replace(expectedToken, next, positions)
	return states, nil
}

func buildReactiveViewState(query QuerySpec, data *collectionData, order *reactiveCollectionOrder, seed maphash.Seed, token uint64, limits ResourceLimits) (*reactiveViewState, error) {
	entries := make([]reactiveTreeEntry, 0)
	var memberCount, memberBytes uint64
	order.mu.RLock()
	defer order.mu.RUnlock()
	if data != nil {
		for _, id := range data.order {
			document, exists := data.documents[id]
			if !exists {
				continue
			}
			position, exists := order.positions[id]
			if !exists {
				return nil, ErrCorrupt
			}
			if query.Match(document) {
				size, err := canonicalDocumentSize(document)
				if err != nil {
					return nil, err
				}
				memberCount, memberBytes, err = admitReactiveViewMember(limits, memberCount, memberBytes, size)
				if err != nil {
					return nil, err
				}
				// collectionData owns immutable document versions: updates replace a
				// complete Document rather than mutating it. The view may share that
				// internal version; public snapshots still clone at the boundary.
				member := reactiveMember{document: document, position: position, bytes: size}
				entries = append(entries, reactiveTreeEntry{id: id, member: member})
			}
		}
	}
	byID, ordered := buildReactiveTrees(seed, query, entries)
	documents := materializeReactiveOrder(ordered, query.skip, query.limit)
	return &reactiveViewState{token: token, memberCount: memberCount, memberBytes: memberBytes, byID: byID, ordered: ordered, snapshot: QuerySnapshot{Token: token, Documents: documents}}, nil
}

func publishNewerViewState(view *sharedReactiveView, next *reactiveViewState) (published, changed bool, previous *reactiveViewState) {
	for {
		current := view.state.Load()
		if current != nil && current.token >= next.token {
			return false, false, current
		}
		changed = current == nil || !documentSlicesEqual(current.snapshot.Documents, next.snapshot.Documents)
		if view.state.CompareAndSwap(current, next) {
			return true, changed, current
		}
	}
}

func (hub *reactiveHub) addSubscriberLocked(view *sharedReactiveView, snapshot QuerySnapshot, buffer int, cancel context.CancelFunc, deltas bool) (*sharedQuerySubscriber, error) {
	if hub.closed || view == nil {
		return nil, ErrClosed
	}
	hub.nextID++
	if hub.nextID == 0 {
		return nil, ErrCorrupt
	}
	subscriber := &sharedQuerySubscriber{
		id: hub.nextID, errors: make(chan error, 1), cancel: cancel,
		lastToken: snapshot.Token,
	}
	if deltas {
		subscriber.initial = cloneSnapshot(snapshot)
		subscriber.deltas = make(chan QueryDelta, buffer)
	} else {
		subscriber.snapshots = make(chan QuerySnapshot, buffer)
		subscriber.snapshots <- cloneSnapshot(snapshot)
	}
	view.subscribers[subscriber.id] = subscriber
	if !hub.started {
		hub.started = true
		go hub.run()
	}
	hub.db.metrics.querySubscribers.Add(1)
	hub.db.metrics.initialSnapshots.Add(1)
	hub.db.metrics.snapshotsEmitted.Add(1)
	hub.db.metrics.documentsEmitted.Add(uint64(len(snapshot.Documents)))
	return subscriber, nil
}

func (hub *reactiveHub) watchContext(ctx context.Context, key string, view *sharedReactiveView, subscriber *sharedQuerySubscriber) {
	stop := context.AfterFunc(ctx, func() {
		hub.detach(key, view, subscriber)
		subscriber.finish(nil)
	})
	subscriber.mu.Lock()
	if subscriber.closed {
		subscriber.mu.Unlock()
		stop()
		return
	}
	subscriber.stop = stop
	subscriber.mu.Unlock()
}

func (hub *reactiveHub) notify(batch ChangeBatch) {
	if hub == nil || len(batch.Changes) == 0 {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed || !hub.started {
		return
	}
	relevant := make(map[string]struct{})
	wakeups := uint64(0)
	for _, change := range batch.Changes {
		if _, seen := relevant[change.Collection]; seen {
			continue
		}
		views := hub.byCollection[change.Collection]
		staleViews := uint64(0)
		for _, view := range views {
			state := view.state.Load()
			if state == nil || state.token < batch.Token {
				staleViews++
			}
		}
		if staleViews > 0 {
			relevant[change.Collection] = struct{}{}
			wakeups += staleViews
		}
	}
	if len(relevant) == 0 {
		return
	}
	bytes := changeBatchDispatchBytes(batch)
	overflow := hub.maxBatches <= 0 || hub.maxChanges <= 0 || hub.maxBytes == 0 || len(hub.queue) >= hub.maxBatches ||
		hub.pendingChanges > hub.maxChanges || len(batch.Changes) > hub.maxChanges-hub.pendingChanges ||
		bytes > hub.maxBytes || hub.pendingBytes > hub.maxBytes-bytes
	if overflow {
		for collection := range hub.byCollection {
			hub.resync[collection] = struct{}{}
		}
		hub.queue = nil
		hub.pendingChanges = 0
		hub.pendingBytes = 0
		hub.db.metrics.reactiveQueueOverflows.Add(1)
	} else {
		// Commit code transfers immutable ownership to the hub. One shared slice
		// is retained regardless of subscriber count.
		hub.queue = append(hub.queue, batch)
		hub.pendingChanges += len(batch.Changes)
		hub.pendingBytes += bytes
	}
	hub.db.metrics.watcherDeliveries.Add(wakeups)
	hub.db.metrics.pendingReactiveBatches.Store(uint64(len(hub.queue)))
	hub.db.metrics.pendingReactiveChanges.Store(uint64(hub.pendingChanges))
	hub.db.metrics.pendingReactiveBytes.Store(hub.pendingBytes)
	close(hub.changed)
	hub.changed = make(chan struct{})
}

// resyncAll is the safe loss-of-continuity path used by the upstream change
// dispatcher. It never synthesizes a delta across omitted commits: each live
// view is rebuilt from one current atomic database snapshot instead.
func (hub *reactiveHub) resyncAll() {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed || !hub.started {
		return
	}
	for collection := range hub.byCollection {
		hub.resync[collection] = struct{}{}
	}
	if len(hub.resync) == 0 {
		return
	}
	close(hub.changed)
	hub.changed = make(chan struct{})
}

func (hub *reactiveHub) run() {
	for {
		hub.mu.Lock()
		if hub.closed {
			hub.mu.Unlock()
			return
		}
		if len(hub.queue) > 0 || len(hub.resync) > 0 {
			hub.mu.Unlock()
			hub.processPending()
			continue
		}
		changed, done := hub.changed, hub.done
		hub.mu.Unlock()
		select {
		case <-done:
			return
		case <-changed:
		}
	}
}

func (hub *reactiveHub) processPending() {
	hub.mu.Lock()
	batches := append([]ChangeBatch(nil), hub.queue...)
	hub.queue = nil
	hub.pendingChanges = 0
	hub.pendingBytes = 0
	resync := make([]string, 0, len(hub.resync))
	for collection := range hub.resync {
		resync = append(resync, collection)
		delete(hub.resync, collection)
	}
	hub.db.metrics.pendingReactiveBatches.Store(0)
	hub.db.metrics.pendingReactiveChanges.Store(0)
	hub.db.metrics.pendingReactiveBytes.Store(0)
	hub.mu.Unlock()

	for _, collection := range resync {
		hub.fullRecomputeCollection(collection)
	}
	for _, batch := range batches {
		hub.applyBatch(batch)
	}
}

func (hub *reactiveHub) applyBatch(batch ChangeBatch) {
	grouped := make(map[string][]Change)
	hub.mu.Lock()
	for _, change := range batch.Changes {
		if len(hub.byCollection[change.Collection]) > 0 {
			grouped[change.Collection] = append(grouped[change.Collection], change)
		}
	}
	hub.mu.Unlock()
	if len(grouped) == 0 {
		return
	}
	hub.db.metrics.incrementalBatches.Add(1)
	for collection, changes := range grouped {
		order := hub.ensureOrder(collection)
		if !order.apply(batch.Token, changes) {
			hub.fullRecomputeCollection(collection)
			continue
		}
		hub.mu.Lock()
		views := make([]*sharedReactiveView, 0, len(hub.byCollection[collection]))
		for _, view := range hub.byCollection[collection] {
			views = append(views, view)
		}
		hub.mu.Unlock()
		for _, view := range views {
			if !hub.applyViewBatch(view, order, batch.Token, changes) {
				hub.fullRecomputeCollection(collection)
				break
			}
		}
	}
}

func (order *reactiveCollectionOrder) apply(token uint64, changes []Change) bool {
	order.mu.Lock()
	defer order.mu.Unlock()
	if token <= order.token {
		return true
	}
	for _, change := range changes {
		switch change.Operation {
		case InsertOperation:
			if _, exists := order.positions[change.DocumentID]; exists {
				return false
			}
			if order.next == ^uint64(0) {
				return false
			}
			order.next++
			order.positions[change.DocumentID] = order.next
		case UpdateOperation:
			if _, exists := order.positions[change.DocumentID]; !exists {
				return false
			}
		case DeleteOperation:
			if _, exists := order.positions[change.DocumentID]; !exists {
				return false
			}
			delete(order.positions, change.DocumentID)
		case CreateIndexOperation:
		default:
			return false
		}
	}
	order.token = token
	return true
}

func (order *reactiveCollectionOrder) position(id DocumentID) (uint64, bool) {
	order.mu.RLock()
	position, exists := order.positions[id]
	order.mu.RUnlock()
	return position, exists
}

func (hub *reactiveHub) applyViewBatch(view *sharedReactiveView, order *reactiveCollectionOrder, token uint64, changes []Change) bool {
	for {
		current := view.state.Load()
		next, valid, err := transitionReactiveViewState(current, view.query, order, hub.prioritySeed, token, changes, hub.db.resourceLimits)
		if err != nil {
			if errors.Is(err, ErrResourceLimit) {
				hub.db.metrics.resourceLimitRejections.Add(1)
			}
			hub.failView(view, err)
			return true
		}
		if !valid {
			return false
		}
		if next == current {
			return true
		}
		changed := !documentSlicesEqual(current.snapshot.Documents, next.snapshot.Documents)
		if !view.state.CompareAndSwap(current, next) {
			continue
		}
		hub.db.metrics.queryRecomputes.Add(1)
		hub.db.metrics.incrementalViewUpdates.Add(1)
		if changed {
			delta, err := buildSharedQueryDelta(current.snapshot.Documents, next.snapshot.Documents, token)
			if err != nil {
				hub.failView(view, err)
				return true
			}
			hub.db.metrics.sharedDeltas.Add(1)
			hub.deliverView(view, next.snapshot, delta)
		}
		return true
	}
}

// transitionReactiveViewState is the single incremental membership/order
// algorithm used by both process-local commits and durable historical replay.
// It is pure with respect to view state; order must already include the batch.
func transitionReactiveViewState(current *reactiveViewState, query QuerySpec, order *reactiveCollectionOrder, seed maphash.Seed, token uint64, changes []Change, limits ResourceLimits) (*reactiveViewState, bool, error) {
	if current == nil {
		return nil, false, nil
	}
	if token <= current.token {
		return current, true, nil
	}
	byID, ordered := current.byID, current.ordered
	memberCount, memberBytes := current.memberCount, current.memberBytes
	mutated := false
	for _, change := range changes {
		if change.Operation == CreateIndexOperation {
			continue
		}
		member, wasMember := reactiveIDGet(byID, change.DocumentID)
		beforeMatches := change.Before != nil && query.Match(*change.Before)
		afterMatches := change.After != nil && query.Match(*change.After)
		if beforeMatches != wasMember {
			return nil, false, nil
		}
		if wasMember && !afterMatches {
			memberSize, err := reactiveMemberCanonicalBytes(member)
			if err != nil || memberCount == 0 || memberSize > memberBytes {
				return nil, false, err
			}
			memberCount--
			memberBytes -= memberSize
			byID = reactiveIDDelete(byID, change.DocumentID)
			ordered = reactiveOrderDelete(ordered, query, change.DocumentID, member)
			mutated = true
			continue
		}
		if !afterMatches {
			continue
		}
		position := member.position
		if !wasMember {
			var exists bool
			position, exists = order.position(change.DocumentID)
			if !exists {
				return nil, false, nil
			}
		}
		if wasMember {
			memberSize, err := reactiveMemberCanonicalBytes(member)
			if err != nil || memberCount == 0 || memberSize > memberBytes {
				return nil, false, err
			}
			memberCount--
			memberBytes -= memberSize
			byID = reactiveIDDelete(byID, change.DocumentID)
			ordered = reactiveOrderDelete(ordered, query, change.DocumentID, member)
		}
		// The commit transfers this immutable After version to the state. Public
		// snapshot and delta boundaries remain responsible for deep cloning.
		size, err := changeAfterCanonicalBytes(change)
		if err != nil {
			return nil, false, err
		}
		memberCount, memberBytes, err = admitReactiveViewMember(limits, memberCount, memberBytes, size)
		if err != nil {
			return nil, false, err
		}
		nextMember := reactiveMember{document: *change.After, position: position, bytes: size}
		byID = reactiveIDPut(byID, seed, change.DocumentID, nextMember)
		ordered = reactiveOrderPut(ordered, seed, query, change.DocumentID, nextMember)
		mutated = true
	}
	next := &reactiveViewState{token: token, memberCount: memberCount, memberBytes: memberBytes, byID: byID, ordered: ordered}
	if mutated {
		next.snapshot = QuerySnapshot{Token: token, Documents: materializeReactiveOrder(ordered, query.skip, query.limit)}
	} else {
		next.snapshot = QuerySnapshot{Token: token, Documents: current.snapshot.Documents}
	}
	return next, true, nil
}

func changeAfterCanonicalBytes(change Change) (uint64, error) {
	if change.After == nil {
		return 0, ErrCorrupt
	}
	if change.afterCanonicalBytesKnown {
		return change.afterCanonicalBytes, nil
	}
	return canonicalDocumentSize(*change.After)
}

type reactiveRebuildTarget struct {
	view  *sharedReactiveView
	query QuerySpec
}

// tryFullRecomputeStorageCollection rebuilds every existing view from one
// snapshot without retaining db.mu during the scan. It succeeds only when both
// the database token and the exact collection view set are unchanged at the
// handoff. Replacing the shared order at that token makes queued older batches
// harmless (their token is already covered); later commits are admitted only
// after the new states are visible.
func (hub *reactiveHub) tryFullRecomputeStorageCollection(collection string) bool {
	if hub == nil || hub.db == nil || hub.db.querySource == nil {
		return false
	}
	db := hub.db
	hub.mu.Lock()
	group := hub.byCollection[collection]
	if hub.closed || len(group) == 0 {
		hub.mu.Unlock()
		return true
	}
	targets := make([]reactiveRebuildTarget, 0, len(group))
	for _, view := range group {
		targets = append(targets, reactiveRebuildTarget{view: view, query: view.query})
	}
	hub.mu.Unlock()

	for attempt := 0; attempt < maxColdReactiveSubscriptionAttempts; attempt++ {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			hub.failCollection(collection, ErrClosed)
			return true
		}
		token := db.token
		db.mu.RUnlock()
		queries := make([]QuerySpec, len(targets))
		for index, target := range targets {
			queries[index] = target.query
		}
		temporaryOrder := &reactiveCollectionOrder{positions: make(map[DocumentID]uint64)}
		states, err := buildStorageReactiveViewStates(db.querySource, token, collection, queries, temporaryOrder, hub.prioritySeed, db.resourceLimits)
		if err != nil {
			if errors.Is(err, ErrResourceLimit) {
				db.metrics.resourceLimitRejections.Add(1)
			}
			hub.failCollection(collection, err)
			return true
		}

		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			hub.failCollection(collection, ErrClosed)
			return true
		}
		if db.token != token {
			db.mu.RUnlock()
			continue
		}
		hub.mu.Lock()
		current := hub.byCollection[collection]
		stable := !hub.closed && len(current) == len(targets)
		if stable {
			for _, target := range targets {
				if current[target.view.key] != target.view {
					stable = false
					break
				}
			}
		}
		order := hub.orders[collection]
		if order == nil {
			stable = false
		}
		if !stable {
			hub.mu.Unlock()
			db.mu.RUnlock()
			return false
		}
		order.replace(token, temporaryOrder.next, temporaryOrder.positions)
		hub.mu.Unlock()
		type rebuiltDelivery struct {
			view     *sharedReactiveView
			previous *reactiveViewState
			next     *reactiveViewState
		}
		deliveries := make([]rebuiltDelivery, 0, len(targets))
		// Publish before releasing db.mu: a later write may then only advance
		// from this complete token, never from the state that triggered this
		// rebuild. Delivery itself is deliberately deferred until after unlock.
		for index, target := range targets {
			view, next := target.view, states[index]
			published, changed, previous := publishNewerViewState(view, next)
			if !published {
				continue
			}
			db.metrics.queryRecomputes.Add(1)
			db.metrics.fullViewRecomputes.Add(1)
			if !changed {
				continue
			}
			deliveries = append(deliveries, rebuiltDelivery{view: view, previous: previous, next: next})
		}
		db.mu.RUnlock()
		for _, delivery := range deliveries {
			delta, err := buildSharedQueryDelta(delivery.previous.snapshot.Documents, delivery.next.snapshot.Documents, delivery.next.snapshot.Token)
			if err != nil {
				hub.failView(delivery.view, err)
				continue
			}
			db.metrics.sharedDeltas.Add(1)
			hub.deliverView(delivery.view, delivery.next.snapshot, delta)
		}
		return true
	}
	return false
}

func (hub *reactiveHub) fullRecomputeCollection(collection string) {
	if hub != nil && hub.db != nil && hub.db.querySource != nil && hub.tryFullRecomputeStorageCollection(collection) {
		return
	}
	hub.fullRecomputeCollectionLocked(collection)
}

// fullRecomputeCollectionLocked retains the original lock-held rebuild as the
// conservative fallback when a storage handoff cannot prove one stable
// snapshot/current-view boundary.
func (hub *reactiveHub) fullRecomputeCollectionLocked(collection string) {
	db := hub.db
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		hub.failCollection(collection, ErrClosed)
		return
	}
	token := db.token
	order := hub.ensureOrder(collection)
	hub.mu.Lock()
	views := make([]*sharedReactiveView, 0, len(hub.byCollection[collection]))
	for _, view := range hub.byCollection[collection] {
		views = append(views, view)
	}
	hub.mu.Unlock()
	type rebuiltView struct {
		view  *sharedReactiveView
		state *reactiveViewState
	}
	rebuilt := make([]rebuiltView, 0, len(views))
	if db.querySource != nil && len(views) > 0 {
		queries := make([]QuerySpec, len(views))
		for index, view := range views {
			queries[index] = view.query
		}
		states, err := buildStorageReactiveViewStates(db.querySource, token, collection, queries, order, hub.prioritySeed, db.resourceLimits)
		if err != nil {
			if errors.Is(err, ErrResourceLimit) {
				db.metrics.resourceLimitRejections.Add(1)
			}
			db.mu.RUnlock()
			hub.failCollection(collection, err)
			return
		}
		for index, view := range views {
			rebuilt = append(rebuilt, rebuiltView{view: view, state: states[index]})
		}
	} else {
		data := db.collections[collection]
		if err := order.rebuild(data, token); err != nil {
			db.mu.RUnlock()
			hub.failCollection(collection, err)
			return
		}
		for _, view := range views {
			next, err := buildReactiveViewState(view.query, data, order, hub.prioritySeed, token, db.resourceLimits)
			if err != nil {
				if errors.Is(err, ErrResourceLimit) {
					db.metrics.resourceLimitRejections.Add(1)
				}
				db.mu.RUnlock()
				hub.failCollection(collection, err)
				return
			}
			rebuilt = append(rebuilt, rebuiltView{view: view, state: next})
		}
	}
	type rebuiltDelivery struct {
		view     *sharedReactiveView
		previous *reactiveViewState
		next     *reactiveViewState
	}
	deliveries := make([]rebuiltDelivery, 0, len(rebuilt))
	// Keep publication inside the token read-lock. A writer that follows can
	// therefore apply its batch only to this complete rebuild, while subscriber
	// delivery remains outside the lock.
	for _, item := range rebuilt {
		view, next := item.view, item.state
		published, changed, previous := publishNewerViewState(view, next)
		if !published {
			continue
		}
		db.metrics.queryRecomputes.Add(1)
		db.metrics.fullViewRecomputes.Add(1)
		if changed {
			deliveries = append(deliveries, rebuiltDelivery{view: view, previous: previous, next: next})
		}
	}
	db.mu.RUnlock()
	for _, delivery := range deliveries {
		delta, err := buildSharedQueryDelta(delivery.previous.snapshot.Documents, delivery.next.snapshot.Documents, delivery.next.snapshot.Token)
		if err != nil {
			hub.failView(delivery.view, err)
			continue
		}
		db.metrics.sharedDeltas.Add(1)
		hub.deliverView(delivery.view, delivery.next.snapshot, delta)
	}
}

func (hub *reactiveHub) deliverView(view *sharedReactiveView, snapshot QuerySnapshot, delta *sharedQueryDelta) {
	hub.mu.Lock()
	if hub.views[view.key] != view {
		hub.mu.Unlock()
		return
	}
	deliveries := subscribersOf(view)
	hub.mu.Unlock()
	for _, subscriber := range deliveries {
		hub.deliver(view.key, view, subscriber, snapshot, delta)
	}
}

func (hub *reactiveHub) deliver(key string, view *sharedReactiveView, subscriber *sharedQuerySubscriber, snapshot QuerySnapshot, delta *sharedQueryDelta) {
	switch subscriber.deliver(snapshot, delta) {
	case reactiveDeliverySkipped:
		return
	case reactiveDeliverySent:
		if subscriber.deltas != nil {
			hub.db.metrics.deltaDeliveries.Add(1)
			hub.db.metrics.deltaOperations.Add(uint64(len(delta.operations)))
		} else {
			hub.db.metrics.snapshotsEmitted.Add(1)
			hub.db.metrics.documentsEmitted.Add(uint64(len(snapshot.Documents)))
		}
		return
	case reactiveDeliveryInvalid:
		hub.failView(view, ErrCorrupt)
		return
	}
	hub.db.metrics.slowConsumers.Add(1)
	hub.detach(key, view, subscriber)
	subscriber.finish(ErrSlowConsumer)
}

func (subscriber *sharedQuerySubscriber) deliver(snapshot QuerySnapshot, delta *sharedQueryDelta) reactiveDeliveryStatus {
	subscriber.mu.Lock()
	defer subscriber.mu.Unlock()
	if subscriber.closed || snapshot.Token <= subscriber.lastToken {
		return reactiveDeliverySkipped
	}
	if subscriber.deltas != nil {
		if delta == nil || delta.token != snapshot.Token {
			return reactiveDeliveryInvalid
		}
		copy := cloneSharedQueryDelta(delta, subscriber.lastToken)
		select {
		case subscriber.deltas <- copy:
			subscriber.lastToken = snapshot.Token
			return reactiveDeliverySent
		default:
			return reactiveDeliverySlow
		}
	}
	copy := cloneSnapshot(snapshot)
	select {
	case subscriber.snapshots <- copy:
		subscriber.lastToken = snapshot.Token
		return reactiveDeliverySent
	default:
		return reactiveDeliverySlow
	}
}

func (subscriber *sharedQuerySubscriber) finish(err error) {
	subscriber.mu.Lock()
	if subscriber.closed {
		subscriber.mu.Unlock()
		return
	}
	subscriber.closed = true
	if err != nil && !errors.Is(err, context.Canceled) {
		select {
		case subscriber.errors <- err:
		default:
		}
	}
	if subscriber.snapshots != nil {
		close(subscriber.snapshots)
	}
	if subscriber.deltas != nil {
		close(subscriber.deltas)
	}
	close(subscriber.errors)
	cancel, stop := subscriber.cancel, subscriber.stop
	subscriber.cancel, subscriber.stop = nil, nil
	subscriber.mu.Unlock()
	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel()
	}
}

func (hub *reactiveHub) detach(key string, view *sharedReactiveView, subscriber *sharedQuerySubscriber) {
	if hub == nil || subscriber == nil {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	current := hub.views[key]
	if current != view || current.subscribers[subscriber.id] != subscriber {
		return
	}
	delete(current.subscribers, subscriber.id)
	hub.db.metrics.querySubscribers.Add(^uint64(0))
	if len(current.subscribers) == 0 {
		hub.deleteViewLocked(current)
	}
}

func (hub *reactiveHub) failCollection(collection string, err error) {
	hub.mu.Lock()
	views := make([]*sharedReactiveView, 0, len(hub.byCollection[collection]))
	for _, view := range hub.byCollection[collection] {
		views = append(views, view)
	}
	hub.mu.Unlock()
	for _, view := range views {
		hub.failView(view, err)
	}
}

func (hub *reactiveHub) failView(view *sharedReactiveView, err error) {
	hub.mu.Lock()
	if hub.views[view.key] != view {
		hub.mu.Unlock()
		return
	}
	subscribers := subscribersOf(view)
	if len(subscribers) > 0 {
		hub.db.metrics.querySubscribers.Add(^uint64(len(subscribers) - 1))
	}
	hub.deleteViewLocked(view)
	hub.mu.Unlock()
	for _, subscriber := range subscribers {
		subscriber.finish(err)
	}
}

func (hub *reactiveHub) deleteViewLocked(view *sharedReactiveView) {
	delete(hub.views, view.key)
	group := hub.byCollection[view.collection]
	delete(group, view.key)
	if len(group) == 0 {
		delete(hub.byCollection, view.collection)
		delete(hub.orders, view.collection)
		delete(hub.resync, view.collection)
	}
	hub.db.metrics.sharedViews.Add(^uint64(0))
}

func (hub *reactiveHub) close() {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		return
	}
	hub.closed = true
	close(hub.done)
	subscribers := make([]*sharedQuerySubscriber, 0)
	for _, view := range hub.views {
		subscribers = append(subscribers, subscribersOf(view)...)
	}
	hub.views = make(map[string]*sharedReactiveView)
	hub.byCollection = make(map[string]map[string]*sharedReactiveView)
	hub.orders = make(map[string]*reactiveCollectionOrder)
	hub.queue = nil
	hub.resync = make(map[string]struct{})
	hub.db.metrics.sharedViews.Store(0)
	hub.db.metrics.querySubscribers.Store(0)
	hub.db.metrics.pendingReactiveBatches.Store(0)
	hub.db.metrics.pendingReactiveChanges.Store(0)
	hub.db.metrics.pendingReactiveBytes.Store(0)
	hub.mu.Unlock()
	for _, subscriber := range subscribers {
		subscriber.finish(ErrClosed)
	}
}

func subscribersOf(view *sharedReactiveView) []*sharedQuerySubscriber {
	result := make([]*sharedQuerySubscriber, 0, len(view.subscribers))
	for _, subscriber := range view.subscribers {
		result = append(result, subscriber)
	}
	return result
}

func snapshotQueryBudgetedUnlocked(ctx context.Context, db *DB, collection string, query QuerySpec, budget *queryBudget) (QuerySnapshot, error) {
	data := db.collections[collection]
	if data == nil {
		return QuerySnapshot{Token: db.token}, nil
	}
	collector := newQueryCandidateCollector(query)
	for _, id := range data.order {
		if err := contextError(ctx); err != nil {
			return QuerySnapshot{}, err
		}
		document, ok := data.documents[id]
		if !ok {
			continue
		}
		if err := budget.document(); err != nil {
			return QuerySnapshot{}, err
		}
		if !query.Match(document) {
			continue
		}
		if err := retainQueryCandidate(&collector, budget, queryCandidate{document: document, position: data.positions[id]}); err != nil {
			return QuerySnapshot{}, err
		}
	}
	return QuerySnapshot{Token: db.token, Documents: collector.Documents()}, nil
}
