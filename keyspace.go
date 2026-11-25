package miniredis

import (
	"regexp"
	"sync"

	"github.com/alicebob/miniredis/v2/server"
)

// Keyspace event types, mirroring the Redis keyspace-notification names.
const (
	// KeyspaceSet is emitted when a key is created or overwritten ("set").
	KeyspaceSet = "set"
	// KeyspaceDel is emitted when a key is explicitly deleted ("del").
	KeyspaceDel = "del"
	// KeyspaceExpired is emitted when a key disappears because its TTL ran
	// out ("expired").
	KeyspaceExpired = "expired"
)

// KeyspaceEvent is a single keyspace notification. Key is the key the event
// happened to, Type is one of KeyspaceSet, KeyspaceDel or KeyspaceExpired.
type KeyspaceEvent struct {
	Key  string
	Type string
}

// KeyspaceSubscriber receives keyspace events for the keys (or key
// patterns) it is subscribed to.
//
// Events are queued per subscriber in an unbounded buffer, so a slow
// consumer never blocks the command that produced the event, and events are
// never dropped because the reader is behind. Read events with Read() or
// Events(), and release the subscription with Close().
type KeyspaceSubscriber struct {
	mu       sync.Mutex
	keys     map[string]struct{}
	patterns map[string]*regexp.Regexp
	queue    []KeyspaceEvent
	notify   chan struct{}
	closed   bool
}

func newKeyspaceSubscriber() *KeyspaceSubscriber {
	return &KeyspaceSubscriber{
		keys:     map[string]struct{}{},
		patterns: map[string]*regexp.Regexp{},
		notify:   make(chan struct{}, 1),
	}
}

// Close unsubscribes from everything and removes the subscriber. Events
// already buffered stay readable until drained; Read() on an empty, closed
// subscriber returns false.
func (s *KeyspaceSubscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.keys = map[string]struct{}{}
	s.patterns = map[string]*regexp.Regexp{}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Subscribe to keyspace events for the given exact key.
func (s *KeyspaceSubscriber) Subscribe(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[key] = struct{}{}
}

// Unsubscribe removes an exact-key subscription.
func (s *KeyspaceSubscriber) Unsubscribe(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, key)
}

// Psubscribe subscribes to keyspace events for keys matching the glob
// pattern ("foo*", "f??", ...).
func (s *KeyspaceSubscriber) Psubscribe(pattern string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.patterns[pattern] = patternRE(pattern)
}

// Punsubscribe removes a pattern subscription.
func (s *KeyspaceSubscriber) Punsubscribe(pattern string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.patterns, pattern)
}

// Events returns a channel which receives a signal whenever a new event is
// available. The channel only signals availability; consume the actual
// events with Read(). It never blocks the event producer.
func (s *KeyspaceSubscriber) Events() <-chan struct{} {
	return s.notify
}

// Read returns the next queued event. ok is false only when the subscriber
// is closed and no buffered events remain.
func (s *KeyspaceSubscriber) Read() (event KeyspaceEvent, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return KeyspaceEvent{}, false
	}
	event = s.queue[0]
	s.queue = s.queue[1:]
	if len(s.queue) > 0 && !s.closed {
		// Keep a pending signal so the next Read has something to wait on.
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
	return event, true
}

// matches reports whether the subscriber wants events for the key.
// Caller holds s.mu.
func (s *KeyspaceSubscriber) matches(key string) bool {
	if _, ok := s.keys[key]; ok {
		return true
	}
	for _, re := range s.patterns {
		if re != nil && re.MatchString(key) {
			return true
		}
	}
	return false
}

// push appends an event when the key matches. The unbounded queue means this
// never blocks on a slow reader. Caller holds the Miniredis lock.
func (s *KeyspaceSubscriber) push(event KeyspaceEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.matches(event.Key) {
		return
	}
	s.queue = append(s.queue, event)
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// keyspaceSubs is the registry of all live keyspace subscribers.
type keyspaceSubs struct {
	mu   sync.Mutex
	subs map[*KeyspaceSubscriber]struct{}
}

func newKeyspaceSubs() *keyspaceSubs {
	return &keyspaceSubs{subs: map[*KeyspaceSubscriber]struct{}{}}
}

func (k *keyspaceSubs) add(s *KeyspaceSubscriber) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.subs[s] = struct{}{}
}

// remove drops a subscriber. It is safe to call more than once, including
// from a connection-disconnect callback, so subscriptions never pile up.
func (k *keyspaceSubs) remove(s *KeyspaceSubscriber) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.subs, s)
}

func (k *keyspaceSubs) len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.subs)
}

// closeAll unregisters and closes every subscriber. Used on shutdown.
func (k *keyspaceSubs) closeAll() {
	k.mu.Lock()
	subs := make([]*KeyspaceSubscriber, 0, len(k.subs))
	for s := range k.subs {
		subs = append(subs, s)
	}
	k.subs = map[*KeyspaceSubscriber]struct{}{}
	k.mu.Unlock()

	for _, s := range subs {
		s.Close()
	}
}

// publish fans an event out to every matching subscriber. Each subscriber
// buffers independently, so one slow reader cannot stall this call or any
// write.
func (k *keyspaceSubs) publish(event KeyspaceEvent) {
	k.mu.Lock()
	subs := make([]*KeyspaceSubscriber, 0, len(k.subs))
	for s := range k.subs {
		subs = append(subs, s)
	}
	k.mu.Unlock()
	for _, s := range subs {
		s.push(event)
	}
}

// NewKeyspaceSubscriber creates a keyspace subscriber and registers it.
// Close() it when done; subscribers created on a client connection are also
// removed automatically when that connection disconnects.
func (m *Miniredis) NewKeyspaceSubscriber() *KeyspaceSubscriber {
	s := newKeyspaceSubscriber()
	m.keyspace.add(s)
	return s
}

// KeyspaceSubscribe creates a keyspace subscriber bound to the given client
// connection and subscribes it to the given exact keys. When the connection
// disconnects the subscription is unregistered automatically, so it can't
// pile up. The returned subscriber can also be released early with Close().
func (m *Miniredis) KeyspaceSubscribe(c *server.Peer, keys ...string) *KeyspaceSubscriber {
	s := m.NewKeyspaceSubscriber()
	for _, key := range keys {
		s.Subscribe(key)
	}

	ctx := getCtx(c)
	ctx.keyspaceSubs = append(ctx.keyspaceSubs, s)
	c.OnDisconnect(func() {
		m.Lock()
		m.closeKeyspaceSubscriber(s)
		m.Unlock()
	})

	return s
}

// KeyspacePsubscribe is the glob-pattern variant of KeyspaceSubscribe.
func (m *Miniredis) KeyspacePsubscribe(c *server.Peer, patterns ...string) *KeyspaceSubscriber {
	s := m.NewKeyspaceSubscriber()
	for _, pattern := range patterns {
		s.Psubscribe(pattern)
	}

	ctx := getCtx(c)
	ctx.keyspaceSubs = append(ctx.keyspaceSubs, s)
	c.OnDisconnect(func() {
		m.Lock()
		m.closeKeyspaceSubscriber(s)
		m.Unlock()
	})

	return s
}

// closeKeyspaceSubscriber unregisters and closes a subscriber.
func (m *Miniredis) closeKeyspaceSubscriber(s *KeyspaceSubscriber) {
	m.keyspace.remove(s)
	s.Close()
}

// notifyKeyspace emits a keyspace event. Caller holds the Miniredis lock.
func (m *Miniredis) notifyKeyspace(key, typ string) {
	m.keyspace.publish(KeyspaceEvent{Key: key, Type: typ})
}

// KeyspaceSubscriberCount returns the number of registered keyspace
// subscribers, mostly useful in tests.
func (m *Miniredis) KeyspaceSubscriberCount() int {
	return m.keyspace.len()
}
