package miniredis

import (
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
	"github.com/alicebob/miniredis/v2/server"
)

// drainEvent waits a short while for one event. Returns false on timeout.
func drainEvent(t *testing.T, s *KeyspaceSubscriber) (KeyspaceEvent, bool) {
	t.Helper()
	select {
	case <-s.Events():
	case <-time.After(time.Second):
		return KeyspaceEvent{}, false
	}
	ev, ok := s.Read()
	return ev, ok
}

// Subscribe to one key, then rewrite and delete it through the direct API.
func TestKeyspaceSetDel(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("foo")
	defer m.closeKeyspaceSubscriber(sub)

	ok(t, m.Set("foo", "bar"))
	ok(t, m.Set("foo", "baz")) // overwrite: another "set"
	equals(t, true, m.Del("foo"))

	want := []KeyspaceEvent{
		{Key: "foo", Type: KeyspaceSet},
		{Key: "foo", Type: KeyspaceSet},
		{Key: "foo", Type: KeyspaceDel},
	}
	for _, w := range want {
		ev, ok := drainEvent(t, sub)
		if !ok {
			t.Fatalf("missing event %#v", w)
		}
		equals(t, w, ev)
	}

	// No further events.
	if ev, ok := sub.Read(); ok {
		t.Fatalf("unexpected extra event: %#v", ev)
	}
}

// Deleting a missing key produces no event; only keys that actually change
// notify.
func TestKeyspaceDelMissing(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("foo")
	defer m.closeKeyspaceSubscriber(sub)

	equals(t, false, m.Del("foo"))

	select {
	case <-sub.Events():
		if ev, ok := sub.Read(); ok {
			t.Fatalf("unexpected event for missing key: %#v", ev)
		}
	case <-time.After(20 * time.Millisecond):
	}
}

// Expiry fires an "expired" event at the moment the key actually vanishes.
func TestKeyspaceExpired(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("ephemeral")
	defer m.closeKeyspaceSubscriber(sub)

	ok(t, m.Set("ephemeral", "v"))
	m.SetTTL("ephemeral", 10*time.Second)

	if ev, ok := drainEvent(t, sub); !ok || ev.Type != KeyspaceSet || ev.Key != "ephemeral" {
		t.Fatalf("want set event, got %#v ok=%v", ev, ok)
	}

	m.FastForward(11 * time.Second)

	ev, ok := drainEvent(t, sub)
	if !ok {
		t.Fatal("missing expired event")
	}
	equals(t, KeyspaceEvent{Key: "ephemeral", Type: KeyspaceExpired}, ev)

	// The key is really gone.
	equals(t, false, m.Exists("ephemeral"))
}

// Only subscribed keys are reported; others never disturb the subscriber.
func TestKeyspaceFiltering(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("mine")
	sub.Psubscribe("prefix:*")
	defer m.closeKeyspaceSubscriber(sub)

	ok(t, m.Set("other", "x"))
	ok(t, m.Set("mine", "x"))
	ok(t, m.Set("prefix:one", "x"))

	ev, ok := drainEvent(t, sub)
	if !ok || ev.Key != "mine" {
		t.Fatalf("want mine, got %#v ok=%v", ev, ok)
	}
	ev, ok = drainEvent(t, sub)
	if !ok || ev.Key != "prefix:one" {
		t.Fatalf("want prefix:one, got %#v ok=%v", ev, ok)
	}
}

// A subscriber which never reads must not block writes or lose events: all
// events stay buffered and can be drained later.
func TestKeyspaceSlowSubscriber(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("k")
	defer m.closeKeyspaceSubscriber(sub)

	// Fire many events without reading anything. Writes must complete.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 500; i++ {
			ok(t, m.Set("k", "v"))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writes blocked on a non-reading subscriber")
	}

	got := 0
	for {
		ev, ok := sub.Read()
		if !ok {
			break
		}
		if ev.Key != "k" || ev.Type != KeyspaceSet {
			t.Fatalf("unexpected event: %#v", ev)
		}
		got++
	}
	equals(t, 500, got)
}

// Subscribers bound to a client connection are unregistered automatically
// when that connection disconnects, so the registry can't grow without
// bounds.
func TestKeyspaceDisconnectCleanup(t *testing.T) {
	m := NewMiniRedis()
	ok(t, m.Start())
	defer m.Close()

	// A custom command hands us the real server-side peer for the
	// connection, which is what a real keyspace-subscribe command would use.
	ok(t, m.Server().Register("KB.SUB", func(c *server.Peer, cmd string, args []string) {
		m.KeyspaceSubscribe(c, "foo")
		c.WriteOK()
	}))

	client, serverConn := net.Pipe()
	m.Server().ServeConn(serverConn)
	pc := proto.NewClient(client)
	defer pc.Close()

	mustOK(t, pc, "KB.SUB")
	equals(t, 1, m.KeyspaceSubscriberCount())

	// The connection-scoped subscriber receives events while connected.
	ok(t, m.Set("foo", "a"))

	// Drop the client connection; the disconnect callback must remove it.
	ok(t, client.Close())

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && m.KeyspaceSubscriberCount() != 0 {
		time.Sleep(time.Millisecond)
	}
	equals(t, 0, m.KeyspaceSubscriberCount())
}

// Explicit Close unregisters a subscriber and stops delivery.
func TestKeyspaceExplicitClose(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("foo")
	equals(t, 1, m.KeyspaceSubscriberCount())

	ok(t, m.Set("foo", "a"))
	if ev, ok := drainEvent(t, sub); !ok || ev.Type != KeyspaceSet {
		t.Fatalf("want set, got %#v ok=%v", ev, ok)
	}

	m.closeKeyspaceSubscriber(sub)
	equals(t, 0, m.KeyspaceSubscriberCount())

	// Events after close are dropped rather than buffered forever.
	ok(t, m.Set("foo", "b"))
	if ev, ok := sub.Read(); ok {
		t.Fatalf("closed subscriber got event: %#v", ev)
	}
}

// SET overwrite emits exactly one "set", not a delete plus a set.
func TestKeyspaceOverwriteSingleEvent(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("k")
	defer m.closeKeyspaceSubscriber(sub)

	ok(t, m.Set("k", "1"))
	ok(t, m.Set("k", "2"))

	for i := 0; i < 2; i++ {
		ev, ok := drainEvent(t, sub)
		if !ok {
			t.Fatalf("missing event %d", i)
		}
		equals(t, KeyspaceSet, ev.Type)
	}
	select {
	case <-sub.Events():
		t.Fatal("unexpected third event on overwrite")
	case <-time.After(20 * time.Millisecond):
	}
}

// End-to-end: drive real Redis commands through a protocol client while a
// subscriber watches the key, covering set/del/expired types in order.
func TestKeyspaceViaCommands(t *testing.T) {
	m := RunT(t)

	c, err := proto.Dial(m.Addr())
	ok(t, err)
	defer c.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("watched")
	defer m.closeKeyspaceSubscriber(sub)

	mustOK(t, c, "SET", "watched", "one")
	mustOK(t, c, "SET", "watched", "two")
	must1(t, c, "DEL", "watched")

	mustOK(t, c, "SET", "watched", "three", "PX", "100")
	m.FastForward(200 * time.Millisecond)

	want := []string{
		KeyspaceSet, KeyspaceSet, KeyspaceDel,
		KeyspaceSet, KeyspaceExpired,
	}
	for _, typ := range want {
		ev, ok := drainEvent(t, sub)
		if !ok {
			t.Fatalf("missing %s event", typ)
		}
		equals(t, "watched", ev.Key)
		equals(t, typ, ev.Type)
	}
}

// HSETNX creating a brand new hash emits a "set" event.
func TestKeyspaceHsetnxCreate(t *testing.T) {
	m := NewMiniRedis()
	defer m.Close()

	sub := m.NewKeyspaceSubscriber()
	sub.Subscribe("h")
	defer m.closeKeyspaceSubscriber(sub)

	db := m.DB(0)
	db.hashSet("h", "f", "v") // create via hashSet
	if ev, ok := drainEvent(t, sub); !ok || ev.Type != KeyspaceSet {
		t.Fatalf("want set from hash create, got %#v ok=%v", ev, ok)
	}

	// A second field on the existing hash must not re-emit a create.
	db.hashSet("h", "g", "w")
	select {
	case <-sub.Events():
		if ev, ok := sub.Read(); ok {
			t.Fatalf("unexpected event on field add: %#v", ev)
		}
	case <-time.After(20 * time.Millisecond):
	}
}
