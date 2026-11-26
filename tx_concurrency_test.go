package miniredis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
	"github.com/alicebob/miniredis/v2/server"
)

func TestTransactionDoesNotBlockUnrelatedRead(t *testing.T) {
	s, c := runWithClient(t)
	addr := s.Addr()

	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	err := s.Server().Register("FEEDBACK_SLOW_READ", func(c *server.Peer, cmd string, args []string) {
		if !s.isValidCMD(c, cmd, args, exactly(1)) {
			return
		}
		withTx(s, c, func(c *server.Peer, ctx *connCtx) {
			close(entered)
			<-release
			c.WriteOK()
		})
	}, server.ReadOnlyOption())
	if err != nil {
		t.Fatal(err)
	}

	mustOK(t, c, "MULTI")
	mustDo(t, c, "SET", "locked-key", "value", proto.Inline("QUEUED"))
	mustDo(t, c, "FEEDBACK_SLOW_READ", "locked-key", proto.Inline("QUEUED"))

	execDone := make(chan error, 1)
	go func() {
		_, execErr := c.Do("EXEC")
		execDone <- execErr
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("transaction command did not start")
	}

	unrelated, err := proto.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer unrelated.Close()

	unrelatedDone := make(chan string, 1)
	go func() {
		res, _ := unrelated.Do("GET", "other-key")
		unrelatedDone <- res
	}()

	select {
	case res := <-unrelatedDone:
		if res != proto.Nil {
			t.Fatalf("unexpected response for unrelated key: %q", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated GET was blocked for the whole transaction")
	}

	sameKey, err := proto.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer sameKey.Close()

	sameKeyDone := make(chan string, 1)
	go func() {
		res, _ := sameKey.Do("GET", "locked-key")
		sameKeyDone <- res
	}()

	select {
	case <-sameKeyDone:
		t.Fatal("GET for a transaction write key observed uncommitted state")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case execErr := <-execDone:
		if execErr != nil {
			t.Fatal(execErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EXEC did not finish after releasing the slow command")
	}

	select {
	case res := <-sameKeyDone:
		if want := proto.String("value"); res != want {
			t.Fatalf("same-key GET = %q, want %q", res, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-key GET remained blocked after EXEC completed")
	}
}

func TestTxWatchABAVersion(t *testing.T) {
	s, c := runWithClient(t)
	c2, err := proto.Dial(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	mustOK(t, c, "SET", "aba", "one")
	mustOK(t, c, "WATCH", "aba")
	mustOK(t, c2, "SET", "aba", "two")
	mustOK(t, c2, "SET", "aba", "one")

	mustOK(t, c, "MULTI")
	mustDo(t, c, "PING", proto.Inline("QUEUED"))
	mustNilList(t, c, "EXEC")
}
