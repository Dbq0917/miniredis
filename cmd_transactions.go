// Commands from https://redis.io/commands#transactions

package miniredis

import (
	"github.com/alicebob/miniredis/v2/server"
)

// commandsTransaction handles MULTI &c.
func commandsTransaction(m *Miniredis) {
	m.srv.Register("DISCARD", m.cmdDiscard)
	m.srv.Register("EXEC", m.cmdExec)
	m.srv.Register("MULTI", m.cmdMulti)
	m.srv.Register("UNWATCH", m.cmdUnwatch)
	m.srv.Register("WATCH", m.cmdWatch)
}

// MULTI
func (m *Miniredis) cmdMulti(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, exactly(0)) {
		return
	}

	ctx := getCtx(c)
	if ctx.nested {
		c.WriteError(msgNotFromScripts(ctx.nestedSHA))
		return
	}
	if inTx(ctx) {
		c.WriteError("ERR MULTI calls can not be nested")
		return
	}

	startTx(ctx)

	c.WriteOK()
}

// EXEC
func (m *Miniredis) cmdExec(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, exactly(0)) {
		return
	}

	ctx := getCtx(c)
	if ctx.nested {
		c.WriteError(msgNotFromScripts(ctx.nestedSHA))
		return
	}
	if !inTx(ctx) {
		c.WriteError("ERR EXEC without MULTI")
		return
	}

	if ctx.dirtyTransaction {
		c.WriteError("EXECABORT Transaction discarded because of previous errors.")
		// a failed EXEC finishes the tx
		stopTx(ctx)
		return
	}

	txCmds := ctx.transaction
	requests := ctx.txRequests
	batchModes := transactionKeyModes(requests, ctx.selectedDB, ctx.watch)
	releaseBatch := m.acquireKeyModes(batchModes)
	defer releaseBatch()
	defer stopTx(ctx)

	// Check WATCHed keys.
	m.Lock()
	for t, version := range ctx.watch {
		if m.db(t.db).keyVersion[t.key] > version {
			m.Unlock()
			// Abort! Abort!
			c.WriteNullArray()
			return
		}
	}
	m.Unlock()

	c.WriteLen(len(txCmds))
	for i, cb := range txCmds {
		sharedGlobal := false
		if i < len(requests) {
			sharedGlobal = commandUsesSharedGlobalLock(requests[i].cmd, requests[i].readOnly)
		}
		m.withCommandGlobalLock(sharedGlobal, func() {
			cb(c, ctx)
		})
	}
}

// DISCARD
func (m *Miniredis) cmdDiscard(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, exactly(0)) {
		return
	}

	ctx := getCtx(c)
	if !inTx(ctx) {
		c.WriteError("ERR DISCARD without MULTI")
		return
	}

	stopTx(ctx)
	c.WriteOK()
}

// WATCH
func (m *Miniredis) cmdWatch(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(1)) {
		return
	}

	ctx := getCtx(c)
	if ctx.nested {
		c.WriteError(msgNotFromScripts(ctx.nestedSHA))
		return
	}
	if inTx(ctx) {
		c.WriteError("ERR WATCH in MULTI")
		return
	}

	m.Lock()
	defer m.Unlock()
	db := m.db(ctx.selectedDB)

	for _, key := range args {
		watch(db, ctx, key)
	}
	c.WriteOK()
}

// UNWATCH
func (m *Miniredis) cmdUnwatch(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, exactly(0)) {
		return
	}

	// Doesn't matter if UNWATCH is in a TX or not. Looks like a Redis bug to me.
	unwatch(getCtx(c))

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		// Do nothing if it's called in a transaction.
		c.WriteOK()
	})
}
