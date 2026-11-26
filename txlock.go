package miniredis

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

type keyLock struct {
	mu   sync.RWMutex
	refs int
}

type keyLockManager struct {
	mu    sync.Mutex
	locks map[dbKey]*keyLock
}

func newKeyLockManager() *keyLockManager {
	return &keyLockManager{locks: map[dbKey]*keyLock{}}
}

// Lock keeps the historical coarse-lock API working. It takes the write side
// of the shared state lock.
func (m *Miniredis) Lock() { m.Mutex.Lock() }

func (m *Miniredis) Unlock() { m.Mutex.Unlock() }

func (m *Miniredis) RLock() { m.Mutex.RLock() }

func (m *Miniredis) RUnlock() { m.Mutex.RUnlock() }

// acquire locks all keys in a stable order. Write locks are used for
// transactions and mutating commands; ordinary read-only commands may share
// the key lock. The returned function must be called exactly once.
func (klm *keyLockManager) acquire(keys []dbKey, write bool) func() {
	modes := make(map[dbKey]bool, len(keys))
	for _, key := range keys {
		modes[key] = write
	}
	return klm.acquireModes(modes)
}

// acquireModes locks each key in read or write mode. The map value is true for
// an exclusive write lock.
func (klm *keyLockManager) acquireModes(modes map[dbKey]bool) func() {
	if klm == nil || len(modes) == 0 {
		return func() {}
	}

	unique := make([]dbKey, 0, len(modes))
	for key := range modes {
		unique = append(unique, key)
	}
	sort.Slice(unique, func(i, j int) bool {
		if unique[i].db != unique[j].db {
			return unique[i].db < unique[j].db
		}
		return unique[i].key < unique[j].key
	})

	klm.mu.Lock()
	locks := make([]*keyLock, len(unique))
	writeModes := make([]bool, len(unique))
	for i, key := range unique {
		lock, ok := klm.locks[key]
		if !ok {
			lock = &keyLock{}
			klm.locks[key] = lock
		}
		lock.refs++
		locks[i] = lock
		writeModes[i] = modes[key]
	}
	klm.mu.Unlock()

	for i, lock := range locks {
		if writeModes[i] {
			lock.mu.Lock()
		} else {
			lock.mu.RLock()
		}
	}

	released := false
	return func() {
		if released {
			return
		}
		released = true

		for i := len(locks) - 1; i >= 0; i-- {
			lock := locks[i]
			if writeModes[i] {
				lock.mu.Unlock()
			} else {
				lock.mu.RUnlock()
			}

			key := unique[i]
			klm.mu.Lock()
			lock.refs--
			if lock.refs <= 0 {
				delete(klm.locks, key)
			}
			klm.mu.Unlock()
		}
	}
}

func (m *Miniredis) acquireKeys(keys []dbKey, write bool) func() {
	if m == nil {
		return func() {}
	}
	if m.keyLocks == nil {
		m.keyLocks = newKeyLockManager()
	}
	return m.keyLocks.acquire(keys, write)
}

func (m *Miniredis) acquireKeyModes(modes map[dbKey]bool) func() {
	if m == nil {
		return func() {}
	}
	if m.keyLocks == nil {
		m.keyLocks = newKeyLockManager()
	}
	return m.keyLocks.acquireModes(modes)
}

// withCommandGlobalLock takes the coarse state lock for one command callback.
// Read-only callbacks can run concurrently unless clock TTL bookkeeping needs
// to mutate every database.
func (m *Miniredis) withCommandGlobalLock(readOnly bool, fn func()) {
	writeLocked := true
	if readOnly {
		m.RLock()
		if m.clockTTL {
			m.RUnlock()
			m.Lock()
		} else {
			writeLocked = false
		}
	} else {
		m.Lock()
	}

	defer func() {
		if writeLocked {
			m.Unlock()
		} else {
			m.RUnlock()
		}
	}()

	fn()
	if writeLocked {
		m.signal.Broadcast()
	}
}

// commandUsesSharedGlobalLock filters read-only commands whose miniredis
// implementation still mutates bookkeeping state under the coarse lock.
func commandUsesSharedGlobalLock(cmd string, readOnly bool) bool {
	if !readOnly {
		return false
	}
	switch cmd {
	case "EVAL_RO", "EVALSHA_RO":
		return false
	default:
		return true
	}
}

func setCurrentCommand(ctx *connCtx, m *Miniredis, cmd string, args []string) {
	upper := strings.ToUpper(cmd)
	ctx.currentCmd = upper
	ctx.currentArgs = append([]string(nil), args...)
	ctx.currentKeys = commandKeys(upper, args, ctx.selectedDB)
	ctx.currentReadOnly = false
	if m.srv != nil {
		ctx.currentReadOnly = m.srv.IsReadOnlyCommand(upper)
	}
}

// transactionKeyModes reconstructs the key set using the database selected at
// the point where each queued command runs.
func transactionKeyModes(requests []txRequest, currentDB int, watched map[dbKey]uint) map[dbKey]bool {
	db := currentDB
	modes := map[dbKey]bool{}
	for _, req := range requests {
		if req.cmd == "SELECT" && len(req.args) == 1 {
			if id, err := strconv.Atoi(req.args[0]); err == nil && id >= 0 {
				db = id
			}
		}
		write := !req.readOnly
		for _, key := range commandKeys(req.cmd, req.args, db) {
			if write {
				modes[key] = true
			} else if _, ok := modes[key]; !ok {
				modes[key] = false
			}
		}
	}
	for key := range watched {
		if _, ok := modes[key]; !ok {
			modes[key] = false
		}
	}
	return modes
}

func indexedKeys(db int, args []string, indexes ...int) []dbKey {
	var keys []dbKey
	for _, index := range indexes {
		if index >= 0 && index < len(args) {
			keys = append(keys, dbKey{db: db, key: args[index]})
		}
	}
	return keys
}

func allArgKeys(db int, args []string) []dbKey {
	return dbKeys(db, args...)
}

func dbKeys(db int, keys ...string) []dbKey {
	res := make([]dbKey, 0, len(keys))
	for _, key := range keys {
		res = append(res, dbKey{db: db, key: key})
	}
	return res
}

func stepKeys(db int, args []string, start, step int) []dbKey {
	var keys []dbKey
	for i := start; i < len(args); i += step {
		keys = append(keys, dbKey{db: db, key: args[i]})
	}
	return keys
}

func numKeysBlock(db int, args []string, numIndex, firstKey int) []dbKey {
	if numIndex < 0 || numIndex >= len(args) {
		return nil
	}
	n, err := strconv.Atoi(args[numIndex])
	if err != nil || n < 0 || firstKey < 0 {
		return nil
	}
	end := firstKey + n
	if end > len(args) {
		end = len(args)
	}
	var keys []dbKey
	for i := firstKey; i < end; i++ {
		keys = append(keys, dbKey{db: db, key: args[i]})
	}
	return keys
}

func commandKeys(cmd string, args []string, db int) []dbKey {
	switch cmd {
	case "WATCH":
		return allArgKeys(db, args)

	case "MULTI", "EXEC", "DISCARD", "UNWATCH", "PING", "ECHO", "AUTH", "HELLO",
		"SELECT", "SWAPDB", "DBSIZE", "TIME", "COMMAND", "INFO", "CLIENT",
		"CONFIG", "SCRIPT", "PUBLISH", "SPUBLISH", "SUBSCRIBE", "UNSUBSCRIBE",
		"PSUBSCRIBE", "PUNSUBSCRIBE", "SSUBSCRIBE", "SUNSUBSCRIBE", "QUIT",
		"SHUTDOWN", "SAVE", "BGSAVE", "BGREWRITEAOF", "LASTSAVE", "MONITOR",
		"RESET", "ROLE", "WAIT", "FAILOVER", "MIGRATE":
		return nil

	case "DEL", "UNLINK", "EXISTS", "TOUCH", "MGET", "SINTER", "SUNION", "SDIFF",
		"PFCOUNT":
		return allArgKeys(db, args)

	case "MSET", "MSETNX":
		return stepKeys(db, args, 0, 2)

	case "RENAME", "RENAMENX", "COPY", "SMOVE", "RPOPLPUSH", "BRPOPLPUSH",
		"LMOVE", "BLMOVE":
		keys := indexedKeys(db, args, 0, 1)
		if cmd == "COPY" {
			targetDB := copyTargetDB(args, db)
			if len(keys) == 2 {
				keys[1].db = targetDB
			}
		}
		return keys

	case "MOVE":
		keys := indexedKeys(db, args, 0)
		if target, ok := moveTargetDB(args); ok && len(keys) == 1 {
			keys = append(keys, dbKey{db: target, key: args[0]})
		}
		return keys

	case "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX":
		if len(args) == 0 {
			return nil
		}
		return allArgKeys(db, args[:len(args)-1])

	case "LMPOP", "ZMPOP":
		return numKeysBlock(db, args, 0, 1)
	case "BLMPOP", "BZMPOP":
		return numKeysBlock(db, args, 1, 2)

	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO":
		return numKeysBlock(db, args, 1, 2)

	case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
		keys := indexedKeys(db, args, 0)
		return append(keys, numKeysBlock(db, args, 1, 2)...)

	case "ZUNION", "ZINTER", "ZINTERCARD", "ZDIFF":
		return numKeysBlock(db, args, 0, 1)

	case "BITOP":
		if len(args) < 2 {
			return nil
		}
		return allArgKeys(db, args[1:])

	case "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE", "PFMERGE":
		return allArgKeys(db, args)

	case "OBJECT":
		return indexedKeys(db, args, 1)
	case "MEMORY":
		if len(args) >= 2 && strings.EqualFold(args[0], "USAGE") {
			return indexedKeys(db, args, 1)
		}
		return nil

	case "XREAD", "XREADGROUP":
		return streamReadKeys(db, args)

	case "KEYS", "SCAN", "FLUSHDB", "FLUSHALL":
		// These touch the keyspace rather than one named key. The coarse
		// command lock still makes their execution safe.
		return nil

	default:
		if len(args) == 0 {
			return nil
		}
		return indexedKeys(db, args, 0)
	}
}

func copyTargetDB(args []string, current int) int {
	for i := 0; i < len(args)-1; i++ {
		if strings.EqualFold(args[i], "DB") {
			if id, err := strconv.Atoi(args[i+1]); err == nil && id >= 0 {
				return id
			}
		}
	}
	return current
}

func moveTargetDB(args []string) (int, bool) {
	if len(args) != 2 {
		return 0, false
	}
	id, err := strconv.Atoi(args[1])
	if err != nil || id < 0 {
		return 0, false
	}
	return id, true
}

func streamReadKeys(db int, args []string) []dbKey {
	streams := -1
	for i, arg := range args {
		if strings.EqualFold(arg, "STREAMS") {
			streams = i
			break
		}
	}
	if streams < 0 || streams+1 >= len(args) {
		return nil
	}
	rest := args[streams+1:]
	n := len(rest) / 2
	var keys []dbKey
	for i := 0; i < n; i++ {
		keys = append(keys, dbKey{db: db, key: rest[i]})
	}
	return keys
}
