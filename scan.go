package miniredis

import (
	"sort"
	"strconv"
)

type scanKind int

const (
	scanKeys scanKind = iota
	scanHash
	scanSet
	scanSortedSet
)

type scanScope struct {
	kind scanKind
	key  string
}

type scanSession struct {
	entries []string
	known   map[string]struct{}
	version uint
	size    int
}

type scanCursorRef struct {
	scope  scanScope
	cursor int
}

func (db *RedisDB) initScanState() {
	if db.scanSessions == nil {
		db.scanSessions = map[scanScope]map[*scanSession]struct{}{}
	}
	if db.scanCursors == nil {
		db.scanCursors = map[scanCursorRef]*scanSession{}
	}
}

// scanPage returns one page of elements and the cursor for the next page.
//
// A cursor is also an offset in an append-only list of element names. Existing
// entries never move, so deletions cannot shift the position of unread
// elements. New names are appended either by the low-level write hooks or when
// a changed collection is reconciled once. Deleted names stay in the list and
// are skipped with a live existence check.
func (db *RedisDB) scanPage(
	scope scanScope,
	cursor int,
	count int,
	pattern string,
	withMatch bool,
	accept func(string) bool,
) (string, []string) {
	db.initScanState()

	if cursor < 0 {
		return "0", nil
	}

	var session *scanSession
	if cursor == 0 {
		session = db.newScanSession(scope)
	} else {
		session = db.scanCursors[scanCursorRef{scope: scope, cursor: cursor}]
		if session == nil {
			return "0", nil
		}
		db.reconcileScanSession(scope, session)
	}

	if count <= 0 {
		count = 10
	}

	var matcher func(string) bool
	if withMatch {
		re := patternRE(pattern)
		if re == nil {
			db.dropScanSession(scope, session)
			return "0", nil
		}
		matcher = re.MatchString
	}

	start := 0
	if cursor > 0 {
		start = cursor
	}
	if start > len(session.entries) {
		db.dropScanSession(scope, session)
		return "0", nil
	}

	capacity := count
	if remaining := len(session.entries) - start; remaining < capacity {
		capacity = remaining
	}
	elements := make([]string, 0, capacity)
	next := start
	for next < len(session.entries) && len(elements) < count {
		name := session.entries[next]
		next++

		if !db.scanExists(scope, name) {
			continue
		}
		if accept != nil && !accept(name) {
			continue
		}
		if matcher != nil && !matcher(name) {
			continue
		}
		elements = append(elements, name)
	}

	nextCursor := 0
	if next < len(session.entries) {
		nextCursor = next
	}

	if nextCursor == 0 {
		db.dropScanSession(scope, session)
	} else {
		sessions := db.scanSessions[scope]
		if sessions == nil {
			sessions = map[*scanSession]struct{}{}
			db.scanSessions[scope] = sessions
		}
		sessions[session] = struct{}{}
		db.scanCursors[scanCursorRef{scope: scope, cursor: nextCursor}] = session
	}

	return strconv.Itoa(nextCursor), elements
}

func (db *RedisDB) newScanSession(scope scanScope) *scanSession {
	names := db.scanNames(scope)
	session := &scanSession{
		entries: names,
		known:   make(map[string]struct{}, len(names)),
		version: db.scanVersion(scope),
		size:    db.scanSize(scope),
	}
	for _, name := range names {
		session.known[name] = struct{}{}
	}

	db.initScanState()
	sessions := db.scanSessions[scope]
	if sessions == nil {
		sessions = map[*scanSession]struct{}{}
		db.scanSessions[scope] = sessions
	}
	sessions[session] = struct{}{}
	return session
}

func (db *RedisDB) dropScanSession(scope scanScope, session *scanSession) {
	if sessions, ok := db.scanSessions[scope]; ok {
		delete(sessions, session)
		if len(sessions) == 0 {
			delete(db.scanSessions, scope)
		}
	}
	for ref, candidate := range db.scanCursors {
		if ref.scope == scope && candidate == session {
			delete(db.scanCursors, ref)
		}
	}
}

func (db *RedisDB) reconcileScanSession(scope scanScope, session *scanSession) {
	version := db.scanVersion(scope)
	size := db.scanSize(scope)
	if session.version == version && session.size == size {
		return
	}

	current := db.scanNames(scope)
	for _, name := range current {
		if _, ok := session.known[name]; ok {
			continue
		}
		session.known[name] = struct{}{}
		session.entries = append(session.entries, name)
	}
	session.version = version
	session.size = len(current)
}

func (db *RedisDB) scanNames(scope scanScope) []string {
	switch scope.kind {
	case scanKeys:
		return db.allKeys()
	case scanHash:
		return db.hashFields(scope.key)
	case scanSet:
		return db.setMembers(scope.key)
	case scanSortedSet:
		return db.ssetMembers(scope.key)
	default:
		return nil
	}
}

func (db *RedisDB) scanExists(scope scanScope, name string) bool {
	switch scope.kind {
	case scanKeys:
		_, ok := db.keys[name]
		return ok
	case scanHash:
		_, ok := db.hashKeys[scope.key][name]
		return ok
	case scanSet:
		_, ok := db.setKeys[scope.key][name]
		return ok
	case scanSortedSet:
		_, ok := db.sortedsetKeys[scope.key][name]
		return ok
	default:
		return false
	}
}

func (db *RedisDB) scanVersion(scope scanScope) uint {
	if scope.kind == scanKeys {
		return db.keyspaceVersion
	}
	return db.keyVersion[scope.key]
}

func (db *RedisDB) scanSize(scope scanScope) int {
	switch scope.kind {
	case scanKeys:
		return len(db.keys)
	case scanHash:
		return len(db.hashKeys[scope.key])
	case scanSet:
		return len(db.setKeys[scope.key])
	case scanSortedSet:
		return len(db.sortedsetKeys[scope.key])
	default:
		return 0
	}
}

// scanElementsAdded records new element names in active cursors without
// rebuilding and sorting the whole collection on the next page.
func (db *RedisDB) scanElementsAdded(kind scanKind, key string, names ...string) {
	scope := scanScope{kind: kind, key: key}
	version := db.scanVersion(scope)
	for session := range db.scanSessions[scope] {
		var fresh []string
		for _, name := range names {
			if _, ok := session.known[name]; ok {
				continue
			}
			session.known[name] = struct{}{}
			fresh = append(fresh, name)
		}
		sort.Strings(fresh)
		session.entries = append(session.entries, fresh...)
		session.version = version
		session.size = db.scanSize(scope)
	}
}

// scanScopeTouched marks a keyed scan version as observed after a mutation
// which does not add new element names.
func (db *RedisDB) scanScopeTouched(kind scanKind, key string) {
	scope := scanScope{kind: kind, key: key}
	version := db.scanVersion(scope)
	for session := range db.scanSessions[scope] {
		session.version = version
		session.size = db.scanSize(scope)
	}
}

func (db *RedisDB) scanKeyspaceTouched(key string) {
	db.keyspaceVersion++
	scope := scanScope{kind: scanKeys}
	version := db.keyspaceVersion
	size := db.scanSize(scope)
	for session := range db.scanSessions[scope] {
		if _, ok := session.known[key]; !ok {
			session.known[key] = struct{}{}
			session.entries = append(session.entries, key)
		}
		session.version = version
		session.size = size
	}
}

func (db *RedisDB) scanKeyspaceChanged() {
	db.keyspaceVersion++
	version := db.keyspaceVersion
	size := db.scanSize(scanScope{kind: scanKeys})
	for session := range db.scanSessions[scanScope{kind: scanKeys}] {
		session.version = version
		session.size = size
	}
}

func scanKindForKeyType(keyType string) (scanKind, bool) {
	switch keyType {
	case keyTypeHash:
		return scanHash, true
	case keyTypeSet:
		return scanSet, true
	case keyTypeSortedSet:
		return scanSortedSet, true
	default:
		return 0, false
	}
}

func (db *RedisDB) scanRenameKey(from, to string) {
	kind, ok := scanKindForKeyType(db.t(to))
	if !ok || from == to {
		return
	}

	oldScope := scanScope{kind: kind, key: from}
	newScope := scanScope{kind: kind, key: to}
	if sessions := db.scanSessions[oldScope]; len(sessions) > 0 {
		merged := db.scanSessions[newScope]
		if merged == nil {
			merged = map[*scanSession]struct{}{}
			db.scanSessions[newScope] = merged
		}
		for session := range sessions {
			merged[session] = struct{}{}
			session.version = db.scanVersion(newScope)
			session.size = db.scanSize(newScope)
		}
		delete(db.scanSessions, oldScope)
	}

	for ref, session := range db.scanCursors {
		if ref.scope != oldScope {
			continue
		}
		delete(db.scanCursors, ref)
		ref.scope = newScope
		db.scanCursors[ref] = session
	}
}
