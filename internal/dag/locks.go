package dag

import (
	"path/filepath"
	"sort"
	"sync"
)

// ResourceLock provides cooperative locking for file paths and workspace resources.
// Deadlocks across tasks are prevented by acquiring all target paths in sorted lexicographical order.
type ResourceLock struct {
	locks sync.Map // map[string]*sync.Mutex
}

// NewResourceLock creates a new ResourceLock instance.
func NewResourceLock() *ResourceLock {
	return &ResourceLock{}
}

func (rl *ResourceLock) getMutex(path string) *sync.Mutex {
	clean := filepath.Clean(path)
	val, _ := rl.locks.LoadOrStore(clean, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func normalizePaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(paths))
	unique := make([]string, 0, len(paths))
	for _, p := range paths {
		cleaned := filepath.Clean(p)
		if !seen[cleaned] {
			seen[cleaned] = true
			unique = append(unique, cleaned)
		}
	}
	sort.Strings(unique)
	return unique
}

// LockPaths acquires cooperative locks on all specified paths in sorted order.
// It returns an idempotent unlock function that releases the acquired locks in reverse order.
func (rl *ResourceLock) LockPaths(paths []string) func() {
	sorted := normalizePaths(paths)
	if len(sorted) == 0 {
		return func() {}
	}

	mutexes := make([]*sync.Mutex, len(sorted))
	for i, p := range sorted {
		mtx := rl.getMutex(p)
		mtx.Lock()
		mutexes[i] = mtx
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			for i := len(mutexes) - 1; i >= 0; i-- {
				mutexes[i].Unlock()
			}
		})
	}
}

// UnlockPaths unlocks the specified paths in reverse order.
func (rl *ResourceLock) UnlockPaths(paths []string) {
	sorted := normalizePaths(paths)
	for i := len(sorted) - 1; i >= 0; i-- {
		mtx := rl.getMutex(sorted[i])
		mtx.Unlock()
	}
}
