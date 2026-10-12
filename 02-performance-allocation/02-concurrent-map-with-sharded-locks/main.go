package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
)

type shard struct {
	mu sync.RWMutex
	m  map[uint64]uint64
}

type ShardedMap struct {
	shards []shard
}

func NewShardedMap(shardCount int) *ShardedMap {
	if shardCount <= 0 {
		panic("shard count must be greater than zero")
	}

	sm := &ShardedMap{
		shards: make([]shard, shardCount),
	}

	for i := range sm.shards {
		sm.shards[i].m = make(map[uint64]uint64)
	}

	return sm
}

func hashUint64(key uint64) uint64 {
	hasher := fnv.New64a()

	var buf [8]byte

	binary.LittleEndian.PutUint64(buf[:], key)

	_, _ = hasher.Write(buf[:])

	return hasher.Sum64()
}

func (sm *ShardedMap) getShard(key uint64) *shard {
	hash := hashUint64(key)

	index := hash % uint64(len(sm.shards))

	return &sm.shards[index]
}

func (sm *ShardedMap) Get(key uint64) (uint64, bool) {
	s := sm.getShard(key)

	s.mu.RLock()
	value, ok := s.m[key]
	s.mu.RUnlock()

	return value, ok
}

func (sm *ShardedMap) Set(key uint64, value uint64) {
	s := sm.getShard(key)

	s.mu.Lock()
	s.m[key] = value
	s.mu.Unlock()
}

func (sm *ShardedMap) Delete(key uint64) {
	s := sm.getShard(key)

	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

func (sm *ShardedMap) Keys() []uint64 {
	keys := make([]uint64, 0)

	for i := range sm.shards {
		s := &sm.shards[i]

		s.mu.RLock()

		for key := range s.m {
			keys = append(keys, key)
		}

		s.mu.RUnlock()
	}

	return keys
}

func (sm *ShardedMap) Increment(key uint64) uint64 {
	s := sm.getShard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[key]++

	return s.m[key]
}

func main() {
	requestCounts := NewShardedMap(64)

	var wg sync.WaitGroup

	for userID := uint64(1); userID <= 8; userID++ {
		wg.Add(1)

		go func(id uint64) {
			defer wg.Done()

			for i := 0; i < 10_000; i++ {
				requestCounts.Increment(id)
			}
		}(userID)
	}

	wg.Wait()

	for userID := uint64(1); userID <= 8; userID++ {
		count, ok := requestCounts.Get(userID)

		if ok {
			fmt.Printf(
				"user=%d requests=%d\n",
				userID,
				count,
			)
		}
	}

	fmt.Println()

	fmt.Println("All keys:", requestCounts.Keys())

	requestCounts.Set(100, 500)

	count, ok := requestCounts.Get(100)

	fmt.Println("user 100:", count, ok)

	requestCounts.Delete(100)

	_, ok = requestCounts.Get(100)

	fmt.Println("user 100 exists:", ok)
}
