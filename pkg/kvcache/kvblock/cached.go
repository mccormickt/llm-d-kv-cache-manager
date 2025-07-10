/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvblock

import (
	"context"

	"k8s.io/apimachinery/pkg/util/sets"
)

// CachedIndex is tiered Index comprised of two Indexes, with one acting as a cache.
type CachedIndex struct {
	cache Index
	index Index
}

// NewCachedIndex creates a new Index that returns cached data backed by another Index.
func NewCachedIndex(cache Index, index Index) Index {
	return &CachedIndex{
		cache: cache,
		index: index,
	}
}

// Add implements the Index interface
func (r *CachedIndex) Add(ctx context.Context, keys []Key, podIdentifiers []PodEntry) error {
	if len(keys) == 0 {
		return nil
	}

	// Add to cache first
	err := r.cache.Add(ctx, keys, podIdentifiers)
	if err != nil {
		return err
	}

	// Add to backing index
	return r.index.Add(ctx, keys, podIdentifiers)
}

// Evict removes entries from cache first, then from backing index
func (r *CachedIndex) Evict(ctx context.Context, key Key, entries []PodEntry) error {
	err := r.cache.Evict(ctx, key, entries)
	if err != nil {
		return err
	}
	return r.index.Evict(ctx, key, entries)
}

// Lookup implements the Index interface
func (r *CachedIndex) Lookup(ctx context.Context, keys []Key,
	podIdentifierSet sets.Set[string],
) ([]Key, map[Key][]string, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	// Lookup in cache first
	hitKeys, hitPods, err := r.cache.Lookup(ctx, keys, podIdentifierSet)
	if err != nil {
		return nil, nil, err
	}

	// If all keys are hit, return
	if len(hitKeys) == len(keys) {
		return hitKeys, hitPods, nil
	}

	// Lookup remaining keys in backing index
	missKeys := sets.New[Key](keys...).Difference(sets.New[Key](hitKeys...)).UnsortedList()
	missKeys, missPods, err := r.index.Lookup(ctx, missKeys, podIdentifierSet)
	if err != nil {
		return nil, nil, err
	}

	// Merge results
	hitKeys = append(hitKeys, missKeys...)
	hitPods = mergePods(hitPods, missPods)

	return hitKeys, hitPods, nil
}

func mergePods(hitPods, missPods map[Key][]string) map[Key][]string {
	for key, pods := range missPods {
		hitPods[key] = append(hitPods[key], pods...)
	}
	return hitPods
}
