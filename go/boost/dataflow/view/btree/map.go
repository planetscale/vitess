// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
package btree

import (
	"slices"
)

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

type prefixable[T ordered] interface {
	HasPrefix(s T) bool
}

func degreeToMinMax(deg int) (min, max int) {
	if deg <= 0 {
		deg = 32
	} else if deg == 1 {
		deg = 2 // must have at least 2
	}
	max = deg*2 - 1 // max items per node. max children is +1
	min = max / 2
	return min, max
}

type mapPair[K ordered, V any] struct {
	// The `value` field should be before the `key` field because doing so
	// allows for the Go compiler to optimize away the `value` field when
	// it's a `struct{}`, which is the case for `btree.Set`.
	value V
	key   K
}

type Map[K ordered, V any] struct {
	root  *mapNode[K, V]
	count int
	empty mapPair[K, V]
	min   int // min items
	max   int // max items
}

func NewMap[K ordered, V any](degree int) *Map[K, V] {
	m := new(Map[K, V])
	m.init(degree)
	return m
}

type mapNode[K ordered, V any] struct {
	count    int
	items    []mapPair[K, V]
	children *[]*mapNode[K, V]
}

func (tr *Map[K, V]) newNode(leaf bool) *mapNode[K, V] {
	n := new(mapNode[K, V])
	if !leaf {
		n.children = new([]*mapNode[K, V])
	}
	return n
}

// leaf returns true if the node is a leaf.
func (n *mapNode[K, V]) leaf() bool {
	return n.children == nil
}

func (tr *Map[K, V]) search(n *mapNode[K, V], key K) (index int, found bool) {
	low, high := 0, len(n.items)

	for low < high {
		h := (low + high) / 2
		if !(key < n.items[h].key) {
			low = h + 1
		} else {
			high = h
		}
	}
	if low > 0 && !(n.items[low-1].key < key) {
		return low - 1, true
	}
	return low, false
}

func (tr *Map[K, V]) init(degree int) {
	if tr.min != 0 {
		return
	}
	tr.min, tr.max = degreeToMinMax(degree)
}

// Set or replace a value for a key
func (tr *Map[K, V]) Set(key K, value V) (V, bool) {
	item := mapPair[K, V]{key: key, value: value}
	if tr.root == nil {
		tr.init(0)
		tr.root = tr.newNode(true)
		tr.root.items = append([]mapPair[K, V]{}, item)
		tr.root.count = 1
		tr.count = 1
		return tr.empty.value, false
	}
	prev, replaced, split := tr.nodeSet(&tr.root, item)
	if split {
		left := tr.root
		right, median := tr.nodeSplit(left)
		tr.root = tr.newNode(false)
		*tr.root.children = make([]*mapNode[K, V], 0, tr.max+1)
		*tr.root.children = append([]*mapNode[K, V]{}, left, right)
		tr.root.items = append([]mapPair[K, V]{}, median)
		tr.root.updateCount()
		return tr.Set(item.key, item.value)
	}
	if replaced {
		return prev, true
	}
	tr.count++
	return tr.empty.value, false
}

func (tr *Map[K, V]) nodeSplit(n *mapNode[K, V],
) (right *mapNode[K, V], median mapPair[K, V]) {
	i := tr.max / 2
	median = n.items[i]

	// right node
	right = tr.newNode(n.leaf())
	right.items = n.items[i+1:]
	if !n.leaf() {
		*right.children = (*n.children)[i+1:]
	}
	right.updateCount()

	// left node
	n.items[i] = tr.empty
	n.items = n.items[:i:i]
	if !n.leaf() {
		*n.children = (*n.children)[: i+1 : i+1]
	}
	n.updateCount()
	return right, median
}

func (n *mapNode[K, V]) updateCount() {
	n.count = len(n.items)
	if !n.leaf() {
		for i := 0; i < len(*n.children); i++ {
			n.count += (*n.children)[i].count
		}
	}
}

func (tr *Map[K, V]) nodeSet(pn **mapNode[K, V], item mapPair[K, V],
) (prev V, replaced bool, split bool) {
	n := *pn
	i, found := tr.search(n, item.key)
	if found {
		prev = n.items[i].value
		n.items[i] = item
		return prev, true, false
	}
	if n.leaf() {
		if len(n.items) == tr.max {
			return tr.empty.value, false, true
		}
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = item
		n.count++
		return tr.empty.value, false, false
	}
	prev, replaced, split = tr.nodeSet(&(*n.children)[i], item)
	if split {
		if len(n.items) == tr.max {
			return tr.empty.value, false, true
		}
		right, median := tr.nodeSplit((*n.children)[i])
		*n.children = append(*n.children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		return tr.nodeSet(&n, item)
	}
	if !replaced {
		n.count++
	}
	return prev, replaced, false
}

func (tr *Map[K, V]) Scan(iter func(key K, value V) bool) {
	if tr.root == nil {
		return
	}
	tr.nodeScan(tr.root, iter)
}

func (tr *Map[K, V]) nodeScan(n *mapNode[K, V], iter func(key K, value V) bool) bool {
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			if !iter(n.items[i].key, n.items[i].value) {
				return false
			}
		}
		return true
	}
	for i := 0; i < len(n.items); i++ {
		if !tr.nodeScan((*n.children)[i], iter) {
			return false
		}
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
	}
	return tr.nodeScan((*n.children)[len(*n.children)-1], iter)
}

func (tr *Map[K, V]) Get(key K) (V, bool) {
	if tr.root == nil {
		return tr.empty.value, false
	}
	n := tr.root
	for {
		i, found := tr.search(n, key)
		if found {
			return n.items[i].value, true
		}
		if n.leaf() {
			return tr.empty.value, false
		}
		n = (*n.children)[i]
	}
}

// Len returns the number of items in the tree
func (tr *Map[K, V]) Len() int {
	return tr.count
}

// Delete a value for a key and returns the deleted value.
// Returns false if there was no value by that key found.
func (tr *Map[K, V]) Delete(key K) (V, bool) {
	if tr.root == nil {
		return tr.empty.value, false
	}
	prev, deleted := tr.delete(tr.root, false, key)
	if !deleted {
		return tr.empty.value, false
	}
	if len(tr.root.items) == 0 && !tr.root.leaf() {
		tr.root = (*tr.root.children)[0]
	}
	tr.count--
	if tr.count == 0 {
		tr.root = nil
	}
	return prev.value, true
}

func (tr *Map[K, V]) delete(n *mapNode[K, V], max bool, key K) (mapPair[K, V], bool) {
	var i int
	var found bool
	if max {
		i, found = len(n.items)-1, true
	} else {
		i, found = tr.search(n, key)
	}
	if n.leaf() {
		if found {
			// found the items at the leaf, remove it and return.
			prev := n.items[i]
			copy(n.items[i:], n.items[i+1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			n.count--
			return prev, true
		}
		return tr.empty, false
	}

	var prev mapPair[K, V]
	var deleted bool
	if found {
		if max {
			i++
			prev, deleted = tr.delete((*n.children)[i], true, tr.empty.key)
		} else {
			prev = n.items[i]
			maxItem, _ := tr.delete((*n.children)[i], true, tr.empty.key)
			deleted = true
			n.items[i] = maxItem
		}
	} else {
		prev, deleted = tr.delete((*n.children)[i], max, key)
	}
	if !deleted {
		return tr.empty, false
	}
	n.count--
	if len((*n.children)[i].items) < tr.min {
		tr.nodeRebalance(n, i)
	}
	return prev, true
}

// DeletePrefix all values for a given key prefix. The iter function is called
// for each deleted entry. The key passed to the iter function is the full key
// of the deleted entry. The iterator is optional and can be nil.
// Returns the number of entries that have been deleted.
func (tr *Map[K, V]) DeletePrefix(key K, iter func(w K, v V)) int {
	if tr.root == nil {
		return 0
	}
	cnt := tr.deletePrefix(tr.root, key, iter)
	if cnt == 0 {
		return 0
	}
	if len(tr.root.items) == 0 && !tr.root.leaf() {
		tr.root = (*tr.root.children)[0]
	}
	tr.count -= cnt
	if tr.count == 0 {
		tr.root = nil
	}
	return cnt
}

func (tr *Map[K, V]) deletePrefix(n *mapNode[K, V], key K, iter func(w K, v V)) int {
	i, _ := tr.search(n, key)
	if i > len(n.items) {
		return 0
	}
	if n.leaf() {
		// If we're in a leaf, we can slice out all matching
		// items in one operation.
		end := i
		for end < len(n.items) && hasPrefix(n.items[end].key, key) {
			if iter != nil {
				iter(n.items[end].key, n.items[end].value)
			}
			end++
		}
		if end == i {
			// We haven't found anything, avoid calling slices.Delete
			// at all as an optimization since we have nothing to delete.
			return 0
		}
		n.items = slices.Delete(n.items, i, end)
		n.count -= end - i
		return end - i
	}

	deleted := tr.deletePrefix((*n.children)[i], key, iter)
	if deleted == 0 {
		// We deleted nothing with the prefix in this child,
		// which means there can't be any further matching
		// items in the tree.
		return 0
	}

	for ; i < len(n.items); i++ {
		if !hasPrefix(n.items[i].key, key) {
			// We have deleted items in the previous child. The only
			// reason we might need to delete items in the next child
			// is if the prefix continues into the next child. That means
			// that the non-leaf node also must be matching the prefix.
			// If the non-leaf node is not matching the prefix, then we
			// know the prefix is not in the tree anymore and the previous
			// child deletion deleted everything with the prefix.
			break
		}

		// Increase this since we're going to delete the current non-leaf
		// node as it's matching the prefix (or we would have bailed in the
		// previous check above).
		deleted++

		if (*n.children)[i].count == 0 {
			// We have deleted all items from this child, which means
			// we need to remove it entirely from the tree. The non-leaf
			// node can also be deleted as well.
			if iter != nil {
				iter(n.items[i].key, n.items[i].value)
			}
			n.items = slices.Delete(n.items, i, i+1)
			*n.children = slices.Delete(*n.children, i, i+1)

			// Delete from the next list to prepare for the next loop.
			deleted += tr.deletePrefix((*n.children)[i], key, iter)
			// Decrease iterator since we have sliced out the current child,
			// so we need to continue with the same i in the next loop.
			i--
			continue
		}

		// Find the maximum item from the current child which will be the new
		// non-leaf value for the current node.
		maxItem, _ := tr.delete((*n.children)[i], true, tr.empty.key)
		n.items[i] = maxItem

		// Now delete from the right side of the tree. If there's nothing
		// to delete there, we know we're done.
		del := tr.deletePrefix((*n.children)[i+1], key, iter)
		if del == 0 {
			break
		}
		deleted += del
	}

	if deleted == 0 {
		return 0
	}
	n.count -= deleted
	if len((*n.children)[i].items) < tr.min {
		tr.nodeRebalance(n, i)
	}
	return deleted
}

// nodeRebalance rebalances the child nodes following a delete operation.
// Provide the index of the child node with the number of items that fell
// below minItems.
func (tr *Map[K, V]) nodeRebalance(n *mapNode[K, V], i int) {
	if len(n.items) == 0 {
		n = nil
		return
	}
	if i == len(n.items) {
		i--
	}

	// ensure copy-on-write
	left := (*n.children)[i]
	right := (*n.children)[i+1]

	if len(left.items)+len(right.items) < tr.max {
		// Merges the left and right children nodes together as a single node
		// that includes (left,item,right), and places the contents into the
		// existing left node. Delete the right node altogether and move the
		// following items and child nodes to the left by one slot.

		// merge (left,item,right)
		left.items = append(left.items, n.items[i])
		left.items = append(left.items, right.items...)
		if !left.leaf() {
			*left.children = append(*left.children, *right.children...)
		}
		left.count += right.count + 1

		// move the items over one slot
		copy(n.items[i:], n.items[i+1:])
		n.items[len(n.items)-1] = tr.empty
		n.items = n.items[:len(n.items)-1]

		// move the children over one slot
		copy((*n.children)[i+1:], (*n.children)[i+2:])
		(*n.children)[len(*n.children)-1] = nil
		(*n.children) = (*n.children)[:len(*n.children)-1]
	} else if len(left.items) > len(right.items) {
		// move left -> right over one slot

		// Move the item of the parent node at index into the right-node first
		// slot, and move the left-node last item into the previously moved
		// parent item slot.
		right.items = append(right.items, tr.empty)
		copy(right.items[1:], right.items)
		right.items[0] = n.items[i]
		right.count++
		n.items[i] = left.items[len(left.items)-1]
		left.items[len(left.items)-1] = tr.empty
		left.items = left.items[:len(left.items)-1]
		left.count--

		if !left.leaf() {
			// move the left-node last child into the right-node first slot
			*right.children = append(*right.children, nil)
			copy((*right.children)[1:], *right.children)
			(*right.children)[0] = (*left.children)[len(*left.children)-1]
			(*left.children)[len(*left.children)-1] = nil
			(*left.children) = (*left.children)[:len(*left.children)-1]
			left.count -= (*right.children)[0].count
			right.count += (*right.children)[0].count
		}
	} else {
		// move left <- right over one slot

		// Same as above but the other direction
		left.items = append(left.items, n.items[i])
		left.count++
		n.items[i] = right.items[0]
		copy(right.items, right.items[1:])
		right.items[len(right.items)-1] = tr.empty
		right.items = right.items[:len(right.items)-1]
		right.count--

		if !left.leaf() {
			*left.children = append(*left.children, (*right.children)[0])
			copy(*right.children, (*right.children)[1:])
			(*right.children)[len(*right.children)-1] = nil
			*right.children = (*right.children)[:len(*right.children)-1]
			left.count += (*left.children)[len(*left.children)-1].count
			right.count -= (*left.children)[len(*left.children)-1].count
		}
	}
}

func (tr *Map[K, V]) Ascend(pivot K, inclusive bool, iter func(key K, value V) bool) {
	if tr.root == nil {
		return
	}
	tr.nodeAscend(tr.root, pivot, inclusive, iter)
}

// The return value of this function determines whether we should keep iterating
// upon this functions return.
func (tr *Map[K, V]) nodeAscend(n *mapNode[K, V], pivot K, inclusive bool, iter func(key K, value V) bool) bool {
	i, found := tr.search(n, pivot)
	if !found {
		if !n.leaf() {
			if !tr.nodeAscend((*n.children)[i], pivot, inclusive, iter) {
				return false
			}
		}
	}

	if found && !inclusive {
		if !n.leaf() {
			if !tr.nodeScan((*n.children)[i+1], iter) {
				return false
			}
		}
		i++
	}

	// We are either in the case that
	// - node is found, we should iterate through it starting at `i`,
	//   the index it was located at.
	// - node is not found, and TODO: fill in.
	for ; i < len(n.items); i++ {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !n.leaf() {
			if !tr.nodeScan((*n.children)[i+1], iter) {
				return false
			}
		}
	}
	return true
}

func (tr *Map[K, V]) Reverse(iter func(key K, value V) bool) {
	if tr.root == nil {
		return
	}
	tr.nodeReverse(tr.root, iter)
}

func (tr *Map[K, V]) nodeReverse(n *mapNode[K, V], iter func(key K, value V) bool) bool {
	if n.leaf() {
		for i := len(n.items) - 1; i >= 0; i-- {
			if !iter(n.items[i].key, n.items[i].value) {
				return false
			}
		}
		return true
	}
	if !tr.nodeReverse((*n.children)[len(*n.children)-1], iter) {
		return false
	}
	for i := len(n.items) - 1; i >= 0; i-- {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !tr.nodeReverse((*n.children)[i], iter) {
			return false
		}
	}
	return true
}

func (tr *Map[K, V]) Descend(pivot K, iter func(key K, value V) bool) {
	if tr.root == nil {
		return
	}
	tr.nodeDescend(tr.root, pivot, iter)
}

func (tr *Map[K, V]) nodeDescend(n *mapNode[K, V], pivot K, iter func(key K, value V) bool) bool {
	i, found := tr.search(n, pivot)
	if !found {
		if !n.leaf() {
			if !tr.nodeDescend((*n.children)[i], pivot, iter) {
				return false
			}
		}
		i--
	}
	for ; i >= 0; i-- {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !n.leaf() {
			if !tr.nodeReverse((*n.children)[i], iter) {
				return false
			}
		}
	}
	return true
}

// Load is for bulk loading pre-sorted items
func (tr *Map[K, V]) Load(key K, value V) (V, bool) {
	item := mapPair[K, V]{key: key, value: value}
	if tr.root == nil {
		return tr.Set(item.key, item.value)
	}
	n := tr.root
	for {
		n.count++ // optimistically update counts
		if n.leaf() {
			if len(n.items) < tr.max {
				if n.items[len(n.items)-1].key < item.key {
					n.items = append(n.items, item)
					tr.count++
					return tr.empty.value, false
				}
			}
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	// revert the counts
	n = tr.root
	for {
		n.count--
		if n.leaf() {
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	return tr.Set(item.key, item.value)
}

func (tr *Map[K, V]) Min() (key K, value V, ok bool) {
	if tr.root == nil {
		return key, value, false
	}
	n := tr.root
	for {
		if n.leaf() {
			item := n.items[0]
			return item.key, item.value, true
		}
		n = (*n.children)[0]
	}
}

// Max returns the maximum item in tree.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) Max() (K, V, bool) {
	if tr.root == nil {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.root
	for {
		if n.leaf() {
			item := n.items[len(n.items)-1]
			return item.key, item.value, true
		}
		n = (*n.children)[len(*n.children)-1]
	}
}

func (tr *Map[K, V]) GetAt(index int) (K, V, bool) {
	if tr.root == nil || index < 0 || index >= tr.count {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.root
	for {
		if n.leaf() {
			return n.items[index].key, n.items[index].value, true
		}
		i := 0
		for ; i < len(n.items); i++ {
			if index < (*n.children)[i].count {
				break
			} else if index == (*n.children)[i].count {
				return n.items[i].key, n.items[i].value, true
			}
			index -= (*n.children)[i].count + 1
		}
		n = (*n.children)[i]
	}
}

// DeleteAt deletes the item at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *Map[K, V]) DeleteAt(index int) (K, V, bool) {
	if tr.root == nil || index < 0 || index >= tr.count {
		return tr.empty.key, tr.empty.value, false
	}
	var pathbuf [8]uint8 // track the path
	path := pathbuf[:0]
	var item mapPair[K, V]
	n := tr.root
outer:
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			// the index is the item position
			item = n.items[index]
			if len(n.items) == tr.min {
				path = append(path, uint8(index))
				break outer
			}
			copy(n.items[index:], n.items[index+1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item.key, item.value, true
		}
		i := 0
		for ; i < len(n.items); i++ {
			if index < (*n.children)[i].count {
				break
			} else if index == (*n.children)[i].count {
				item = n.items[i]
				path = append(path, uint8(i))
				break outer
			}
			index -= (*n.children)[i].count + 1
		}
		path = append(path, uint8(i))
		n = (*n.children)[i]
	}
	// revert the counts
	n = tr.root
	for i := 0; i < len(path); i++ {
		n.count++
		if !n.leaf() {
			n = (*n.children)[uint8(path[i])]
		}
	}
	value, deleted := tr.Delete(item.key)
	if deleted {
		return item.key, value, true
	}
	return tr.empty.key, tr.empty.value, false
}

// Height returns the height of the tree.
// Returns zero if tree has no items.
func (tr *Map[K, V]) Height() int {
	var height int
	if tr.root != nil {
		n := tr.root
		for {
			height++
			if n.leaf() {
				break
			}
			n = (*n.children)[0]
		}
	}
	return height
}

func (tr *Map[K, V]) Values() []V {
	values := make([]V, 0, tr.Len())
	if tr.root != nil {
		values = tr.nodeValues(&tr.root, values)
	}
	return values
}

func (tr *Map[K, V]) nodeValues(cn **mapNode[K, V], values []V) []V {
	n := *cn
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			values = append(values, n.items[i].value)
		}
		return values
	}
	for i := 0; i < len(n.items); i++ {
		values = tr.nodeValues(&(*n.children)[i], values)
		values = append(values, n.items[i].value)
	}
	return tr.nodeValues(&(*n.children)[len(*n.children)-1], values)
}

// Keys returns all the keys in order.
func (tr *Map[K, V]) Keys() []K {
	keys := make([]K, 0, tr.Len())
	if tr.root != nil {
		keys = tr.root.keys(keys)
	}
	return keys
}

func (n *mapNode[K, V]) keys(keys []K) []K {
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			keys = append(keys, n.items[i].key)
		}
		return keys
	}
	for i := 0; i < len(n.items); i++ {
		keys = (*n.children)[i].keys(keys)
		keys = append(keys, n.items[i].key)
	}
	return (*n.children)[len(*n.children)-1].keys(keys)
}

func (tr *Map[K, V]) KeyValues() ([]K, []V) {
	keys := make([]K, 0, tr.Len())
	values := make([]V, 0, tr.Len())
	if tr.root != nil {
		keys, values = tr.nodeKeyValues(&tr.root, keys, values)
	}
	return keys, values
}

func (tr *Map[K, V]) nodeKeyValues(cn **mapNode[K, V], keys []K, values []V) ([]K, []V) {
	n := *cn
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			keys = append(keys, n.items[i].key)
			values = append(values, n.items[i].value)
		}
		return keys, values
	}
	for i := 0; i < len(n.items); i++ {
		keys, values = tr.nodeKeyValues(&(*n.children)[i], keys, values)
		keys = append(keys, n.items[i].key)
		values = append(values, n.items[i].value)
	}
	return tr.nodeKeyValues(&(*n.children)[len(*n.children)-1], keys, values)
}

// Clear will delete all items.
func (tr *Map[K, V]) Clear() {
	tr.count = 0
	tr.root = nil
}

func hasPrefix[K ordered](key, prefix K) bool {
	if k, ok := any(key).(prefixable[K]); ok {
		return k.HasPrefix(prefix)
	}
	return key == prefix
}
