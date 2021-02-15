// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"fmt"
	"math"
	"sync"
)

const (
	IndexOutOfBoundErrMsg = "index out of bound, index=%d, offset=%d, pos=%d"
)

type CSliceOption func(opts *CSliceOptions)

type CSliceOptions struct {
	capacity int32
}

type ConcurrentSlice struct {
	lock     sync.RWMutex
	capacity int32
	size     int32
	cursor   int32
	values   []interface{}
}

func NewConcurrentSlice(opts ...CSliceOption) *ConcurrentSlice {
	cfg := new(CSliceOptions)
	for _, opt := range opts {
		opt(cfg)
	}
	return &ConcurrentSlice{
		lock:     sync.RWMutex{},
		capacity: 0,
		size:     0,
		cursor:   0,
		values:   make([]interface{}, cfg.capacity),
	}
}

func (cs *ConcurrentSlice) Remove(v interface{}) {
	defer cs.lock.Unlock()
	cs.lock.Lock()
	target := cs.values[:0]
	for _, item := range cs.values {
		if item != v {
			target = append(target, item)
		}
	}
	cs.values = target
	cs.cursor = int32(len(cs.values))
}

func (cs *ConcurrentSlice) Add(v interface{}) {
	defer cs.lock.Unlock()
	cs.lock.Lock()
	if cs.cursor >= cs.capacity {
		newValues := make([]interface{}, cs.capacity+cs.capacity/2)
		copy(newValues, cs.values)
		cs.values = newValues
		cs.capacity = int32(len(cs.values))
	}
	cs.values[cs.cursor] = v
	cs.cursor++
}

func (cs *ConcurrentSlice) GetFirst() interface{} {
	defer cs.lock.RUnlock()
	cs.lock.RUnlock()
	return cs.values[0]
}

func (cs *ConcurrentSlice) GetLast() interface{} {
	defer cs.lock.RUnlock()
	cs.lock.RUnlock()
	return cs.values[cs.size-1]
}

func (cs *ConcurrentSlice) ForEach(consumer func(index int, v interface{})) {
	defer cs.lock.RUnlock()
	cs.lock.RUnlock()
	for i, v := range cs.values {
		consumer(i, v)
	}
}

func (cs *ConcurrentSlice) Get(index int32) (interface{}, error) {
	defer cs.lock.RUnlock()
	cs.lock.RUnlock()
	if index >= cs.capacity {
		return nil, fmt.Errorf("index : %d is >= capacity : %d", index, cs.capacity)
	}
	return cs.values[index], nil
}

func (cs *ConcurrentSlice) Size() int32 {
	defer cs.lock.RUnlock()
	cs.lock.RUnlock()
	return cs.size
}

type ConcurrentMap struct {
	actualMap map[interface{}]interface{}
	rwLock    sync.RWMutex
}

func (cm *ConcurrentMap) Put(k, v interface{}) {
	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	cm.actualMap[k] = v
}

func (cm *ConcurrentMap) Remove(k interface{}) {
	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	delete(cm.actualMap, k)
}

func (cm *ConcurrentMap) Get(k interface{}) interface{} {
	defer cm.rwLock.RUnlock()
	cm.rwLock.RLock()
	return cm.actualMap[k]
}

func (cm *ConcurrentMap) Contains(k interface{}) bool {
	defer cm.rwLock.RUnlock()
	cm.rwLock.RLock()
	_, exist := cm.actualMap[k]
	return exist
}

func (cm *ConcurrentMap) ForEach(consumer func(k, v interface{})) {
	defer cm.rwLock.RUnlock()
	cm.rwLock.RLock()
	for k, v := range cm.actualMap {
		consumer(k, v)
	}
}

func (cm *ConcurrentMap) Keys() []interface{} {
	defer cm.rwLock.RUnlock()
	cm.rwLock.RLock()
	keys := make([]interface{}, len(cm.actualMap))
	i := 0
	for k := range cm.actualMap {
		keys[i] = k
		i++
	}
	return keys
}

func (cm *ConcurrentMap) Values() []interface{} {
	defer cm.rwLock.RUnlock()
	cm.rwLock.RLock()
	values := make([]interface{}, len(cm.actualMap))
	i := 0
	for _, v := range cm.actualMap {
		values[i] = v
		i++
	}
	return values
}

func (cm *ConcurrentMap) ComputeIfAbsent(key interface{}, supplier func() interface{}) {
	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()

	if _, exist := cm.actualMap[key]; !exist {
		cm.actualMap[key] = supplier()
	}
}

func (cm *ConcurrentMap) Clear() {
	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	cm.actualMap = make(map[interface{}]interface{})
}

func (cm *ConcurrentMap) Size() int {
	return len(cm.actualMap)
}

type void struct{}

var member void

type Set struct {
	container map[interface{}]void
}

func NewSet() *Set {
	return &Set{
		container: make(map[interface{}]void),
	}
}

func NewSetWithValues(arr ...interface{}) *Set {
	s := &Set{
		container: make(map[interface{}]void),
	}
	for _, e := range arr {
		s.Add(e)
	}
	return s
}

func (s *Set) Range(f func(value interface{})) {
	for v := range s.container {
		f(v)
	}
}

func (s *Set) Add(value interface{}) {
	s.container[value] = member
}

func (s *Set) AddAll(values ...interface{}) {
	for _, v := range values {
		s.container[v] = member
	}
}

func (s *Set) AddAllWithSet(set *Set) {
	set.Range(func(value interface{}) {
		s.container[value] = member
	})
}

func (s *Set) Remove(value interface{}) {
	delete(s.container, value)
}

func (s *Set) Size() int {
	return len(s.container)
}

func (s *Set) Contain(value interface{}) bool {
	_, exist := s.container[value]
	return exist
}

func (s *Set) RetainAll(arr ...interface{}) {
	for _, e := range arr {
		if !s.Contain(e) {
			delete(s.container, e)
		}
	}
}

func (s *Set) RetainAllWithSet(set *Set) {
	set.Range(func(value interface{}) {
		if !s.Contain(value) {
			delete(s.container, value)
		}
	})
}

func (s *Set) RemoveAll(arr []interface{}) {
	for _, e := range arr {
		delete(s.container, e)
	}
}

func (s *Set) RemoveAllWithSet(set *Set) {
	set.Range(func(value interface{}) {
		delete(s.container, value)
	})
}

func (s *Set) ToSlice() []interface{} {
	arr := make([]interface{}, len(s.container))
	for v := range s.container {
		arr = append(arr, v)
	}
	return arr
}

func (s *Set) IsEmpty() bool {
	return s.Size() == 0
}

type ConcurrentSet struct {
	lock      sync.RWMutex
	container *Set
}

func NewSyncSet() *ConcurrentSet {
	return &ConcurrentSet{
		lock:      sync.RWMutex{},
		container: NewSet(),
	}
}

func (s *ConcurrentSet) Range(f func(value interface{})) {
	defer s.lock.RUnlock()
	s.lock.RLock()
	s.container.Range(f)
}

func (s *ConcurrentSet) Add(value interface{}) {
	defer s.lock.Unlock()
	s.lock.Lock()
	s.container.Add(value)
}

func (s *ConcurrentSet) Remove(value interface{}) {
	defer s.lock.Unlock()
	s.lock.Lock()
	s.container.Remove(value)
}

const (
	SegmentShift = 7
	SegmentSize  = 2 << (SegmentShift - 1)
)

type SegmentList struct {
	pool        sync.Pool
	segments    []*Segment
	firstOffset int32
	size        int32
}

func NewSegmentList() *SegmentList {
	sl := &SegmentList{
		segments: nil,
	}

	sl.pool = sync.Pool{
		New: func() interface{} {
			return NewSegment(sl)
		},
	}
	return sl
}

func (sl *SegmentList) Get(index int32) (interface{}, error) {
	index += sl.firstOffset
	slot := index / 128
	i := index % 128
	return sl.segments[slot].Get(i)
}

func (sl *SegmentList) PeekFirst() interface{} {
	f := sl.GetFirst()
	return f.PeekFirst()
}

func (sl *SegmentList) PeekLast() interface{} {
	l := sl.GetLast()
	return l.PeekLast()
}

func (sl *SegmentList) GetFirst() *Segment {
	if sl.segments == nil || len(sl.segments) == 0 {
		return nil
	}
	return sl.segments[0]
}

func (sl *SegmentList) GetLast() *Segment {
	if sl.segments == nil || len(sl.segments) == 0 {
		return nil
	}
	return sl.segments[len(sl.segments)-1]
}

func (sl *SegmentList) Add(e interface{}) {
	lastSeg := sl.GetLast()
	if lastSeg == nil || lastSeg.IsReachEnd() {
		lastSeg = sl.pool.Get().(*Segment)
		sl.segments = append(sl.segments, lastSeg)
	}
	lastSeg.Add(e)
	sl.size++
}

func (sl *SegmentList) Size() int32 {
	return sl.size
}

func (sl *SegmentList) SegmentsSize() int32 {
	return int32(len(sl.segments))
}

func (sl *SegmentList) IsEmpty() bool {
	return sl.size == 0
}

func (sl *SegmentList) RemoveFromFirstWhen(predicate func(v interface{}) bool) {
	firstSeg := sl.GetFirst()
	for {
		if firstSeg == nil {
			sl.firstOffset = 0
			sl.size = 0
			return
		}
		removed := firstSeg.RemoveFromFirstWhen(predicate)
		if removed == 0 {
			break
		}
		sl.size -= removed
		sl.firstOffset = firstSeg.offset
		if firstSeg.IsEmpty() {
			sl.segments = sl.segments[1:]
			firstSeg.recycle()
			firstSeg = sl.GetFirst()
			sl.firstOffset = 0
		}
	}
}

func (sl *SegmentList) RemoveFromLastWhen(predicate func(v interface{}) bool) {
	lastSeg := sl.GetLast()
	for {
		if lastSeg == nil {
			sl.firstOffset = 0
			sl.size = 0
			return
		}
		removed := lastSeg.RemoveFromLastWhen(predicate)
		if removed == 0 {
			break
		}
		sl.size -= removed
		if lastSeg.IsEmpty() {
			sl.segments = sl.segments[:sl.SegmentsSize()-1]
			lastSeg.recycle()
			lastSeg = sl.GetLast()
		}
	}
}

func (sl *SegmentList) RemoveFromFirst(toIndex int32) {
	alignedIndex := sl.firstOffset + toIndex
	toSegIndex := alignedIndex / SegmentSize
	toIndexInSeg := alignedIndex % SegmentSize
	if toSegIndex > 0 {
		sl.segments = sl.segments[toIndexInSeg-1:]
		sl.size = toSegIndex*SegmentSize - sl.firstOffset
	}
	firstSeg := sl.GetFirst()
	if firstSeg != nil {
		sl.size -= firstSeg.RemoveFromFirst(toIndexInSeg)
		sl.firstOffset = firstSeg.offset
		if firstSeg.IsEmpty() {
			firstSeg.recycle()
			sl.firstOffset = 0
		}
	} else {
		sl.firstOffset = 0
		sl.size = 0
	}
}

func (sl *SegmentList) AddAll(arr []interface{}) {
	srcPos := int32(0)
	srcSize := int32(len(arr))

	lastSeg := sl.GetLast()
	for srcPos < srcSize {
		if lastSeg == nil || lastSeg.IsReachEnd() {
			lastSeg = sl.pool.Get().(*Segment)
			sl.segments = append(sl.segments, lastSeg)
		}
		l := int32(math.Min(float64(srcSize-srcPos), float64(lastSeg.Cap())))
		lastSeg.AddAll(arr, srcPos, l)
		srcPos += l
		sl.size += l
	}
}

func (sl *SegmentList) Clear() {
	for _, seg := range sl.segments {
		seg.recycle()
	}
	sl.size = 0
}

type Segment struct {
	owner    *SegmentList
	elements []interface{}
	pos      int32
	offset   int32
}

func NewSegment(owner *SegmentList) *Segment {
	return &Segment{
		owner:    owner,
		elements: make([]interface{}, SegmentSize),
		pos:      0,
		offset:   0,
	}
}

func (s *Segment) recycle() {
	s.Clear()
	s.owner.pool.Put(s)
}

func (s *Segment) Clear() {
	s.pos = 0
	s.offset = 0
	FillTargetElement(s.elements, nil)
}

func (s *Segment) Cap() int32 {
	return SegmentSize - s.pos
}

func (s *Segment) AddAll(src []interface{}, srcPos, size int32) {
	ArrayCopy(src, srcPos, s.elements, s.pos, size)
	s.pos += size
}

func (s *Segment) Add(e interface{}) {
	s.elements[s.pos] = e
	s.pos++
}

func (s *Segment) Get(index int32) (interface{}, error) {
	if !(index < s.pos && index >= s.offset) {
		return nil, fmt.Errorf(IndexOutOfBoundErrMsg, index, s.offset, s.pos)
	}
	return s.elements[index], nil
}

func (s *Segment) PeekFirst() interface{} {
	return s.elements[s.offset]
}

func (s *Segment) PeekLast() interface{} {
	return s.elements[s.pos-1]
}

func (s *Segment) RemoveFromFirstWhen(predicate func(v interface{}) bool) int32 {
	removed := int32(0)
	for i := s.offset; i < s.pos; i++ {
		e := s.elements[i]
		if predicate(e) {
			s.elements[i] = nil
			removed++
		} else {
			break
		}
	}
	s.offset += removed
	return removed
}

func (s *Segment) RemoveFromLastWhen(predicate func(v interface{}) bool) int32 {
	removed := int32(0)
	for i := s.pos - 1; i >= s.offset; i-- {
		e := s.elements[i]
		if predicate(e) {
			s.elements[i] = nil
			removed++
		} else {
			break
		}
	}
	s.pos -= removed
	return removed
}

func (s *Segment) RemoveFromFirst(toIndex int32) int32 {
	removed := int32(0)
	for i := s.offset; i < int32(math.Max(float64(toIndex), float64(s.pos))); i++ {
		s.elements[i] = nil
		removed++
	}
	s.offset += removed
	return removed
}

func (s *Segment) IsReachEnd() bool {
	return s.pos == SegmentSize
}

func (s *Segment) IsEmpty() bool {
	return s.Size() == 0
}

func (s *Segment) Size() int32 {
	return s.pos - s.offset
}

func ArrayCopy(src []interface{}, srcPos int32, target []interface{}, targetPos int32, length int32) {
	ti := targetPos
	for i := srcPos; i < length; i++ {
		target[ti] = src[i]
		ti++
	}
}

func FillTargetElement(array []interface{}, e interface{}) {
	size := len(array)
	for i := 0; i < size; i++ {
		array[i] = e
	}
}

type BinarySearchTree struct {
	root    *node
	size    int64
	compare func(a, b interface{}) int
}

type node struct {
	val    interface{}
	parent *node
	left   *node
	right  *node
}

func NewBinarySearchTree(compare func(a, b interface{}) int) *BinarySearchTree {
	return &BinarySearchTree{
		compare: compare,
		root:    nil,
		size:    0,
	}
}

func (bTree *BinarySearchTree) SeekLevel() [][]*node {
	if bTree.root == nil {
		return nil
	}

	ans := make([][]*node, 0)
	tmp := make([]*node, 0)
	_stack := make([]*node, 0)
	_stack = append(_stack, bTree.root)
	nowNodeSize := len(_stack)
	for len(_stack) != 0 {
		if nowNodeSize == 0 {
			ans = append(ans, tmp)
			tmp = make([]*node, 0)
			nowNodeSize = len(_stack)
		}
		p := _stack[0]
		_stack = _stack[1:]
		nowNodeSize--
		tmp = append(tmp, p)
		if p.left != nil {
			_stack = append(_stack, p.left)
		}
		if p.right != nil {
			_stack = append(_stack, p.right)
		}
	}
	ans = append(ans, tmp)
	return ans
}

func (bTree *BinarySearchTree) Find(v interface{}) *node {
	return bTree.findNearbyLeftNode(v, bTree.root)
}

func (bTree *BinarySearchTree) findTargetNode(v interface{}, root *node) *node {
	if root != nil {
		if bTree.compare(v, root.val) == 0 {
			return root
		}
		if bTree.compare(v, root.val) < 0 {
			return bTree.findTargetNode(v, root.left)
		}
		return bTree.findTargetNode(v, root.right)
	}
	return nil
}

//					5
//				  /   \
//				 3     8
//				/ \   / \
//             1   4 7   9
//
// if you find 5, will return 3, if find 1, will return nil
func (bTree *BinarySearchTree) FindNearbyLeft(v interface{}) *node {
	return bTree.findNearbyLeftNode(v, bTree.root)
}

func (bTree *BinarySearchTree) findNearbyLeftNode(v interface{}, root *node) *node {
	if root == nil {
		return nil
	}
	if bTree.compare(v, root.val) <= 0 {
		if root.left != nil {
			if bTree.compare(v, root.left.val) > 0 {
				return root.left
			} else {
				return bTree.findNearbyLeftNode(v, root.left)
			}
		}
		return root
	} else {
		return bTree.findNearbyLeftNode(v, root.right)
	}
}

//					5
//				  /   \
//				 3     8
//				/ \   / \
//             1   4 7   9
//
// if you find 5, will return 7, if find 8, will return 9
func (bTree *BinarySearchTree) FindNearbyRight(v string) *node {
	return bTree.findMaxNode(bTree.root)
}

func (bTree *BinarySearchTree) FindMax() *node {
	return bTree.findMaxNode(bTree.root)
}

func (bTree *BinarySearchTree) findMaxNode(root *node) *node {
	if root == nil {
		return nil
	}
	if root.left == nil && root.right == nil {
		return root
	}
	return bTree.findMaxNode(root.left)
}

func (bTree *BinarySearchTree) FindMin() *node {
	return bTree.findMinNode(bTree.root)
}

func (bTree *BinarySearchTree) findMinNode(root *node) *node {
	if root == nil {
		return nil
	}
	if root.left == nil && root.right == nil {
		return root
	}
	return bTree.findMinNode(root.left)
}

func (bTree *BinarySearchTree) Insert(v interface{}, replaceOld bool) {
	bTree.root = bTree.insertVal(v, bTree.root, bTree.root, true)
	bTree.size++
}

func (bTree *BinarySearchTree) insertVal(v interface{}, root *node, parent *node, replace bool) *node {
	if root == nil {
		return &node{
			val:    v,
			parent: parent,
			left:   nil,
			right:  nil,
		}
	}
	if bTree.compare(v, root.val) < 0 {
		root.left = bTree.insertVal(v, root.left, root, replace)
	} else if bTree.compare(v, root.val) > 0 {
		root.right = bTree.insertVal(v, root.right, root, replace)
	}
	if replace {
		root.val = v
	}
	return root
}

func (bTree *BinarySearchTree) Delete(v interface{}) {
	bTree.deleteVal(v, bTree.root)
	bTree.size--
}

func (bTree *BinarySearchTree) deleteVal(v interface{}, root *node) *node {
	if root == nil {
		return nil
	}
	if bTree.compare(v, root.val) < 0 {
		root.left = bTree.deleteVal(v, root.left)
		root.left.parent = root
	} else if bTree.compare(v, root.val) > 0 {
		root.right = bTree.deleteVal(v, root.right)
		root.right.parent = root
	} else if root.left != nil && root.right != nil {
		rMin := bTree.findMinNode(root.right)
		root.val = rMin.val
		root.right = bTree.deleteVal(rMin.val, root.right)
	} else {
		tmpCell := root
		if root.left == nil {
			root = root.right
		} else if root.right != nil {
			root = root.left
		}

		tmpCell.left = nil
		tmpCell.right = nil
		tmpCell.parent = nil
	}
	return root
}

func (bTree *BinarySearchTree) Range(call func(n *node)) {
	bTree.rangeVal(bTree.root, call)
}

func (bTree *BinarySearchTree) rangeVal(root *node, call func(n *node)) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("err : %#v", err)
		}
	}()
	if root != nil {
		call(root)
		bTree.rangeVal(root.left, call)
		bTree.rangeVal(root.right, call)
	}
}

//TreeMap 后面要优化成为红黑树，或者较为简单的 AVL 树
type TreeMap struct {
	BinarySearchTree
	keyCompare func(a, b interface{}) int
}

//NewTreeMap 创建一个 TreeMap
func NewTreeMap(compare func(a, b interface{}) int) *TreeMap {
	tMap := &TreeMap{
		keyCompare: nil,
	}
	tMap.compare = func(a, b interface{}) int {
		aEntry := a.(mapEntry)
		bEntry := b.(mapEntry)
		return tMap.keyCompare(aEntry.key, bEntry.key)
	}
	tMap.size = 0

	return tMap
}

type mapEntry struct {
	key interface{}
	val interface{}
}

//Put 添加一个 key-value 键值对
func (tMap *TreeMap) Put(key, val interface{}) {
	entry := mapEntry{
		key: key,
		val: val,
	}
	tMap.Insert(entry, true)
}

func (tMap *TreeMap) RemoveKey(key interface{}) {
	entry := mapEntry{
		key: key,
	}
	tMap.Delete(entry)
}

func (tMap *TreeMap) Get(key interface{}) interface{} {
	entry := mapEntry{
		key: key,
		val: nil,
	}
	n := tMap.Find(entry)
	if n == nil {
		return nil
	}
	return n.val.(mapEntry).val
}

func (tMap *TreeMap) RangeEntry(consumer func(k, v interface{})) {
	tMap.Range(func(n *node) {
		entry := n.val.(mapEntry)
		consumer(entry.key, entry.val)
	})
}

func (tMap *TreeMap) RangeLessThan(key interface{}, consumer func(k, v interface{})) {
	tMap.rangeLessThan(mapEntry{
		key: key,
	}, tMap.root, consumer)
}

func (tMap *TreeMap) rangeLessThan(entry mapEntry, root *node, consumer func(k, v interface{})) {
	if root != nil {
		goR := false
		// 如果当前 root 的 值都比 entry 来得大，一定不需要进入右子树进行遍历
		if tMap.keyCompare(entry, root.val) <= 0 {
			e := root.val.(mapEntry)
			consumer(e.key, e.val)
			goR = true
		}
		if root.left != nil {
			if tMap.keyCompare(entry, root.left.val) <= 0 {
				tMap.rangeLessThan(entry, root.left, consumer)
			}
		}
		if root.right != nil && goR {
			if tMap.keyCompare(entry, root.right.val) <= 0 {
				tMap.rangeLessThan(entry, root.right, consumer)
			}
		}
	}
}

func (tMap *TreeMap) ComputeIfAbsent(key interface{}, supplier func() interface{}) interface{} {
	keyEntry := mapEntry{
		key: key,
		val: nil,
	}
	targetEntry := tMap.insertIfValueNotExist(keyEntry, supplier, tMap.root, nil)
	tMap.size++
	return targetEntry.val
}

func (tMap *TreeMap) insertIfValueNotExist(val mapEntry, supplier func() interface{}, root *node, parent *node) *node {
	if root == nil {
		val.val = supplier()
		return &node{
			val:    val,
			parent: parent,
			left:   nil,
			right:  nil,
		}
	}
	if tMap.compare(val, root.val) < 0 {
		root.left = tMap.insertIfValueNotExist(val, supplier, root, parent)
	} else if tMap.compare(val, root.val) > 0 {
		root.right = tMap.insertIfValueNotExist(val, supplier, root, parent)
	}
	return root
}

func (tMap *TreeMap) Size() int64 {
	return tMap.size
}

func (tMap *TreeMap) IsEmpty() bool {
	return tMap.size == 0
}

func (tMap *TreeMap) Clear() {
	tMap.size = 0
	tMap.root = nil
}
