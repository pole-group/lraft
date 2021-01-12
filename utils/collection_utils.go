// Copyright (c) 2020, Conf-Group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"errors"
	"math"
	"sync"
)

var (
	ErrArrayOutOfBound = errors.New("array out of bound")
)

const (
	IndexOutOfBoundErrMsg = "index out of bound, index=%d, offset=%d, pos=%s"
)

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
	for v, _ := range s.container {
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

func (s *Set) ToSlice(arr ...interface{}) {
	for v, _ := range s.container {
		arr = append(arr, v)
	}
}

func (s *Set) IsEmpty() bool {
	return s.Size() == 0
}

type SyncSet struct {
	container sync.Map
}

func NewSyncSet() *SyncSet {
	return &SyncSet{
		container: sync.Map{},
	}
}

func (s *SyncSet) Range(f func(value interface{})) {
	s.container.Range(func(key, value interface{}) bool {
		f(key)
		return true
	})
}

func (s *SyncSet) Add(value interface{}) {
	s.container.Store(value, member)
}

func (s *SyncSet) Remove(value interface{}) {
	s.container.Delete(value)
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

func (sl *SegmentList) Get(index int32) interface{} {
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

func (s *Segment) Get(index int32) interface{} {
	RequireTrue(index < s.pos && index >= s.offset, IndexOutOfBoundErrMsg, index, s.offset, s.pos)
	return s.elements[index]
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
