// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"fmt"
)

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

	ans := make([][]*node, 0, 0)
	tmp := make([]*node, 0, 0)
	_stack := make([]*node, 0, 0)
	_stack = append(_stack, bTree.root)
	nowNodeSize := len(_stack)
	for len(_stack) != 0 {
		if nowNodeSize == 0 {
			ans = append(ans, tmp)
			tmp = make([]*node, 0, 0)
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

type TreeMap struct {
	BinarySearchTree
	keyCompare func(a, b interface{}) int
}

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
