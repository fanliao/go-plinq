package plinq

import (
	//"time"
	"bytes"
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	ptrSize        = unsafe.Sizeof((*byte)(nil))
	kindMask       = 0x7f
	kindNoPointers = 0x80
)

func self(v interface{}) interface{} {
	return v
}

//the Utils functions for Slice and Chunk----------------------
func distinctChunkValues(c *Chunk, distKVs map[interface{}]int, pResults *[]interface{}) *Chunk {
	if pResults == nil {
		size := c.Data.Len()
		result := make([]interface{}, 0, size)
		pResults = &result
	}

	forEachSlicer(c.Data, func(i int, v interface{}) {
		if kv, ok := v.(*hKeyValue); ok {
			//fmt.Println("distinctChunkValues get==", i, v, kv)
			if _, ok := distKVs[kv.keyHash]; !ok {
				distKVs[kv.keyHash] = 1
				*pResults = append(*pResults, kv.value)
			}
		} else {
			if _, ok := distKVs[v]; !ok {
				distKVs[v] = 1
				*pResults = append(*pResults, v)
			}
		}
	})
	c.Data = NewSlicer(*pResults)
	return c
}

func getChunkOprFunc(sliceOpr func(Slicer, interface{}) Slicer, opr interface{}) func(*Chunk) *Chunk {
	return func(c *Chunk) *Chunk {
		result := sliceOpr(c.Data, opr)
		if result != nil {
			return &Chunk{result, c.Order, c.StartIndex}
		} else {
			return nil
		}
	}
}

func getMapChunkFunc(f OneArgsFunc) func(*Chunk) *Chunk {
	return func(c *Chunk) *Chunk {
		result := mapSlice(c.Data, f)
		return &Chunk{result, c.Order, c.StartIndex}
	}
}

func getMapChunkToSelfFunc(f OneArgsFunc) func(*Chunk) *Chunk {
	return func(c *Chunk) *Chunk {
		result := mapSliceToSelf(c.Data, f)
		return &Chunk{result, c.Order, c.StartIndex}
	}
}

func filterSlice(data Slicer, f interface{}) Slicer {
	var (
		predicate PredicateFunc
		ok        bool
	)
	if predicate, ok = f.(PredicateFunc); !ok {
		predicate = PredicateFunc(f.(func(interface{}) bool))
	}

	size := data.Len()
	count, dst := 0, make([]interface{}, size)
	for i := 0; i < size; i++ {
		v := data.Index(i)
		if predicate(v) {
			dst[count] = v
			count++
		}
	}
	return NewSlicer(dst[0:count])
}

func forEachSlicer(src Slicer, f interface{}) Slicer {
	act := f.(func(int, interface{}))
	size := src.Len()
	for i := 0; i < size; i++ {
		act(i, src.Index(i))
	}
	return nil
}

func mapSliceToMany(src Slicer, f func(interface{}) []interface{}) Slicer {
	size := src.Len()
	dst := make([]interface{}, 0, size)

	for i := 0; i < size; i++ {
		rs := f(src.Index(i))
		dst = appendToSlice(dst, rs...)
	}
	return NewSlicer(dst)
}

func mapSlice(src Slicer, f interface{}) Slicer {
	var (
		mapFunc OneArgsFunc
		ok      bool
	)
	if mapFunc, ok = f.(OneArgsFunc); !ok {
		mapFunc = OneArgsFunc(f.(func(interface{}) interface{}))
	}

	size := src.Len()
	dst := make([]interface{}, size)
	//fmt.Println("mapSlice,", src.ToInterfaces())
	for i := 0; i < size; i++ {
		dst[i] = mapFunc(src.Index(i))
	}
	return NewSlicer(dst)
}

func mapSliceToSelf(src Slicer, f interface{}) Slicer {
	var (
		mapFunc OneArgsFunc
		ok      bool
	)
	if mapFunc, ok = f.(OneArgsFunc); !ok {
		mapFunc = OneArgsFunc(f.(func(interface{}) interface{}))
	}

	if s, ok := src.(*interfaceSlicer); ok {
		size := src.Len()
		for i := 0; i < size; i++ {
			s.data[i] = mapFunc(s.data[i])
		}
		return NewSlicer(s.data)
	} else {
		panic(errors.New(fmt.Sprint("mapSliceToSelf, Unsupport type",
			reflect.Indirect(reflect.ValueOf(src)).Type())))
	}
}

func getMapChunkToKeyList(useDefHash *uint32, converter OneArgsFunc, getResult func(*Chunk, bool) Slicer) func(c *Chunk) Slicer {
	return func(c *Chunk) (rs Slicer) {
		useValAsKey := false
		valCanAsKey := atomic.LoadUint32(useDefHash)
		useSelf := isNil(converter)

		if converter == nil {
			converter = self
		}

		if valCanAsKey == 1 {
			useValAsKey = true
		} else if valCanAsKey == 0 {
			if c.Data.Len() > 0 && testCanAsKey(converter(c.Data.Index(0))) {
				atomic.StoreUint32(useDefHash, 1)
				useValAsKey = true
			} else if c.Data.Len() == 0 {
				useValAsKey = false
			} else {
				atomic.StoreUint32(useDefHash, 1000)
				useValAsKey = false
			}
		} else {
			useValAsKey = false
		}

		if !useValAsKey {
			if c.Data.Len() > 0 {
				//fmt.Println("WARNING:use hash")
			}
		}
		if useValAsKey && useSelf {
			rs = c.Data
		} else {
			rs = getResult(c, useValAsKey)
		}
		return
	}
}

func getMapChunkToKVs(useDefHash *uint32, converter OneArgsFunc) func(c *Chunk) Slicer {
	return getMapChunkToKeyList(useDefHash, converter, func(c *Chunk, useValAsKey bool) Slicer {
		return chunkToKeyValues(c, !useValAsKey, converter, nil)
	})
}

func getMapChunkToKVChunkFunc(useDefHash *uint32, converter OneArgsFunc) func(c *Chunk) (r *Chunk) {
	return func(c *Chunk) (r *Chunk) {
		slicer := getMapChunkToKVs(useDefHash, converter)(c)
		//fmt.Println("\ngetMapChunkToKVChunk", c, slicer)
		return &Chunk{slicer, c.Order, c.StartIndex}
	}
}

func getMapChunkToKVChunk2(useDefHash *uint32, maxOrder *int, converter OneArgsFunc) func(c *Chunk) (r *Chunk) {
	return func(c *Chunk) (r *Chunk) {
		slicer := getMapChunkToKVs(useDefHash, converter)(c)
		if c.Order > *maxOrder {
			*maxOrder = c.Order
		}
		return &Chunk{slicer, c.Order, c.StartIndex}
	}
}

//TODO: the code need be restructured
func aggregateSlice(src Slicer, fs []*AggregateOperation, asSequential bool, asParallel bool) Slicer {
	size := src.Len()
	if size == 0 {
		panic(errors.New("Cannot aggregate empty slice"))
	}

	rs := make([]interface{}, len(fs))
	for j := 0; j < len(fs); j++ {
		//if (asSequential && fs[j].ReduceAction == nil) || (asParallel && fs[j].ReduceAction != nil) {
		rs[j] = fs[j].Seed
		//}
	}

	for i := 0; i < size; i++ {
		for j := 0; j < len(fs); j++ {
			//if (asSequential && fs[j].ReduceAction == nil) || (asParallel && fs[j].ReduceAction != nil) {
			rs[j] = fs[j].AggAction(src.Index(i), rs[j])
			//}
		}
	}
	return NewSlicer(rs)
}

func expandChunks(src []interface{}, keepOrder bool) []interface{} {
	if src == nil {
		return nil
	}

	if keepOrder {
		//根据需要排序
		src = sortSlice(src, func(a interface{}, b interface{}) bool {
			var (
				a1, b1 *Chunk
			)

			if isNil(a) {
				return true
			} else if isNil(b) {
				return false
			}

			switch v := a.(type) {
			case []interface{}:
				a1, b1 = v[0].(*Chunk), b.([]interface{})[0].(*Chunk)
			case *Chunk:
				a1, b1 = v, b.(*Chunk)
			}
			return a1.Order < b1.Order
		})
	}

	//得到块列表
	count := 0
	chunks := make([]*Chunk, len(src))
	for i, c := range src {
		if isNil(c) {
			continue
		}
		switch v := c.(type) {
		case []interface{}:
			chunks[i] = v[0].(*Chunk)
		case *Chunk:
			chunks[i] = v
		}
		count += chunks[i].Data.Len()
	}

	//得到interface{} slice
	result := make([]interface{}, count)
	start := 0
	for _, c := range chunks {
		if c == nil {
			continue
		}
		size := c.Data.Len()
		copy(result[start:start+size], c.Data.ToInterfaces())
		start += size
	}
	return result
}

func max(i, j int) int {
	if i > j {
		return i
	} else {
		return j
	}
}

func appendToSlice(src []interface{}, vs ...interface{}) []interface{} {
	c, l := cap(src), len(src)
	if c <= l+len(vs) {
		newSlice := make([]interface{}, l, max(2*c, l+len(vs)))
		_ = copy(newSlice[0:l], src)
		for _, v := range vs {
			//reslice
			newSlice = append(newSlice, v)
		}
		return newSlice
	} else {
		for _, v := range vs {
			src = append(src, v)
		}
		return src
	}
}

//reflect functions------------------------------------------------

// interfaceHeader is the header for an interface{} value. it is copied from unsafe.emptyInterface
type interfaceHeader struct {
	typ  *rtype
	word uintptr
}

// rtype is the common implementation of most values.
// It is embedded in other, public struct types, but always
// with a unique tag like `reflect:"array"` or `reflect:"ptr"`
// so that code cannot convert from, say, *arrayType to *ptrType.
type rtype struct {
	size              uintptr        // size in bytes
	hash              uint32         // hash of type; avoids computation in hash tables
	_                 uint8          // unused/padding
	align             uint8          // alignment of variable with this type
	fieldAlign        uint8          // alignment of struct field with this type
	kind              uint8          // enumeration for C
	alg               *uintptr       // algorithm table (../runtime/runtime.h:/Alg)
	gc                unsafe.Pointer // garbage collection data
	string            *string        // string form; unnecessary but undeniably useful
	ptrToUncommonType uintptr        // (relatively) uncommon fields
	ptrToThis         *rtype         // type for pointer to this type, if used in binary or has methods
}

func (t *rtype) Kind() reflect.Kind { return reflect.Kind(t.kind & kindMask) }

// structType represents a struct type.
type structType struct {
	rtype  `reflect:"struct"`
	fields []structField // sorted by offset
}

// Struct field
type structField struct {
	name    *string // nil for embedded fields
	pkgPath *string // nil for exported Names; otherwise import path
	typ     *rtype  // type of field
	tag     *string // nil if no tag
	offset  uintptr // byte offset of field within struct
}

func dataPtr(data interface{}) (ptr unsafe.Pointer, typ *rtype) {
	headerPtr := *((*interfaceHeader)(unsafe.Pointer(&data)))

	typ = headerPtr.typ
	if headerPtr.typ != nil {
		size := headerPtr.typ.size
		if size > ptrSize && headerPtr.word != 0 {
			//如果是非指针类型并且数据size大于一个字，则interface的word是数据的地址
			ptr = unsafe.Pointer(headerPtr.word)
		} else {
			//如果是指针类型或者数据size小于等于一个字，则interface的word是数据本身
			ptr = unsafe.Pointer(&(headerPtr.word))
		}
	}
	return
}

func isNil(data interface{}) bool {
	if data == nil {
		return true
	}

	ptr := *((*interfaceHeader)(unsafe.Pointer(&data)))
	typ := ptr.typ
	size := typ.size

	switch typ.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr:
		//ptr := unsafe.Pointer(ptr.word)
		if size > ptrSize {
			//如果size大于1个字，word是一个指向数据地址的指针
			return *(*unsafe.Pointer)(unsafe.Pointer(ptr.word)) == nil
		} else {
			//如果size不大于1个字，word保存的是数据本身，如果是引用，则就是被引用的数据地址
			return ptr.word == 0
		}
		//return ptr == nil
	case reflect.Interface, reflect.Slice:
		return ptr.word == 0
	default:
		return false
	}
}

//hash functions-------------------------------------------------------

func hashByPtr(dataPtr unsafe.Pointer, typ *rtype, hashObj *sHash) {
	t := typ
	if t == nil {
		hashValue(unsafe.Pointer(uintptr(0)), 0, hashObj)
		return
	}

	switch t.Kind() {
	case reflect.String:
		hashString(*((*string)(dataPtr)), hashObj)
	case reflect.Struct:
		hashStruct(dataPtr, t, hashObj)
	case reflect.Int8:
		hashUInt32(uint32(*((*int8)(dataPtr))), hashObj)
	case reflect.Int16:
		hashUInt32(uint32(*((*int16)(dataPtr))), hashObj)
	case reflect.Int32:
		hashUInt32(uint32(*((*int32)(dataPtr))), hashObj)
	case reflect.Uint8:
		hashUInt32(uint32(*((*uint8)(dataPtr))), hashObj)
	case reflect.Uint16:
		hashUInt32(uint32(*((*uint16)(dataPtr))), hashObj)
	case reflect.Uint32:
		hashUInt32(*((*uint32)(dataPtr)), hashObj)
	case reflect.Int64:
		v := *((*int64)(dataPtr))
		hashUInt32((uint32)(v&0x7FFFFFFF), hashObj)
		hashUInt32((uint32)(v>>32), hashObj)
	case reflect.Uint64:
		v := *((*uint64)(dataPtr))
		hashUInt32((uint32)(v&0x7FFFFFFF), hashObj)
		hashUInt32((uint32)(v>>32), hashObj)
	case reflect.Int:
		if ptrSize > 4 {
			v := *((*int64)(dataPtr))
			hashUInt32((uint32)(v&0x7FFFFFFF), hashObj)
			hashUInt32((uint32)(v>>32), hashObj)
		} else {
			hashUInt32(uint32(*((*int32)(dataPtr))), hashObj)
		}
	case reflect.Uint:
		if ptrSize > 4 {
			v := *((*uint64)(dataPtr))
			hashUInt32((uint32)(v&0x7FFFFFFF), hashObj)
			hashUInt32((uint32)(v>>32), hashObj)
		} else {
			hashUInt32(*((*uint32)(dataPtr)), hashObj)
		}
	default:
		hashValue(dataPtr, typ.size, hashObj)
	}
}

func hashString(s string, hashObj *sHash) {
	hashObj.Write([]byte(s))
}

func hashStruct(dataPtr unsafe.Pointer, typ *rtype, hashObj *sHash) {
	s := *((*structType)(unsafe.Pointer(typ)))

	numField := len(s.fields)
	for i := 0; i < numField; i++ {
		fld := s.fields[i]
		offset, fTyp := fld.offset, fld.typ
		hashByPtr(unsafe.Pointer(uintptr(dataPtr)+offset), fTyp, hashObj)
	}
}

func hashUInt32(data uint32, hashObj *sHash) {
	hashObj.WriteUInt32(data)
}

func hashValue(dataPtr unsafe.Pointer, size uintptr, hashObj *sHash) {
	var i uintptr
	for i = 0; i < size; i++ {
		c := *((*byte)(unsafe.Pointer(uintptr(dataPtr) + i)))
		hashObj.WriteBype(c)
	}
}

func hash64(data interface{}) interface{} {
	dataPtr, size := dataPtr(data)
	sh := newSHash()
	hashByPtr(dataPtr, size, sh)
	//fmt.Println("hash", data, bkdr.Sum32(), djb.Sum32(), uint64(bkdr.Sum32())<<32|uint64(djb.Sum32()), "\n")
	return sh.Sum64()
}

const (
	bKDR32seed   = 131
	dJB32prime32 = 5381
)

// NewBKDR32 returns a new 32-bit BKDR hash
func newBKDR32() shash32 {
	var s bkdr32 = 0
	return &s
}

// NewBKDR32 returns a new 32-bit BKDR hash
func newDJB32() shash32 {
	var s djb32 = dJB32prime32
	return &s
}

type shash32 interface {
	Sum32() uint32
	Write(data []byte)
	WriteBype(data byte)
	WriteUInt32(data uint32)
}

type (
	bkdr32 uint32
	djb32  uint32
)

func (s *bkdr32) Sum32() uint32 { return uint32(*s) }
func (s *djb32) Sum32() uint32  { return uint32(*s) }

func (s *bkdr32) Write(data []byte) {
	hash := *s
	for _, c := range data {
		hash = hash*bKDR32seed + bkdr32(c)
	}
	*s = hash
}

func (s *djb32) Write(data []byte) {
	hash := *s
	for _, c := range data {
		hash = ((hash << 5) + hash) + djb32(c)
	}
	*s = hash
}

func (s *bkdr32) WriteBype(data byte) {
	hash := *s
	hash = hash*bKDR32seed + bkdr32(data)
	*s = hash
}

func (s *djb32) WriteBype(data byte) {
	hash := *s
	hash = ((hash << 5) + hash) + djb32(data)
	*s = hash
}

func (s *bkdr32) WriteUInt32(data uint32) {
	hash := *s
	hash = hash*bKDR32seed + bkdr32(data)
	*s = hash
}

func (s *djb32) WriteUInt32(data uint32) {
	hash := *s
	hash = ((hash << 5) + hash) + djb32(data)
	*s = hash
}

type sHash struct {
	hash1 shash32
	hash2 shash32
}

func (this *sHash) Sum64() uint64 {
	return uint64(this.hash1.Sum32())<<32 | uint64(this.hash2.Sum32())
}

func (this *sHash) Write(data []byte) {
	for _, c := range data {
		this.hash1.WriteBype(c)
		this.hash2.WriteBype(c)
	}
}

func (this *sHash) WriteBype(data byte) {
	this.hash1.WriteBype(data)
	this.hash2.WriteBype(data)
}

func (this *sHash) WriteUInt32(data uint32) {
	this.hash1.WriteUInt32(data)
	this.hash2.WriteUInt32(data)
}

func newSHash() *sHash {
	return &sHash{newBKDR32(), newDJB32()}
}

//sort util func-------------------------------------------------------------------------------------------
type sortable struct {
	values []interface{}
	less   func(this, that interface{}) bool
}

func (q sortable) Len() int           { return len(q.values) }
func (q sortable) Swap(i, j int)      { q.values[i], q.values[j] = q.values[j], q.values[i] }
func (q sortable) Less(i, j int) bool { return q.less(q.values[i], q.values[j]) }

func sortSlice(data []interface{}, less func(interface{}, interface{}) bool) []interface{} {
	sortable := sortable{}
	sortable.less = less
	sortable.values = make([]interface{}, len(data))
	_ = copy(sortable.values, data)
	sort.Sort(sortable)
	return sortable.values
}

//AVL----------------------------------------------------
type avlNode struct {
	data           interface{}
	sameList       []interface{}
	bf             int
	lchild, rchild *avlNode
}

func rRotate(node **avlNode) {
	l := (*node).lchild
	(*node).lchild = l.rchild
	l.rchild = *node
	*node = l
}

func lRotate(node **avlNode) {
	r := (*node).rchild
	(*node).rchild = r.lchild
	r.lchild = *node
	*node = r
}

const (
	avl_LH int = 1
	avl_EH     = 0
	avl_RH     = -1
)

func lBalance(root **avlNode) {
	var lr *avlNode
	l := (*root).lchild
	switch l.bf {
	case avl_LH:
		(*root).bf = avl_EH
		l.bf = avl_EH
		rRotate(root)
	case avl_RH:
		lr = l.rchild
		switch lr.bf {
		case avl_LH:
			(*root).bf = avl_RH
			l.bf = avl_EH
		case avl_EH:
			(*root).bf = avl_EH
			l.bf = avl_EH
		case avl_RH:
			(*root).bf = avl_EH
			l.bf = avl_LH
		}
		lr.bf = avl_EH
		lRotate(&((*root).lchild))
		rRotate(root)
	}
}

func rBalance(root **avlNode) {
	var rl *avlNode
	r := (*root).rchild
	switch r.bf {
	case avl_RH:
		(*root).bf = avl_EH
		r.bf = avl_EH
		lRotate(root)
	case avl_LH:
		rl = r.lchild
		switch rl.bf {
		case avl_LH:
			(*root).bf = avl_RH
			r.bf = avl_EH
		case avl_EH:
			(*root).bf = avl_EH
			r.bf = avl_EH
		case avl_RH:
			(*root).bf = avl_EH
			r.bf = avl_LH
		}
		rl.bf = avl_EH
		//pRchild := (avlTree)((*root).rchild)
		rRotate(&((*root).rchild))
		lRotate(root)
	}
}

func insertAVL(root **avlNode, e interface{}, taller *bool, compare1 func(interface{}, interface{}) int) bool {
	if *root == nil {
		node := avlNode{e, nil, avl_EH, nil, nil}
		*root = &node
		*taller = true
	} else {
		i := compare1(e, (*root).data)
		if e == (*root).data || i == 0 {
			if (*root).sameList == nil {
				(*root).sameList = make([]interface{}, 0, 2)
			}

			(*root).sameList = appendToSlice((*root).sameList, e)
			return false
		}

		if i == -1 {
			if !insertAVL(&((*root).lchild), e, taller, compare1) {
				return false
			}

			if *taller {
				switch (*root).bf {
				case avl_LH:
					lBalance(root)
					*taller = false
				case avl_EH:
					(*root).bf = avl_LH
					*taller = true
				case avl_RH:
					(*root).bf = avl_EH
					*taller = false
				}
			}
		} else if i == 1 {
			if !insertAVL(&((*root).rchild), e, taller, compare1) {
				return false
			}

			if *taller {
				switch (*root).bf {
				case avl_RH:
					rBalance(root)
					*taller = false
				case avl_EH:
					(*root).bf = avl_RH
					*taller = true
				case avl_LH:
					(*root).bf = avl_EH
					*taller = false
				}
			}
		}
	}
	return true
}

type avlTree struct {
	root    *avlNode
	count   int
	compare func(a interface{}, b interface{}) int
}

func (this *avlTree) Insert(node interface{}) {
	var taller bool
	insertAVL(&(this.root), node, &taller, this.compare)
	this.count++
}

func (this *avlTree) ToSlice() []interface{} {
	result := (make([]interface{}, 0, this.count))
	avlToSlice(this.root, &result)
	return result
}

func avlToSlice(root *avlNode, result *[]interface{}) []interface{} {
	if result == nil {
		panic(errors.New("avlToSlice, result must be not nil"))
		//r := make([]interface{}, 0, 10)
		//result = &r
	}

	if root == nil {
		return *result
	}

	if (root).lchild != nil {
		l := root.lchild
		avlToSlice(l, result)
	}
	*result = append(*result, root.data)
	if root.sameList != nil {
		for _, v := range root.sameList {
			*result = append(*result, v)
		}
	}
	if (root).rchild != nil {
		r := (root.rchild)
		avlToSlice(r, result)
	}
	return *result
}

func newAvlTree(compare func(a interface{}, b interface{}) int) *avlTree {
	return &avlTree{nil, 0, compare}
}

func newChunkAvlTree() *avlTree {
	return newAvlTree(func(a interface{}, b interface{}) int {
		c1, c2 := a.(*Chunk), b.(*Chunk)
		if c1.Order < c2.Order {
			return -1
		} else if c1.Order == c2.Order {
			return 0
		} else {
			return 1
		}
	})
}

type chunkOrderedList struct {
	list     []interface{}
	count    int
	maxOrder int
}

func (this *chunkOrderedList) Insert(node interface{}) {
	//fmt.Println("\ninsert chunk", node, len(this.list))
	order := node.(*Chunk).Order
	//某些情况下Order会重复，比如Union的第二个数据源的Order会和第一个重复
	if order < len(this.list) && this.list[order] != nil {
		order = this.maxOrder + 1
	}

	if order > this.maxOrder {
		this.maxOrder = order
	}

	if len(this.list) > order {
		this.list[order] = node
	} else {
		newList := make([]interface{}, order+1, max(2*len(this.list), order+1))
		_ = copy(newList[0:len(this.list)], this.list)
		newList[order] = node
		this.list = newList
	}
	this.count++
	//fmt.Println("after insert chunk", this.maxOrder, this.count, this.list)
}

func (this *chunkOrderedList) ToSlice() []interface{} {
	//if the error appears, this.list may includes nil
	return this.list[0 : this.maxOrder+1]
}

func newChunkOrderedList() *chunkOrderedList {
	return &chunkOrderedList{make([]interface{}, 0, 0), 0, -1}
}

//error handling functions------------------------------------
type stringer interface {
	String() string
}

func getError(i interface{}) (e error) {
	if i != nil {
		switch v := i.(type) {
		case error:
			e = v
		case stringer:
			e = errors.New(v.String())
		default:
			e = errors.New("unknow error")
		}
	}
	return
}

// NewAggregateError returns an error that formats as the given text and includes the given inner errors.
func NewAggregateError(text string, err interface{}) *AggregateError {
	if aggErr, ok := err.(*promise.AggregateError); ok {
		return &AggregateError{text, aggErr.InnerErrs}
	} else if errs, ok := err.([]interface{}); ok {
		errs1 := make([]error, len(errs))
		for i, e := range errs {
			//fmt.Println("get Aggregate errors2", e)
			errs1[i] = errors.New(fmt.Sprintf("%v", e))
		}
		return &AggregateError{text, errs1}
	} else if errs, ok := err.([]error); ok {
		return &AggregateError{text, errs}
	} else if e, ok := err.(error); ok {
		return &AggregateError{text, []error{e}}
	} else {
		panic(errors.New("unsupport error type"))
	}
}

// Represents one or more errors that occur during query execution.
type AggregateError struct {
	s         string
	innerErrs []error
}

func (e *AggregateError) Error() string {
	if e.innerErrs == nil {
		return e.s
	} else {
		var str string
		str += e.s + "\n"
		for _, ie := range e.innerErrs {
			if ie == nil {
				continue
			}
			if se, ok := ie.(error); ok {
				str += se.Error() + "\n"
			} else {
				str += fmt.Sprintf("%v", ie) + "\n"
			}
		}
		return str
	}
}

type StepErr struct {
	stepIdx int
	stepTyp int
	errs    []interface{}
}

func (e *StepErr) Error() string {
	buf := bytes.NewBufferString("error appears in ")
	buf.WriteString(stepTypToString(e.stepTyp))
	buf.WriteString(":\n")

	for _, err := range e.errs {
		buf.WriteString(fmt.Sprintf("%v", err))
		buf.WriteString("\n")
	}
	return buf.String()
}

func stepTypToString(typ int) string {
	switch typ {
	case ACT_SELECT:
		return "Select opretion"
	case ACT_SELECTMANY:
		return "SelectMany opretion"
	case ACT_WHERE:
		return "Where opretion"
	case ACT_GROUPBY:
		return "GroupBy opretion"
	case ACT_HGROUPBY:
		return "HGroupBy opretion"
	case ACT_ORDERBY:
		return "OrderBy opretion"
	case ACT_DISTINCT:
		return "Distinct opretion"
	case ACT_JOIN:
		return "Join opretion"
	case ACT_GROUPJOIN:
		return "GroupJoin opretion"
	case ACT_UNION:
		return "Union opretion"
	case ACT_CONCAT:
		return "Concat opretion"
	case ACT_INTERSECT:
		return "Intersect opretion"
	case ACT_REVERSE:
		return "Reverse opretion"
	case ACT_EXCEPT:
		return "Except opretion"
	case ACT_AGGREGATE:
		return "Aggregate opretion"
	case ACT_SKIP:
		return "Skip opretion"
	case ACT_SKIPWHILE:
		return "SkipWhile opretion"
	case ACT_TAKE:
		return "Take opretion"
	case ACT_TAKEWHILE:
		return "TakeWhile opretion"
	case ACT_ELEMENTAT:
		return "ElementAt opretion"
	case ACT_SINGLEVALUE:
		return "Single value opretion"
	default:
		return "unknown opretion" + strconv.Itoa(typ)
	}

}

// NewAggregateError returns an error that formats as the given text and includes the given inner errors.
func NewStepError(stepIdx int, stepTyp int, innerErrs interface{}) *StepErr {
	if ies, ok := innerErrs.([]interface{}); ok {
		rs := make([]interface{}, 0, len(ies))
		if len(ies) > 0 {
			//if _, ok := ies[0].(promise.PromiseResult); ok {
			for i, r := range ies {
				pr := r.(promise.PromiseResult)
				if pr.Typ != promise.RESULT_SUCCESS {
					rs = append(rs,
						strings.Join([]string{"error appears in Future ",
							strconv.Itoa(i), ":",
							fmt.Sprintf("%v", pr.Result)}, ""))
				}
			}
			//} else {
			rs = ies
			//}
		}
		return &StepErr{stepIdx, stepTyp, rs}
	} else if err := innerErrs.(error); err != nil {
		return &StepErr{stepIdx, stepTyp, []interface{}{innerErrs}}
	} else {
		return nil
	}
}

func newErrorWithStacks(i interface{}) (e error) {
	err := getError(i)
	buf := bytes.NewBufferString(err.Error())
	buf.WriteString("\n")

	pcs := make([]uintptr, 10)
	num := runtime.Callers(2, pcs)
	for _, v := range pcs[0:num] {
		fun := runtime.FuncForPC(v)
		file, line := fun.FileLine(v)
		name := fun.Name()
		//fmt.Println(name, file + ":", line)
		writeStrings(buf, []string{name, " ", file, ":", strconv.Itoa(line), "\n"})
	}
	return errors.New(buf.String())
}

func writeStrings(buf *bytes.Buffer, strings []string) {
	for _, s := range strings {
		buf.WriteString(s)
	}
}

//Asset function--------------------------------------
func mustNotNil(v interface{}, err error) {
	if isNil(v) {
		panic(err)
	}
}

//func isNil(v interface{}) bool {
//	if v == nil {
//		return true
//	}
//	if val := reflect.ValueOf(v); val.Kind() == reflect.Chan ||
//		val.Kind() == reflect.Ptr || val.Kind() == reflect.Slice ||
//		val.Kind() == reflect.Func || val.Kind() == reflect.Interface {
//		if val.IsNil() {
//			return true
//		}
//	}
//	return false
//}

//aggregate functions---------------------------------------------------------------
func sumIntOpr(v interface{}, t interface{}) interface{} {
	//if isNil(t) {
	//	return v
	//}

	return v.(int) + t.(int)
}

func sumOpr(v interface{}, t interface{}) interface{} {
	//if isNil(t) {
	//	return toFloat64(v)
	//}

	return toFloat64(v) + t.(float64) //toFloat64(t)
}

func countOpr(v interface{}, t interface{}) interface{} {
	return t.(int) + 1
}

func minOpr(v interface{}, t interface{}, less func(interface{}, interface{}) bool) interface{} {
	if isNil(t) {
		return v
	}
	if less(v, t) {
		return v
	} else {
		return t
	}
}

func maxOpr(v interface{}, t interface{}, less func(interface{}, interface{}) bool) interface{} {
	if isNil(t) {
		return v
	}
	if less(v, t) {
		return t
	} else {
		return v
	}
}

func getAggByFunc(oriaAggFunc TwoArgsFunc, convert OneArgsFunc) TwoArgsFunc {
	fun := oriaAggFunc
	if convert != nil {
		fun = func(a interface{}, b interface{}) interface{} {
			return oriaAggFunc(convert(a), b)
		}
	}
	return fun
}

func getSumOpr(convert OneArgsFunc) *AggregateOperation {
	return &AggregateOperation{float64(0), getAggByFunc(sumOpr, convert), sumOpr}
}

func getMinOpr(less func(interface{}, interface{}) bool, convert OneArgsFunc) *AggregateOperation {
	fun := func(a interface{}, b interface{}) interface{} {
		return minOpr(a, b, less)
	}
	return &AggregateOperation{nil, getAggByFunc(fun, convert), fun}
}

func getMaxOpr(less func(interface{}, interface{}) bool, convert OneArgsFunc) *AggregateOperation {
	fun := func(a interface{}, b interface{}) interface{} {
		return maxOpr(a, b, less)
	}
	return &AggregateOperation{nil, getAggByFunc(fun, convert), fun}
}

func getCountByOpr(predicate PredicateFunc) *AggregateOperation {
	fun := countOpr
	if predicate != nil {
		fun = func(v interface{}, t interface{}) interface{} {
			if predicate(v) {
				t = countOpr(v, t)
			}
			return t
		}
	}
	return &AggregateOperation{0, fun, sumIntOpr}
}

func defLess(a interface{}, b interface{}) bool {
	return defCompare(a, b) == -1
}

func defCompare(a interface{}, b interface{}) int {
	switch val := a.(type) {
	case int:
		if val < b.(int) {
			return -1
		} else if val == b.(int) {
			return 0
		} else {
			return 1
		}
	case int8:
		if val < b.(int8) {
			return -1
		} else if val == b.(int8) {
			return 0
		} else {
			return 1
		}
	case int16:
		if val < b.(int16) {
			return -1
		} else if val == b.(int16) {
			return 0
		} else {
			return 1
		}
	case int32:
		if val < b.(int32) {
			return -1
		} else if val == b.(int32) {
			return 0
		} else {
			return 1
		}
	case int64:
		if val < b.(int64) {
			return -1
		} else if val == b.(int64) {
			return 0
		} else {
			return 1
		}
	case uint:
		if val < b.(uint) {
			return -1
		} else if val == b.(uint) {
			return 0
		} else {
			return 1
		}
	case uint8:
		if val < b.(uint8) {
			return -1
		} else if val == b.(uint8) {
			return 0
		} else {
			return 1
		}
	case uint16:
		if val < b.(uint16) {
			return -1
		} else if val == b.(uint16) {
			return 0
		} else {
			return 1
		}
	case uint32:
		if val < b.(uint32) {
			return -1
		} else if val == b.(uint32) {
			return 0
		} else {
			return 1
		}
	case uint64:
		if val < b.(uint64) {
			return -1
		} else if val == b.(uint64) {
			return 0
		} else {
			return 1
		}
	case float32:
		if val < b.(float32) {
			return -1
		} else if val == b.(float32) {
			return 0
		} else {
			return 1
		}
	case float64:
		if val < b.(float64) {
			return -1
		} else if val == b.(float64) {
			return 0
		} else {
			return 1
		}
	case string:
		if val < b.(string) {
			return -1
		} else if val == b.(string) {
			return 0
		} else {
			return 1
		}
	case time.Time:
		if val.Before(b.(time.Time)) {
			return -1
		} else if val.After(b.(time.Time)) {
			return 1
		} else {
			return 0
		}
	default:
		panic(errors.New(fmt.Sprintf("Cannot compare %v %v", a, b))) //reflect.NewAt(t, ptr).Elem().Interface()
	}
}

func divide(a interface{}, count float64) (r float64) {
	return toFloat64(a) / count
}

func toFloat64(v interface{}) (r float64) {
	switch val := v.(type) {
	case int:
		r = float64(val)
	case uint:
		r = float64(val)
	case float32:
		r = float64(val)
	case float64:
		r = val
	case int8:
		r = float64(val)
	case int16:
		r = float64(val)
	case int32:
		r = float64(val)
	case int64:
		r = float64(val)
	case uint8:
		r = float64(val)
	case uint16:
		r = float64(val)
	case uint32:
		r = float64(val)
	case uint64:
		r = float64(val)
	default:
		panic(errors.New(fmt.Sprintf("Cannot convert %v to float64", v))) //reflect.NewAt(t, ptr).Elem().Interface()
	}
	return
}

//test a value can be used as key in map
func testCanAsKey(v interface{}) (ok bool) {
	defer func() {
		if e := recover(); e != nil {
			ok = false
		}
	}()
	m := make(map[interface{}]bool, 1)
	m[v] = true
	ok = true
	return
}

func testCanUseDefaultHash(src, src2 DataSource) bool {
	if src.Typ() == SOURCE_CHANNEL && src2.Typ() == SOURCE_CHANNEL {
		slicer1 := src.ToSlice(false)
		if slicer1.Len() > 0 {
			if testCanAsKey(slicer1.Index(0)) {
				return true
			}
		}

		slicer2 := src2.ToSlice(false)
		if slicer2.Len() > 0 {
			if testCanAsKey(slicer2.Index(0)) {
				return true
			}
		}
	}
	return false
}

// compare functions------------------------------------------------

func equals(a interface{}, b interface{}) bool {
	return checkEquals(a, b)
}

// equals tests for equality. It uses normal == equality where
// possible but will scan elements of arrays, slices, maps, and fields of
// structs. In maps, keys are compared with == but elements use deep
// equality. DeepEqual correctly handles recursive types. Functions are equal
// only if they are pointer to one function.
// An empty slice is not equal to a nil slice.
// A pointer with nil value is equal to nil
func checkEquals(a interface{}, b interface{}) bool {

	//fmt.Printf("interface layout is %v %v\n", faceToStruct(a), faceToStruct(b))
	v1, v2 := reflect.ValueOf(a), reflect.ValueOf(b)
	addr1, _ := dataPtr(a)
	addr2, _ := dataPtr(b)
	aIsNil, bIsNil := isNil(a), isNil(b)
	if aIsNil || bIsNil {
		//fmt.Printf("isnil is %v %v %v %v\n", aIsNil, bIsNil, a, b)
		return aIsNil == bIsNil
	}

	if v1.Type() != v2.Type() {
		//fmt.Println("type isnot same", a, b, v1.Type(), v2.Type())
		return false
	}

	switch k := v1.Type().Kind(); k {
	case reflect.Map, reflect.Slice:
		return bytesEquals(uintptr(addr1), uintptr(addr2), v1.Type().Size())
	case reflect.Func:
		return addr1 == addr2

	case reflect.Struct:
		if v1.Type().Size() > ptrSize {
			return bytesEquals(uintptr(addr1), uintptr(addr2), v1.Type().Size())
		} else {
			return addr1 == addr2
		}

	default:
		return a == b
	}
}

func bytesEquals(addr1 uintptr, addr2 uintptr, size uintptr) bool {
	for i := 0; uintptr(i) < size; i++ {
		if *((*byte)(unsafe.Pointer(addr1 + uintptr(i)))) != *((*byte)(unsafe.Pointer(addr2 + uintptr(i)))) {
			return false
		}
	}
	return true
}

//func Equals(a, b interface{}) bool {
//	return equals(a, b)
//}
