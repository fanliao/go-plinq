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
	"time"
	"unsafe"
)

const (
	ptrSize        = unsafe.Sizeof((*byte)(nil))
	kindMask       = 0x7f
	kindNoPointers = 0x80
)

//hash an object-----------------------------------------------
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
			//如果是非指针类型并且数据size小于等于一个字，则interface的word是数据本身
			ptr = unsafe.Pointer(&(headerPtr.word))
		}
	}
	return
}

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

func hash64(data interface{}) uint64 {
	dataPtr, size := dataPtr(data)
	sh := NewSHash()
	hashByPtr(dataPtr, size, sh)
	//fmt.Println("hash", data, bkdr.Sum32(), djb.Sum32(), uint64(bkdr.Sum32())<<32|uint64(djb.Sum32()), "\n")
	return sh.Sum64()
}

const (
	BKDR32seed   = 131
	DJB32prime32 = 5381
)

// NewBKDR32 returns a new 32-bit BKDR hash
func NewBKDR32() shash32 {
	var s BKDR32 = 0
	return &s
}

// NewBKDR32 returns a new 32-bit BKDR hash
func NewDJB32() shash32 {
	var s DJB32 = DJB32prime32
	return &s
}

type shash32 interface {
	Sum32() uint32
	Write(data []byte)
	WriteBype(data byte)
	WriteUInt32(data uint32)
}

type (
	BKDR32 uint32
	DJB32  uint32
)

func (s *BKDR32) Sum32() uint32 { return uint32(*s) }
func (s *DJB32) Sum32() uint32  { return uint32(*s) }

func (s *BKDR32) Write(data []byte) {
	hash := *s
	for _, c := range data {
		hash = hash*BKDR32seed + BKDR32(c)
	}
	*s = hash
}

func (s *DJB32) Write(data []byte) {
	hash := *s
	for _, c := range data {
		hash = ((hash << 5) + hash) + DJB32(c)
	}
	*s = hash
}

func (s *BKDR32) WriteBype(data byte) {
	hash := *s
	hash = hash*BKDR32seed + BKDR32(data)
	*s = hash
}

func (s *DJB32) WriteBype(data byte) {
	hash := *s
	hash = ((hash << 5) + hash) + DJB32(data)
	*s = hash
}

func (s *BKDR32) WriteUInt32(data uint32) {
	hash := *s
	hash = hash*BKDR32seed + BKDR32(data)
	*s = hash
}

func (s *DJB32) WriteUInt32(data uint32) {
	hash := *s
	hash = ((hash << 5) + hash) + DJB32(data)
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

func NewSHash() *sHash {
	return &sHash{NewBKDR32(), NewDJB32()}
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
	LH int = 1
	EH     = 0
	RH     = -1
)

func lBalance(root **avlNode) {
	var lr *avlNode
	l := (*root).lchild
	switch l.bf {
	case LH:
		(*root).bf = EH
		l.bf = EH
		rRotate(root)
	case RH:
		lr = l.rchild
		switch lr.bf {
		case LH:
			(*root).bf = RH
			l.bf = EH
		case EH:
			(*root).bf = EH
			l.bf = EH
		case RH:
			(*root).bf = EH
			l.bf = LH
		}
		lr.bf = EH
		lRotate(&((*root).lchild))
		rRotate(root)
	}
}

func rBalance(root **avlNode) {
	var rl *avlNode
	r := (*root).rchild
	switch r.bf {
	case RH:
		(*root).bf = EH
		r.bf = EH
		lRotate(root)
	case LH:
		rl = r.lchild
		switch rl.bf {
		case LH:
			(*root).bf = RH
			r.bf = EH
		case EH:
			(*root).bf = EH
			r.bf = EH
		case RH:
			(*root).bf = EH
			r.bf = LH
		}
		rl.bf = EH
		//pRchild := (avlTree)((*root).rchild)
		rRotate(&((*root).rchild))
		lRotate(root)
	}
}

func InsertAVL(root **avlNode, e interface{}, taller *bool, compare1 func(interface{}, interface{}) int) bool {
	if *root == nil {
		node := avlNode{e, nil, EH, nil, nil}
		*root = &node
		*taller = true
	} else {
		i := compare1(e, (*root).data)
		if e == (*root).data || i == 0 {
			if (*root).sameList == nil {
				(*root).sameList = make([]interface{}, 0, 2)
			}

			(*root).sameList = appendSlice((*root).sameList, e)
			return false
		}

		if i == -1 {
			if !InsertAVL(&((*root).lchild), e, taller, compare1) {
				return false
			}

			if *taller {
				switch (*root).bf {
				case LH:
					lBalance(root)
					*taller = false
				case EH:
					(*root).bf = LH
					*taller = true
				case RH:
					(*root).bf = EH
					*taller = false
				}
			}
		} else if i == 1 {
			if !InsertAVL(&((*root).rchild), e, taller, compare1) {
				return false
			}

			if *taller {
				switch (*root).bf {
				case RH:
					rBalance(root)
					*taller = false
				case EH:
					(*root).bf = RH
					*taller = true
				case LH:
					(*root).bf = EH
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
	InsertAVL(&(this.root), node, &taller, this.compare)
	this.count++
}

func (this *avlTree) ToSlice() []interface{} {
	result := (make([]interface{}, 0, this.count))
	avlToSlice(this.root, &result)
	return result
}

func avlToSlice(root *avlNode, result *[]interface{}) []interface{} {
	if result == nil {
		r := make([]interface{}, 0, 10)
		result = &r
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

func NewAvlTree(compare func(a interface{}, b interface{}) int) *avlTree {
	return &avlTree{nil, 0, compare}
}

func newChunkAvlTree() *avlTree {
	return NewAvlTree(func(a interface{}, b interface{}) int {
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

// NewLinqError returns an error that formats as the given text and includes the given inner errors.
func NewLinqError(text string, err interface{}) error {
	if aggErr, ok := err.(*promise.AggregateError); ok {
		//errs := make([]interface{}, len(aggErr.InnerErrs))
		//for i, e := range aggErr.InnerErrs {
		//	errs[i] = e
		//}
		return &errorLinq{text, aggErr.InnerErrs}
	} else if errs, ok := err.([]interface{}); ok {
		errs1 := make([]error, len(errs))
		for i, e := range errs {
			//fmt.Println("get Aggregate errors2", e)
			errs1[i] = errors.New(fmt.Sprintf("%v", e))
		}
		return &errorLinq{text, errs1}
	} else if errs, ok := err.([]error); ok {
		//v := reflect.ValueOf(err)
		//fmt.Println("\nget Aggregate errors3", len(errs))
		//for i := 0; i < v.Len(); i++ {
		//	se := v.Index(i).Interface().(stepErr)
		//	fmt.Println("item", i, "=", (&se).Error())
		//}

		return &errorLinq{text, errs}
	} else if e, ok := err.(error); ok {
		//v := reflect.ValueOf(err)
		//fmt.Println("\nget Aggregate errors3", len(errs))
		//for i := 0; i < v.Len(); i++ {
		//	se := v.Index(i).Interface().(stepErr)
		//	fmt.Println("item", i, "=", (&se).Error())
		//}

		return &errorLinq{text, []error{e}}
	} else {
		panic(errors.New("unsupport error type"))
	}
}

// errorLinq is a trivial implementation of error.
type errorLinq struct {
	s         string
	innerErrs []error
}

func (e *errorLinq) Error() string {
	if e.innerErrs == nil {
		return e.s
	} else {
		var str string
		str += e.s + "\n"
		for _, ie := range e.innerErrs {
			if se, ok := ie.(error); ok {
				str += se.Error() + "\n"
			} else {
				str += fmt.Sprintf("%v", ie) + "\n"
			}
		}
		return str
	}
}

type stepErr struct {
	stepIdx int
	stepTyp int
	errs    []interface{}
}

func (e *stepErr) Error() string {
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
		return "SELECT opretion"
	case ACT_WHERE:
		return "WHERE opretion"
	case ACT_GROUPBY:
		return "GROUPBY opretion"
	case ACT_HGROUPBY:
		return "HGROUPBY opretion"
	case ACT_ORDERBY:
		return "ORDERBY opretion"
	case ACT_DISTINCT:
		return "DISTINCT opretion"
	case ACT_JOIN:
		return "JOIN opretion"
	case ACT_GROUPJOIN:
		return "GROUPJOIN opretion"
	case ACT_UNION:
		return "UNION opretion"
	case ACT_CONCAT:
		return "CONCAT opretion"
	case ACT_INTERSECT:
		return "INTERSECT opretion"
	case ACT_SKIP:
		return "Skip opretion"
	case ACT_SKIPWHILE:
		return "SkipWhile opretion"
	case ACT_TAKE:
		return "Take opretion"
	case ACT_TAKEWHILE:
		return "TakeWhile opretion"
	default:
		return "unknown opretion" + strconv.Itoa(typ)
	}

}

// NewLinqError returns an error that formats as the given text and includes the given inner errors.
func NewStepError(stepIdx int, stepTyp int, innerErrs interface{}) *stepErr {
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
		return &stepErr{stepIdx, stepTyp, rs}
	} else if err := innerErrs.(error); err != nil {
		return &stepErr{stepIdx, stepTyp, []interface{}{innerErrs}}
	} else {
		return nil
	}
}

func newErrorWithStacks(i interface{}) (e error) {
	err := getError(i)
	buf := bytes.NewBufferString(err.Error())
	buf.WriteString("\n")

	pcs := make([]uintptr, 50)
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

func isNil(v interface{}) bool {
	if v == nil {
		return true
	}
	if val := reflect.ValueOf(v); val.Kind() == reflect.Chan ||
		val.Kind() == reflect.Ptr || val.Kind() == reflect.Slice ||
		val.Kind() == reflect.Func || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return true
		}
	}
	return false
}

//aggregate functions---------------------------------------------------------------
func sumOpr(v interface{}, t interface{}) interface{} {
	switch val := v.(type) {
	case int:
		return val + t.(int)
	case int8:
		return val + t.(int8)
	case int16:
		return val + t.(int16)
	case int32:
		return val + t.(int32)
	case int64:
		return val + t.(int64)
	case uint:
		return val + t.(uint)
	case uint8:
		return val + t.(uint8)
	case uint16:
		return val + t.(uint16)
	case uint32:
		return val + t.(uint32)
	case uint64:
		return val + t.(uint64)
	case float32:
		return val + t.(float32)
	case float64:
		return val + t.(float64)
	case string:
		return val + t.(string)
	default:
		panic(errors.New("unsupport aggregate type")) //reflect.NewAt(t, ptr).Elem().Interface()
	}
}

func countOpr(v interface{}, t interface{}) interface{} {
	return t.(int) + 1
}

func minOpr(v interface{}, t interface{}, less func(interface{}, interface{}) bool) interface{} {
	if t == nil {
		return v
	}
	if less(v, t) {
		return v
	} else {
		return t
	}
}

func maxOpr(v interface{}, t interface{}, less func(interface{}, interface{}) bool) interface{} {
	if t == nil {
		return v
	}
	if less(v, t) {
		return t
	} else {
		return v
	}
}

func getMinOpr(less func(interface{}, interface{}) bool) *AggregateOpretion {
	fun := func(a interface{}, b interface{}) interface{} {
		return minOpr(a, b, less)
	}
	return &AggregateOpretion{0, fun, fun}
}

func getMaxOpr(less func(interface{}, interface{}) bool) *AggregateOpretion {
	fun := func(a interface{}, b interface{}) interface{} {
		return maxOpr(a, b, less)
	}
	return &AggregateOpretion{0, fun, fun}
}

func getCountByOpr(predicate predicateFunc) *AggregateOpretion {
	fun := func(v interface{}, t interface{}) interface{} {
		if predicate(v) {
			t = t.(int) + 1
		}
		return t
	}
	return &AggregateOpretion{0, fun, sumOpr}
}

func defLess(a interface{}, b interface{}) bool {
	switch val := a.(type) {
	case int:
		return val < b.(int)
	case int8:
		return val < b.(int8)
	case int16:
		return val < b.(int16)
	case int32:
		return val < b.(int32)
	case int64:
		return val < b.(int64)
	case uint:
		return val < b.(uint)
	case uint8:
		return val < b.(uint8)
	case uint16:
		return val < b.(uint16)
	case uint32:
		return val < b.(uint32)
	case uint64:
		return val < b.(uint64)
	case float32:
		return val < b.(float32)
	case float64:
		return val < b.(float64)
	case string:
		return val < b.(string)
	case time.Time:
		return val.Before(b.(time.Time))
	default:
		panic(errors.New("unsupport aggregate type")) //reflect.NewAt(t, ptr).Elem().Interface()
	}
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
		panic(errors.New("unsupport aggregate type")) //reflect.NewAt(t, ptr).Elem().Interface()
	}
}

func divide(a interface{}, count float64) (r float64) {
	switch val := a.(type) {
	case int:
		r = float64(val) / count
	case int8:
		r = float64(val) / count
	case int16:
		r = float64(val) / count
	case int32:
		r = float64(val) / count
	case int64:
		r = float64(val) / count
	case uint:
		r = float64(val) / count
	case uint8:
		r = float64(val) / count
	case uint16:
		r = float64(val) / count
	case uint32:
		r = float64(val) / count
	case uint64:
		r = float64(val) / count
	case float32:
		r = float64(val) / count
	case float64:
		r = float64(val) / count
	default:
		panic(errors.New("unsupport aggregate type")) //reflect.NewAt(t, ptr).Elem().Interface()
	}
	return
}
