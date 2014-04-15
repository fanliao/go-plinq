package main

import (
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"sync"
	//"time"
	"unsafe"
)

const (
	ptrSize          = unsafe.Sizeof((*byte)(nil))
	kindMask         = 0x7f
	kindNoPointers   = 0x80
	DEFAULTCHUNKSIZE = 20
)

var (
	numCPU               int
	ErrUnsupportSource   = errors.New("unsupport DataSource")
	ErrNilSource         = errors.New("datasource cannot be nil")
	ErrUnionNilSource    = errors.New("cannot union nil data source")
	ErrConcatNilSource   = errors.New("cannot concat nil data source")
	ErrInterestNilSource = errors.New("cannot interest nil data source")
	ErrJoinNilSource     = errors.New("cannot join nil data source")
	ErrNilAction         = errors.New("action cannot be nil")
	ErrOuterKeySelector  = errors.New("outerKeySelector cannot be nil")
	ErrInnerKeySelector  = errors.New("innerKeySelector cannot be nil")
	ErrResultSelector    = errors.New("resultSelector cannot be nil")
	ErrTaskFailure       = errors.New("ErrTaskFailure")
)

func init() {
	numCPU = runtime.NumCPU()
	fmt.Println("ptrSize", ptrSize)
}

// the struct and interface about data DataSource---------------------------------------------------
// A Chunk presents a data chunk, the paralleliam algorithm always handle a chunk in a paralleliam tasks.
type Chunk struct {
	data  []interface{}
	order int
}

const (
	SOURCE_BLOCK int = iota
	SOURCE_CHUNK
)

type DataSource interface {
	Typ() int //block or chan?
	ToSlice(bool) []interface{}
	//ToChan() chan interface{}
}

// KeyValue presents a key value pair, it be used by GroupBy operations
type KeyValue struct {
	key   interface{}
	value interface{}
}

//the queryable struct-------------------------------------------------------------------------
// A ParallelOption presents the options of the paralleliam algorithm.
type ParallelOption struct {
	degree    int  //The degree of the paralleliam algorithm
	chunkSize int  //The size of chunk
	keepOrder bool //whether need keep order of original data
}

// A Queryable presents an object includes the data and query operations
// All query functions will return Queryable.
// For getting the results of the query, use Results().
type Queryable struct {
	data     DataSource
	steps    []step
	stepErrs []interface{}
	errChan  chan *stepErr
	ParallelOption
}

// From initializes a linq query with passed slice, map or channel as the data source.
// input parameter must be a slice, map or channel. Otherwise panics ErrUnsupportSource
//
// Example:
//     i1 := []int{1,2,3,4,5,6}
//     q := From(i)
//     i2 := []interface{}{1,2,3,4,5,6}
//     q := From(i)
//
//     c1 := chan string
//     q := From(c1)
//     c2 := chan interface{}
//     q := From(c2)
//
//	   Todo: need to test map
//
// Note: if the source is a channel, the channel must be closed by caller of linq,
// otherwise will be deadlock
func From(src interface{}) (q *Queryable) {
	if src == nil {
		panic(ErrNilSource)
	}

	q = &Queryable{}
	q.keepOrder = true
	q.steps = make([]step, 0, 4)
	q.degree = numCPU
	q.chunkSize = DEFAULTCHUNKSIZE
	q.errChan = make(chan *stepErr)

	if k := reflect.ValueOf(src).Kind(); k == reflect.Slice || k == reflect.Map {
		q.data = &listSource{data: src}
	} else if s, ok := src.(chan *Chunk); ok {
		q.data = &chanSource{chunkChan: s}
	} else if k == reflect.Chan {
		q.data = &chanSource{new(sync.Once), src, nil}
	} else {
		panic(ErrUnsupportSource)
	}
	return
}

// Results evaluates the query and returns the results as interface{} slice.
// An error occurred in during evaluation of the query will be returned.
//
// Example:
// 	results, err := From([]interface{}{"Jack", "Rock"}).Select(something).Results()
func (this *Queryable) Results() (results []interface{}, err error) {
	if ds, e := this.get(); e == nil {
		results = ds.ToSlice(this.keepOrder)
		if len(this.stepErrs) > 0 {
			err = NewLinqError("Aggregate errors", this.stepErrs)
		}
		if this.errChan != nil {
			close(this.errChan)
		}
		return
	} else {
		return nil, e
	}
}

// Where returns a query includes the Where operation
// Where operation filters a sequence of values based on a predicate function.
//
// Example:
// 	q := From(users).Where(func (v interface{}) bool{
//		return v.(*User).Age > 18
// 	})
func (this *Queryable) Where(sure func(interface{}) bool, degrees ...int) *Queryable {
	if sure == nil {
		panic(ErrNilAction)
	}

	this.steps = append(this.steps, commonStep{ACT_WHERE, sure, getDegreeArg(degrees...)})
	return this
}

// Select returns a query includes the Select operation
// Select operation projects each element of the data source into a new data source.
// with invoking the transform function on each element of original source.
//
// Example:
// 	q := From(users).Select(func (v interface{}) interface{}{
//		return v.(*User).Name
// 	})
func (this *Queryable) Select(selectFunc func(interface{}) interface{}, degrees ...int) *Queryable {
	if selectFunc == nil {
		panic(ErrNilAction)
	}
	this.steps = append(this.steps, commonStep{ACT_SELECT, selectFunc, getDegreeArg(degrees...)})
	return this
}

// Distinct returns a query includes the Distinct operation
// Distinct operation distinct elements from the data source.
//
// Example:
// 	q := From(users).Distinct()
func (this *Queryable) Distinct(degrees ...int) *Queryable {
	return this.DistinctBy(func(v interface{}) interface{} { return v })
}

// DistinctBy returns a query includes the DistinctBy operation
// DistinctBy operation returns distinct elements from the data source using the
// provided key selector function.
//
// Example:
// 	q := From(user).DistinctBy(func (p interface{}) interface{}{
//		return p.(*Person).FirstName
// 	})
func (this *Queryable) DistinctBy(distinctFunc func(interface{}) interface{}, degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_DISTINCT, distinctFunc, getDegreeArg(degrees...)})
	return this
}

// OrderBy returns a query includes the OrderBy operation
// OrderBy operation sorts elements with provided compare function
// in ascending order.
// The comparer function should return -1 if the parameter "this" is less
// than "that", returns 0 if the "this" is same with "that", otherwisze returns 1
//
// Example:
//	q := From(user).OrderBy(func (this interface{}, that interface{}) bool {
//		return this.(*User).Age < that.(*User).Age
// 	})
func (this *Queryable) OrderBy(compare func(interface{}, interface{}) int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_ORDERBY, compare, this.degree})
	return this
}

// GroupBy returns a query includes the GroupBy operation
// GroupBy operation groups elements with provided key selector function.
// it returns a slice inlcudes Pointer of KeyValue
//
// Example:
//	q := From(user).GroupBy(func (v interface{}) interface{} {
//		return this.(*User).Age
// 	})
func (this *Queryable) GroupBy(keySelector func(interface{}) interface{}, degrees ...int) *Queryable {
	if keySelector == nil {
		panic(ErrNilAction)
	}
	this.steps = append(this.steps, commonStep{ACT_GROUPBY, keySelector, getDegreeArg(degrees...)})
	return this
}

// Union returns a query includes the Union operation
// Union operation returns set union of the source and the provided
// secondary source using hash function comparer, hash(i)==hash(o). the secondary source must
// be a valid linq data source
//
// Example:
// 	q := From(int[]{1,2,3,4,5}).Union(int[]{3,4,5,6})
// 	// q.Results() returns {1,2,3,4,5,6}
func (this *Queryable) Union(source2 interface{}, degrees ...int) *Queryable {
	if source2 == nil {
		panic(ErrUnionNilSource)
	}
	this.steps = append(this.steps, commonStep{ACT_UNION, source2, getDegreeArg(degrees...)})
	return this
}

// Concat returns a query includes the Concat operation
// Concat operation returns set union all of the source and the provided
// secondary source. the secondary source must be a valid linq data source
//
// Example:
// 	q := From(int[]{1,2,3,4,5}).Union(int[]{3,4,5,6})
// 	// q.Results() returns {1,2,3,4,5,3,4,5,6}
func (this *Queryable) Concat(source2 interface{}) *Queryable {
	if source2 == nil {
		panic(ErrConcatNilSource)
	}
	this.steps = append(this.steps, commonStep{ACT_CONCAT, source2, this.degree})
	return this
}

// Intersect returns a query includes the Intersect operation
// Intersect operation returns set intersection of the source and the
// provided secondary using hash function comparer, hash(i)==hash(o). the secondary source must
// be a valid linq data source.
//
// Example:
// 	q := From(int[]{1,2,3,4,5}).Intersect(int[]{3,4,5,6})
// 	// q.Results() returns {3,4,5}
func (this *Queryable) Intersect(source2 interface{}, degrees ...int) *Queryable {
	if source2 == nil {
		panic(ErrInterestNilSource)
	}
	this.steps = append(this.steps, commonStep{ACT_INTERSECT, source2, getDegreeArg(degrees...)})
	return this
}

// Join returns a query includes the Join operation
// Join operation correlates the elements of two source based on the equality of keys.
// Inner and outer keys are matched using hash function comparer, hash(i)==hash(o).
//
// Outer collection is the original sequence.
//
// Inner source is the one provided as inner parameter as and valid linq source.
// outerKeySelector extracts a key from outer element for outerKeySelector.
// innerKeySelector extracts a key from outer element for innerKeySelector.
//
// resultSelector takes outer element and inner element as inputs
// and returns a value which will be an element in the resulting source.
func (this *Queryable) Join(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, interface{}) interface{}, degrees ...int) *Queryable {
	if inner == nil {
		panic(ErrJoinNilSource)
	}
	if outerKeySelector == nil {
		panic(ErrOuterKeySelector)
	}
	if innerKeySelector == nil {
		panic(ErrInnerKeySelector)
	}
	if resultSelector == nil {
		panic(ErrResultSelector)
	}
	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

// LeftJoin returns a query includes the LeftJoin operation
// LeftJoin operation is similar with Join operation,
// but LeftJoin returns all elements in outer source,
// the inner elements will be null if there is not matching element in inner source
func (this *Queryable) LeftJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, interface{}) interface{}, degrees ...int) *Queryable {
	if inner == nil {
		panic(ErrJoinNilSource)
	}
	if outerKeySelector == nil {
		panic(ErrOuterKeySelector)
	}
	if innerKeySelector == nil {
		panic(ErrInnerKeySelector)
	}
	if resultSelector == nil {
		panic(ErrResultSelector)
	}
	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

// GroupJoin returns a query includes the GroupJoin operation
// GroupJoin operation is similar with Join operation,
// but GroupJoin will correlates the element of the outer source and
// the matching elements slice of the inner source.
func (this *Queryable) GroupJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, []interface{}) interface{}, degrees ...int) *Queryable {
	if inner == nil {
		panic(ErrJoinNilSource)
	}
	if outerKeySelector == nil {
		panic(ErrOuterKeySelector)
	}
	if innerKeySelector == nil {
		panic(ErrInnerKeySelector)
	}
	if resultSelector == nil {
		panic(ErrResultSelector)
	}
	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

// LeftGroupJoin returns a query includes the LeftGroupJoin operation
// LeftGroupJoin operation is similar with GroupJoin operation,
// but LeftGroupJoin returns all elements in outer source,
// the inner elements will be [] if there is not matching element in inner source
func (this *Queryable) LeftGroupJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, []interface{}) interface{}, degrees ...int) *Queryable {
	if inner == nil {
		panic(ErrJoinNilSource)
	}
	if outerKeySelector == nil {
		panic(ErrOuterKeySelector)
	}
	if innerKeySelector == nil {
		panic(ErrInnerKeySelector)
	}
	if resultSelector == nil {
		panic(ErrResultSelector)
	}
	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

func (this *Queryable) Reverse(degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_REVERSE, nil, getDegreeArg(degrees...)})
	return this
}

//KeepOrder
func (this *Queryable) KeepOrder(keep bool) *Queryable {
	this.keepOrder = keep
	return this
}

// SetDegreeOfParallelism set the degree of parallelism, it is the
// count of Goroutines when executes the each operations.
// The degree can also be customized in each linq operation function.
func (this *Queryable) SetDegreeOfParallelism(degree int) *Queryable {
	this.degree = degree
	return this
}

// SetSizeOfChunk set the size of chunk.
// chunk is the data unit of the parallelism
func (this *Queryable) SetSizeOfChunk(size int) *Queryable {
	this.chunkSize = size
	return this
}

func (this *Queryable) hGroupBy(keySelector func(interface{}) interface{}, degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_HGROUPBY, keySelector, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) makeCopiedSrc() DataSource {
	if len(this.steps) > 0 {
		if typ := this.steps[0].Typ(); typ == ACT_REVERSE || typ == ACT_SELECT {
			if listSrc, ok := this.data.(*listSource); ok {
				switch data := listSrc.data.(type) {
				case []interface{}:
					newSlice := make([]interface{}, len(data))
					_ = copy(newSlice, data)
					return &listSource{newSlice}
				default:
					newSlice := listSrc.ToSlice(false)
					return &listSource{newSlice}
				}
			}
		}
	}
	return this.data
}

// get be used in queryable internal.
// get will executes all linq operations included in Queryable
// and return the result
func (this *Queryable) get() (data DataSource, err error) {
	//create a goroutines to collect the errors for the pipeline mode step
	//errChan := make(chan *stepErr)
	go func() {
		//fmt.Println("start receive errors")
		for e := range this.errChan {
			this.stepErrs = appendSlice(this.stepErrs, e)
		}
		//fmt.Println("end receive errors")
	}()

	data = this.makeCopiedSrc() //data
	pOption, keepOrder := this.ParallelOption, this.ParallelOption.keepOrder

	for _, step := range this.steps {
		var f *promise.Future
		step1 := step

		//execute the step
		if data, f, keepOrder, err = step.Action()(data, step.POption(pOption)); err != nil {
			return nil, err
		} else if f != nil {
			//add a fail callback to collect the errors in pipeline mode
			//because the steps will be paralle in piplline mode,
			//so cannot use return value of the function
			f.Fail(func(results ...interface{}) {
				this.errChan <- NewStepError(step1.Typ(), results)
			})
		}

		//set the keepOrder for next step
		//some operation will enforce after operations keep the order,
		//e.g OrderBy operation
		pOption.keepOrder = keepOrder
	}

	return data, nil
}

//The listsource and chanSource structs----------------------------------
// listSource presents the slice or map source
type listSource struct {
	data interface{}
}

func (this listSource) Typ() int {
	return SOURCE_BLOCK
}

// ToSlice returns the interface{} slice
func (this listSource) ToSlice(keepOrder bool) []interface{} {
	switch data := this.data.(type) {
	case []interface{}:
		return data
	case map[interface{}]interface{}:
		i := 0
		results := make([]interface{}, len(data))
		for k, v := range data {
			results[i] = &KeyValue{k, v}
			i++
		}
		return results
	default:
		value := reflect.ValueOf(this.data)
		switch value.Kind() {
		case reflect.Slice:
			size := value.Len()
			results := make([]interface{}, size)
			for i := 0; i < size; i++ {
				results[i] = value.Index(i).Interface()
			}
			return results
		case reflect.Map:
			size := value.Len()
			results := make([]interface{}, size)
			for i, k := range value.MapKeys() {
				results[i] = &KeyValue{k.Interface(), value.MapIndex(k).Interface()}
			}
			return results
		}
		return nil

	}
	return nil
}

//func (this listSource) ToChan() chan interface{} {
//	out := make(chan interface{})
//	go func() {
//		for _, v := range this.ToSlice(true) {
//			out <- v
//		}
//		close(out)
//	}()
//	return out
//}

// chanSource presents the channel source
// note: the channel must be closed by caller of linq,
// otherwise will be deadlock
type chanSource struct {
	//data1 chan *Chunk
	once      *sync.Once
	data      interface{}
	chunkChan chan *Chunk
	//chunkSize int
}

func (this chanSource) Typ() int {
	return SOURCE_CHUNK
}

// makeChunkChanSure make the channel of chunk for linq operations
// This function will only run once
func (this *chanSource) makeChunkChanSure(chunkSize int) {
	if this.chunkChan == nil {
		this.once.Do(func() {
			//fmt.Println("makeChunkChanSure once")
			if this.chunkChan != nil {
				return
			}
			srcChan := reflect.ValueOf(this.data)
			this.chunkChan = make(chan *Chunk)

			go func() {
				chunkData := make([]interface{}, 0, chunkSize)
				i := 0
				for {
					if v, ok := srcChan.Recv(); ok {
						chunkData = append(chunkData, v.Interface())
						if len(chunkData) == cap(chunkData) {
							this.chunkChan <- &Chunk{chunkData, i}
							i++
							chunkData = make([]interface{}, 0, chunkSize)
						}
					} else {
						break
					}
				}

				if len(chunkData) > 0 {
					this.chunkChan <- &Chunk{chunkData, i}
				}

				this.chunkChan <- nil
			}()
		})
	}
}

//Itr returns a function that returns a pointer of chunk every be called
func (this *chanSource) Itr(chunkSize int) func() (*Chunk, bool) {
	this.makeChunkChanSure(chunkSize)
	ch := this.chunkChan
	return func() (*Chunk, bool) {
		c, ok := <-ch
		//fmt.Println("chanSource receive", c)
		return c, ok
	}
}

//Close closes the channel of the chunk
func (this chanSource) Close() {
	if this.chunkChan != nil {
		close(this.chunkChan)
	}
}

//ToSlice returns a slice included all elements in the channel source
func (this chanSource) ToSlice(keepOrder bool) []interface{} {
	if this.chunkChan != nil {
		chunks := make([]interface{}, 0, 2)
		avl := newChunkAvlTree()

		//fmt.Println("ToSlice start receive Chunk")
		for c := range this.chunkChan {
			//if use the buffer channel, then must receive a nil as end flag
			if reflect.ValueOf(c).IsNil() {
				this.Close()
				//fmt.Println("close chan")
				break
			}

			if keepOrder {
				avl.Insert(c)
			} else {
				chunks = appendSlice(chunks, c)
			}
			//fmt.Println("ToSlice end receive a Chunk")
		}
		if keepOrder {
			chunks = avl.ToSlice()
		}

		return expandChunks(chunks, false)
	} else {
		srcChan := reflect.ValueOf(this.data)
		if srcChan.Kind() != reflect.Chan {
			panic(ErrUnsupportSource)
		}

		result := make([]interface{}, 0, 10)
		for {
			if v, ok := srcChan.Recv(); ok {
				result = append(result, v.Interface())
			} else {
				break
			}
		}
		return result
	}
}

//func (this chanSource) ToChan() chan interface{} {
//	out := make(chan interface{})
//	go func() {
//		for c := range this.data {
//			for _, v := range c.data {
//				out <- v
//			}
//		}
//	}()
//	return out
//}

// hKeyValue be used in Distinct, Join, Union/Intersect operations
type hKeyValue struct {
	keyHash uint64
	key     interface{}
	value   interface{}
}

//the struct and functions of each operation-------------------------------------------------------------------------
const (
	ACT_SELECT int = iota
	ACT_WHERE
	ACT_GROUPBY
	ACT_HGROUPBY
	ACT_ORDERBY
	ACT_DISTINCT
	ACT_JOIN
	ACT_GROUPJOIN
	ACT_UNION
	ACT_CONCAT
	ACT_INTERSECT
	ACT_REVERSE
)

//stepAction presents a action related to a linq operation
type stepAction func(DataSource, *ParallelOption) (DataSource, *promise.Future, bool, error)

//step present a linq operation
type step interface {
	Action() stepAction
	Typ() int
	Degree() int
	POption(option ParallelOption) *ParallelOption
}

type commonStep struct {
	typ    int
	act    interface{}
	degree int
}

type joinStep struct {
	commonStep
	outerKeySelector func(interface{}) interface{}
	innerKeySelector func(interface{}) interface{}
	resultSelector   interface{}
	isLeftJoin       bool
}

func (this commonStep) Typ() int    { return this.typ }
func (this commonStep) Degree() int { return this.degree }
func (this commonStep) POption(option ParallelOption) *ParallelOption {
	if this.degree != 0 {
		option.degree = this.degree
	}
	return &option
}

func (this commonStep) Action() (act stepAction) {
	switch this.typ {
	case ACT_SELECT:
		act = getSelect(this.act.(func(interface{}) interface{}))
	case ACT_WHERE:
		act = getWhere(this.act.(func(interface{}) bool))
	case ACT_DISTINCT:
		act = getDistinct(this.act.(func(interface{}) interface{}))
	case ACT_ORDERBY:
		act = getOrder(this.act.(func(interface{}, interface{}) int))
	case ACT_GROUPBY:
		act = getGroupBy(this.act.(func(interface{}) interface{}), false)
	case ACT_HGROUPBY:
		act = getGroupBy(this.act.(func(interface{}) interface{}), true)
	case ACT_UNION:
		act = getUnion(this.act)
	case ACT_CONCAT:
		act = getConcat(this.act)
	case ACT_INTERSECT:
		act = getIntersect(this.act)
	case ACT_REVERSE:
		act = getReverse()
	}
	return
}

func (this joinStep) Action() (act stepAction) {
	switch this.typ {
	case ACT_JOIN:
		act = getJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(func(interface{}, interface{}) interface{}), this.isLeftJoin)
	case ACT_GROUPJOIN:
		act = getGroupJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(func(interface{}, []interface{}) interface{}), this.isLeftJoin)
	}
	return
}

// The functions get linq operation------------------------------------
func getSelect(selectFunc func(interface{}) interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.keepOrder
		mapChunk := func(c *Chunk) *Chunk {
			//result := make([]interface{}, len(c.data)) //c.end-c.start+2)
			mapSlice(c.data, selectFunc, &(c.data))
			return c //&Chunk{result, c.order}
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, handled := trySequential(src, option, mapChunk); handled {
			return list, nil, option.keepOrder, nil
		}

		switch s := src.(type) {
		case *listSource:
			size := len(s.ToSlice(false))
			results := make([]interface{}, size)
			f := parallelMapListToList(s, func(c *Chunk) *Chunk {
				out := results[c.order : c.order+len(c.data)]
				mapSlice(c.data, selectFunc, &out)
				return nil
			}, option)

			dst, e = getFutureResult(f, func(r []interface{}) DataSource {
				//fmt.Println("results=", results)
				return &listSource{results}
			})
			return
		case *chanSource:
			//use channel mode if the source is a channel
			f, out := parallelMapChanToChan(s, nil, mapChunk, option)

			sf, dst, e = f, &chanSource{chunkChan: out}, nil
			//noted when use pipeline mode to start next step, the future of current step must be returned
			//otherwise the errors in current step will be missed
			return
		}

		panic(ErrUnsupportSource)
	})

}

func getWhere(sure func(interface{}) bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = filterChunk(c, sure)
			return
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, handled := trySequential(src, option, mapChunk); handled {
			return list, nil, option.keepOrder, nil
		}

		//always use channel mode in Where operation
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		//fmt.Println("return where")
		return &chanSource{chunkChan: reduceSrc}, f, option.keepOrder, nil
	})
}

func getOrder(compare func(interface{}, interface{}) int) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		//order operation be sequentail
		option.degree = 1

		switch s := src.(type) {
		case *listSource:
			//quick sort
			sorteds := sortSlice(s.ToSlice(false), func(this, that interface{}) bool {
				return compare(this, that) == -1
			})
			return &listSource{sorteds}, nil, true, nil
		case *chanSource:
			//AVL tree sort
			avl := NewAvlTree(compare)
			f, _ := parallelMapChanToChan(s, nil, func(c *Chunk) *Chunk {
				for _, v := range c.data {
					avl.Insert(v)
				}
				return nil
			}, option)

			dst, e = getFutureResult(f, func(r []interface{}) DataSource {
				return &listSource{avl.ToSlice()}
			})
			keep = true
			return
		}
		panic(ErrUnsupportSource)
	})
}

func getDistinct(distinctFunc func(interface{}) interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, distinctFunc, nil), c.order}
			return
		}

		//test the size = 100, trySequential only speed up 10%
		//if list, handled := trySequential(src, &option, mapChunk); handled {
		//	c := &Chunk{list.ToSlice(false), 0}
		//	distKvs := make(map[uint64]int)
		//	c = distinctChunkVals(c, distKvs)
		//	return &listSource{c.data}, nil, option.keepOrder, nil
		//}

		//map the element to a keyValue that key is hash value and value is element
		f, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, option)

		//reduce the keyValue map to get distinct values
		if chunks, err := reduceDistinctVals(f, reduceSrcChan); err == nil {
			//get distinct values
			result := expandChunks(chunks, false)
			return &listSource{result}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, err
		}
	})
}

//note the groupby cannot keep order because the map cannot keep order
func getGroupBy(groupFunc func(interface{}) interface{}, hashAsKey bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, groupFunc, nil), c.order}
			return
		}

		//map the element to a keyValue that key is group key and value is element
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		groupKvs := make(map[interface{}]interface{})
		groupKv := func(v interface{}) {
			kv := v.(*hKeyValue)
			k := iif(hashAsKey, kv.keyHash, kv.key)
			if v, ok := groupKvs[k]; !ok {
				groupKvs[k] = []interface{}{kv.value}
			} else {
				list := v.([]interface{})
				groupKvs[k] = appendSlice(list, kv.value)
			}
		}

		//reduce the keyValue map to get grouped slice
		//get key with group values values
		errs := reduceChan(f.GetChan(), reduceSrc, func(c *Chunk) {
			for _, v := range c.data {
				groupKv(v)
			}
		})

		if errs == nil {
			return &listSource{groupKvs}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, NewLinqError("Group error", errs)
		}

	})
}

func getJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, interface{}) interface{}, isLeftJoin bool) stepAction {
	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *hKeyValue, innerList []interface{}, results *[]interface{}) {
			for _, iv := range innerList {
				*results = appendSlice(*results, resultSelector(outerkv.value, iv))
			}
		}, func(outerkv *hKeyValue, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, nil))
		}, isLeftJoin)
}

func getGroupJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, []interface{}) interface{}, isLeftJoin bool) stepAction {

	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *hKeyValue, innerList []interface{}, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, innerList))
		}, func(outerkv *hKeyValue, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, []interface{}{}))
		}, isLeftJoin)
}

func getJoinImpl(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	matchSelector func(*hKeyValue, []interface{}, *[]interface{}),
	unmatchSelector func(*hKeyValue, *[]interface{}), isLeftJoin bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.keepOrder
		innerKVtask := promise.Start(func() []interface{} {
			if innerKvsDs, err := From(inner).hGroupBy(innerKeySelector).get(); err == nil {
				return []interface{}{innerKvsDs.(*listSource).data, true}
			} else {
				return []interface{}{err, false}
			}
		})

		mapChunk := func(c *Chunk) (r *Chunk) {
			defer func() {
				if e := recover(); e != nil {
					fmt.Println("mapChunk get error:", e)
				}
			}()
			outerKvs := getKeyValues(c, outerKeySelector, nil)
			results := make([]interface{}, 0, 10)

			if r, ok := innerKVtask.Get(); ok != promise.RESULT_SUCCESS {
				//todo:
				fmt.Println("innerKV get error", ok, r)
			} else {
				innerKvs := r[0].(map[interface{}]interface{})

				for _, o := range outerKvs {
					outerkv := o.(*hKeyValue)
					if innerList, ok := innerKvs[outerkv.keyHash]; ok {
						//fmt.Println("outer", *outerkv, "inner", innerList)
						matchSelector(outerkv, innerList.([]interface{}), &results)
					} else if isLeftJoin {
						//fmt.Println("outer", *outerkv)
						unmatchSelector(outerkv, &results)
					}
				}
			}

			return &Chunk{results, c.order}
		}

		switch s := src.(type) {
		case *listSource:
			//Todo:
			//can use channel mode like Where operation?
			outerKeySelectorFuture := parallelMapListToList(s, mapChunk, option)

			dst, e = getFutureResult(outerKeySelectorFuture, func(results []interface{}) DataSource {
				result := expandChunks(results, false)
				return &listSource{result}
			})
			return
		case *chanSource:
			f, out := parallelMapChanToChan(s, nil, mapChunk, option)
			dst, sf, e = &chanSource{chunkChan: out}, f, nil
			return
		}

		panic(ErrUnsupportSource)
	})
}

func getUnion(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		reduceSrcChan := make(chan *Chunk)
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
			return
		}

		//map the elements of source and source2 to the a KeyValue slice
		//includes the hash value and the original element
		f1, reduceSrcChan := parallelMapToChan(src, reduceSrcChan, mapChunk, option)
		f2, reduceSrcChan := parallelMapToChan(From(source2).data, reduceSrcChan, mapChunk, option)

		mapFuture := promise.WhenAll(f1, f2)

		//reduce the KeyValue slices to get distinct slice
		//get key with group values values
		if chunks, err := reduceDistinctVals(mapFuture, reduceSrcChan); err == nil {
			//get distinct values
			result := expandChunks(chunks, false)

			return &listSource{result}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, err
		}

	})
}

func getConcat(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		slice1 := src.ToSlice(option.keepOrder)
		if slice2, err2 := From(source2).KeepOrder(option.keepOrder).Results(); err2 == nil {
			result := make([]interface{}, len(slice1)+len(slice2))
			_ = copy(result[0:len(slice1)], slice1)
			_ = copy(result[len(slice1):len(slice1)+len(slice2)], slice2)
			return &listSource{result}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, err2
		}

	})
}

func getIntersect(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (result DataSource, f *promise.Future, keep bool, err error) {
		distKvs := make(map[uint64]bool)

		f1 := promise.Start(func() []interface{} {
			mapChunk := func(c *Chunk) (r *Chunk) {
				r = &Chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
				return
			}

			f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

			//get distinct values of src1
			errs := reduceChan(f.GetChan(), reduceSrc, func(c *Chunk) {
				for _, v := range c.data {
					kv := v.(*hKeyValue)
					if _, ok := distKvs[kv.keyHash]; !ok {
						distKvs[kv.keyHash] = true
						//fmt.Println("add", kv.value, "to distKvs")
					}
				}
			})

			if errs == nil {
				return nil
			} else {
				return []interface{}{NewLinqError("Group error", errs), false}
			}
		})

		query2 := From(source2).Select(func(v interface{}) interface{} {
			return &KeyValue{hash64(v), v}
		})

		var dataSource2 []interface{}
		if dataSource2, err = query2.Results(); err != nil {
			return nil, nil, option.keepOrder, err
		}

		if r, typ := f1.Get(); typ != promise.RESULT_SUCCESS {
			return nil, nil, option.keepOrder, NewLinqError("Intersect error", r)
		}

		//filter source2
		resultKVs := make(map[uint64]interface{}, len(distKvs))
		for _, v := range dataSource2 {
			kv := v.(*KeyValue)
			k := kv.key.(uint64)
			if _, ok := distKvs[k]; ok {
				if _, ok := resultKVs[k]; !ok {
					resultKVs[k] = kv.value
				}
			}
		}

		//get the intersection slices
		results := make([]interface{}, len(resultKVs))
		i := 0
		for _, v := range resultKVs {
			results[i] = v
			i++
		}

		return &listSource{results[0:i]}, nil, option.keepOrder, nil

	})
}

func getReverse() stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.keepOrder
		wholeSlice := src.ToSlice(true)
		srcSlice, size := wholeSlice[0:len(wholeSlice)/2], len(wholeSlice)

		mapChunk := func(c *Chunk) *Chunk {
			for i := 0; i < len(c.data); i++ {
				j := c.order + i
				t := wholeSlice[size-1-j]
				wholeSlice[size-1-j] = c.data[i]
				c.data[i] = t
			}
			return c
		}

		reverseSrc := &listSource{srcSlice}

		//try to use sequentail if the size of the data is less than size of chunk
		if _, handled := trySequential(reverseSrc, option, mapChunk); handled {
			return &listSource{wholeSlice}, nil, option.keepOrder, nil
		}

		f := parallelMapListToList(reverseSrc, func(c *Chunk) *Chunk {
			return mapChunk(c)
		}, option)
		dst, e = getFutureResult(f, func(r []interface{}) DataSource {
			//fmt.Println("results=", results)
			return &listSource{wholeSlice}
		})
		return
	})
}

//paralleliam functions------------------------------------------
func parallelMapToChan(src DataSource, reduceSrcChan chan *Chunk, mapChunk func(c *Chunk) (r *Chunk), option *ParallelOption) (f *promise.Future, ch chan *Chunk) {
	//get all values and keys
	switch s := src.(type) {
	case *listSource:
		return parallelMapListToChan(s, reduceSrcChan, mapChunk, option)
	case *chanSource:
		return parallelMapChanToChan(s, reduceSrcChan, mapChunk, option)
	default:
		panic(ErrUnsupportSource)
	}
}

func parallelMapChanToChan(src *chanSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption) (*promise.Future, chan *Chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.degree)
		createOutChan = true
	}

	itr := src.Itr(option.chunkSize)
	fs := make([]*promise.Future, option.degree)
	for i := 0; i < option.degree; i++ {
		f := promise.Start(func() []interface{} {
			for {
				//fmt.Println("begin select receive")
				if c, ok := itr(); ok {
					//fmt.Println("select receive", c)
					if reflect.ValueOf(c).IsNil() {
						src.Close()
						//fmt.Println("select receive a nil-----------------")
						break
					}
					//fmt.Println("select receive", *c)
					d := task(c)
					if out != nil && d != nil {
						out <- d
						//fmt.Println("end select send", *d)
					}
				} else {
					//fmt.Println("end receive--------------")
					break
				}
			}
			return nil
			//fmt.Println("r=", r)
		})
		fs[i] = f
	}
	f := promise.WhenAll(fs...)

	if createOutChan {
		addCloseChanCallback(f, out)
	}
	return f, out
}

func parallelMapListToChan(src DataSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption) (*promise.Future, chan *Chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.degree)
		createOutChan = true
	}

	f := parallelMapList(src, func(c *Chunk) func() []interface{} {
		return func() []interface{} {
			r := task(c)
			if out != nil {
				out <- r
				//fmt.Println("send", len(r.data))
			}
			return nil
		}
	}, option)
	if createOutChan {
		addCloseChanCallback(f, out)
	}
	//fmt.Println("return f")
	return f, out
}

func parallelMapListToList(src DataSource, task func(*Chunk) *Chunk, option *ParallelOption) *promise.Future {
	return parallelMapList(src, func(c *Chunk) func() []interface{} {
		return func() []interface{} {
			r := task(c)
			return []interface{}{r, true}
		}
	}, option)
}

func parallelMapList(src DataSource, getAction func(*Chunk) func() []interface{}, option *ParallelOption) (f *promise.Future) {
	fs := make([]*promise.Future, option.degree)
	data := src.ToSlice(false)
	lenOfData, size, j := len(data), ceilChunkSize(len(data), option.degree), 0

	if size < option.chunkSize {
		size = option.chunkSize
	}

	for i := 0; i < option.degree && i*size < lenOfData; i++ {
		end := (i + 1) * size
		if end >= lenOfData {
			end = lenOfData
		}
		c := &Chunk{data[i*size : end], i * size} //, end}

		f = promise.Start(getAction(c))
		fs[i] = f
		j++
	}
	f = promise.WhenAll(fs[0:j]...)

	return
}

//The functions for check if should be Sequential and execute the Sequential mode ----------------------------

//if the data source is listSource, then computer the degree of paralleliam.
//if the degree is 1, the paralleliam is no need.
func singleDegree(src DataSource, option *ParallelOption) bool {
	if s, ok := src.(*listSource); ok {
		list := s.ToSlice(false)
		return len(list) <= option.chunkSize
	} else {
		//the channel source will always use paralleliam
		return false
	}
}

func trySequential(src DataSource, option *ParallelOption, mapChunk func(c *Chunk) (r *Chunk)) (DataSource, bool) {
	if useSingle := singleDegree(src, option); useSingle {
		c := &Chunk{src.ToSlice(false), 0}
		r := mapChunk(c)
		return &listSource{r.data}, true
	} else {
		return nil, false
	}

}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceChan(chEndFlag chan *promise.PromiseResult, src chan *Chunk, reduce func(*Chunk)) []interface{} {
	if cap(src) == 0 {
		//for no buffer channel,
		//receiving the end flag from the chEndFlag presents the the all data be sent
		for {
			select {
			case r := <-chEndFlag:
				//fmt.Println("return reduceChan")
				if r.Typ != promise.RESULT_SUCCESS {
					return r.Result
				} else {
					return nil
				}
			case v, ok := <-src:
				if ok && v != nil {
					reduce(v)
				}
			}
		}
	} else {
		//for buffer channel,
		//receiving a nil presents the all data be sent
		for {
			select {
			case r := <-chEndFlag:
				if r != nil && r.Typ != promise.RESULT_SUCCESS {
					//the future tasks related to source channel gets the error
					return r.Result
				}
			case v, ok := <-src:
				if ok {
					if v != nil {
						reduce(v)
					} else {
						close(src)
						return nil
					}
				}
			}
		}
	}

}

func reduceDistinctVals(mapFuture *promise.Future, reduceSrcChan chan *Chunk) ([]interface{}, error) {
	//get distinct values
	chunks := make([]interface{}, 0, 2)
	distKvs := make(map[uint64]int)
	errs := reduceChan(mapFuture.GetChan(), reduceSrcChan, func(c *Chunk) {
		chunks = appendSlice(chunks, distinctChunkVals(c, distKvs))
	})
	if errs == nil {
		return chunks, nil
	} else {
		return nil, NewLinqError("reduceDistinctVals error", errs)
	}
}

//util functions--------------------------------------------------------
func distinctChunkVals(c *Chunk, distKvs map[uint64]int) *Chunk {
	result := make([]interface{}, len(c.data))
	i := 0
	for _, v := range c.data {
		kv := v.(*hKeyValue)
		if _, ok := distKvs[kv.keyHash]; !ok {
			distKvs[kv.keyHash] = 1
			result[i] = kv.value
			i++
		}
	}
	c.data = result[0:i]
	return c
}

func addCloseChanCallback(f *promise.Future, out chan *Chunk) {
	//fmt.Println("addCloseChanCallback")
	f.Always(func(results ...interface{}) {
		//must use goroutiner, else may deadlock when out is buffer chan
		//because it maybe called before the chan receiver be started.
		//if the buffer is full, out <- nil will be holder then deadlock
		go func() {
			if out != nil {
				if cap(out) == 0 {
					close(out)
				} else {
					//fmt.Println("begin send nil")
					out <- nil
					//fmt.Println("send nil")
				}
			}
		}()
	})
	//fmt.Println("addCloseChanCallback done")
}

func getFutureResult(f *promise.Future, dataSourceFunc func([]interface{}) DataSource) (DataSource, error) {
	if results, typ := f.Get(); typ != promise.RESULT_SUCCESS {
		//todo
		fmt.Println("getFutureResult get", results, typ)
		return nil, nil
	} else {
		//fmt.Println("(results)=", (results))
		return dataSourceFunc(results), nil
	}
}

func filterChunk(c *Chunk, f func(interface{}) bool) *Chunk {
	result := filterSlice(c.data, f)
	//fmt.Println("c=", c)
	//fmt.Println("result=", result)
	return &Chunk{result, c.order}
}

func filterSlice(src []interface{}, f func(interface{}) bool) []interface{} {
	dst := make([]interface{}, 0, 10)

	for _, v := range src {
		if f(v) {
			dst = append(dst, v)
		}
	}
	return dst
}

func mapSlice(src []interface{}, f func(interface{}) interface{}, out *[]interface{}) []interface{} {
	var dst []interface{}
	if out == nil {
		dst = make([]interface{}, len(src))
	} else {
		dst = *out
	}

	for i, v := range src {
		dst[i] = f(v)
	}
	return dst
}

func expandChunks(src []interface{}, keepOrder bool) []interface{} {
	if src == nil {
		return nil
	}

	if keepOrder {
		src = sortSlice(src, func(a interface{}, b interface{}) bool {
			var (
				a1, b1 *Chunk
			)
			switch v := a.(type) {
			case []interface{}:
				a1, b1 = v[0].(*Chunk), b.([]interface{})[0].(*Chunk)
			case *Chunk:
				a1, b1 = v, b.(*Chunk)
			}
			return a1.order < b1.order
		})
	}

	count := 0
	chunks := make([]*Chunk, len(src))
	for i, c := range src {
		switch v := c.(type) {
		case []interface{}:
			chunks[i] = v[0].(*Chunk)
		case *Chunk:
			chunks[i] = v
		}
		count += len(chunks[i].data)
	}

	//fmt.Println("count", count)
	result := make([]interface{}, count)
	start := 0
	for _, c := range chunks {
		size := len(c.data)
		copy(result[start:start+size], c.data)
		start += size
	}
	return result
}

func appendSlice(src []interface{}, v interface{}) []interface{} {
	c, l := cap(src), len(src)
	if c >= l+1 {
		return append(src, v)
	} else {
		//reslice
		newSlice := make([]interface{}, l, 2*c)
		_ = copy(newSlice[0:l], src)
		return append(newSlice, v)
	}
}

//func appendChunkSlice(src []*Chunk, v *Chunk) []*Chunk {
//	c, l := cap(src), len(src)
//	if c >= l+1 {
//		return append(src, v)
//	} else {
//		//reslice
//		newSlice := make([]*Chunk, l+1, 2*c)
//		_ = copy(newSlice[0:l], src)
//		return append(newSlice, v)
//	}
//}

func ceilChunkSize(a int, b int) int {
	if a%b != 0 {
		return a/b + 1
	} else {
		return a / b
	}
}

func getKeyValues(c *Chunk, keyFunc func(v interface{}) interface{}, KeyValues *[]interface{}) []interface{} {
	if KeyValues == nil {
		list := (make([]interface{}, len(c.data)))
		KeyValues = &list
	}
	mapSlice(c.data, func(v interface{}) interface{} {
		k := keyFunc(v)
		return &hKeyValue{hash64(k), k, v}
	}, KeyValues)
	return *KeyValues
}

func iif(sure bool, trueVal interface{}, falseVal interface{}) interface{} {
	if sure {
		return trueVal
	} else {
		return falseVal
	}
}

func getDegreeArg(degrees ...int) int {
	degree := 0
	if degrees != nil && len(degrees) > 0 {
		degree = degrees[0]
		if degree == 0 {
			degree = 1
		}
	}
	return degree
}
