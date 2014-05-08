package plinq

import (
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	DEFAULTCHUNKSIZE       = 200
	DEFAULTMINCUNKSIZE     = 40
	LARGECHUNKSIZE         = 2000
	SOURCE_LIST        int = iota //presents the list source
	SOURCE_CHANNEL                //presents the channel source
)

var (
	numCPU               int
	ErrUnsupportSource   = errors.New("unsupport DataSource")
	ErrNilSource         = errors.New("datasource cannot be nil")
	ErrUnionNilSource    = errors.New("cannot union nil data source")
	ErrConcatNilSource   = errors.New("cannot concat nil data source")
	ErrInterestNilSource = errors.New("cannot interest nil data source")
	ErrExceptNilSource   = errors.New("cannot Except nil data source")
	ErrJoinNilSource     = errors.New("cannot join nil data source")
	ErrNilAction         = errors.New("action cannot be nil")
	ErrOuterKeySelector  = errors.New("outerKeySelector cannot be nil")
	ErrInnerKeySelector  = errors.New("innerKeySelector cannot be nil")
	ErrResultSelector    = errors.New("resultSelector cannot be nil")
	ErrTaskFailure       = errors.New("ErrTaskFailure")
)

func init() {
	numCPU = runtime.NumCPU()
	_ = fmt.Println
	//fmt.Println("numCPU is", numCPU)

	Sum = &AggregateOperation{0, sumOpr, sumOpr}
	Count = &AggregateOperation{0, countOpr, sumOpr}
	Min = getMinOpr(defLess)
	Max = getMaxOpr(defLess)
}

type PredicateFunc func(interface{}) bool
type OneArgsFunc func(interface{}) interface{}
type TwoArgsFunc func(interface{}, interface{}) interface{}
type CompareFunc func(interface{}, interface{}) int

// the struct and interface about data DataSource---------------------------------------------------
// A Chunk presents a data chunk, the paralleliam algorithm always handle a chunk in a paralleliam tasks.
type Chunk struct {
	Data       Slicer //[]interface{}
	Order      int    //a index presents the order of chunk
	StartIndex int    //a index presents the start index in whole data
}

// The DataSource presents the data of linq operation
// Most linq operations usually convert a DataSource to another DataSource
type DataSource interface {
	Typ() int                 //list or chan?
	ToSlice(bool) Slicer      //Get a slice includes all datas
	ToChan() chan interface{} //will be implement in futures
}

// KeyValue presents a key value pair, it be used by GroupBy, Join and Set operations
type KeyValue struct {
	Key   interface{}
	Value interface{}
}

//Aggregate operation structs and functions-------------------------------

//TODO: let user can set the size of chunk for Aggregate operation
type AggregateOperation struct {
	Seed         interface{}
	AggAction    TwoArgsFunc
	ReduceAction TwoArgsFunc
}

// Standard Sum, Count, Min and Max Aggregation operation
var (
	Sum   *AggregateOperation
	Count *AggregateOperation
	Min   *AggregateOperation
	Max   *AggregateOperation
)

//the queryable struct-------------------------------------------------------------------------

// A ParallelOption presents the options of the paralleliam algorithm.
type ParallelOption struct {
	Degree    int  //The degree of the paralleliam algorithm
	ChunkSize int  //The size of chunk
	KeepOrder bool //whether need keep order of original data
}

// A Queryable presents an object includes the data and query operations
// All query functions will return Queryable.
// For getting the result slice of the query, use Results().
type Queryable struct {
	data  DataSource
	steps []step
	//stepErrs []interface{}
	errChan chan []error
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
	return newQueryable(newDataSource(src))
}

// Results evaluates the query and returns the results as interface{} slice.
// If the error occurred in during evaluation of the query, it will be returned.
//
// Example:
// 	results, err := From([]interface{}{"Jack", "Rock"}).Select(something).Results()
func (this *Queryable) Results() (results []interface{}, err error) {
	if ds, e := this.execute(); e == nil {
		//在Channel模式下，必须先取完全部的数据，否则stepErrs将死锁
		//e将被丢弃，因为e会被send到errChan并在this.stepErrs()中返回
		results = ds.ToSlice(this.KeepOrder).ToInterfaces()
	}

	err = this.stepErrs()
	if !isNil(err) {
		results = nil
	} else {
		err = nil
	}
	return
}

// ToChan evaluates the query and returns the results as interface{} channel.
// If the error occurred in during evaluation of the query, it will be returned.
//
// Example:
// 	ch, err := From([]interface{}{"Jack", "Rock"}).Select(something).toChan
func (this *Queryable) ToChan() (out chan interface{}, errChan chan error, err error) {
	if ds, e := this.execute(); e == nil {
		out = ds.ToChan()
		errChan = make(chan error)
		go func() {
			err1 := this.stepErrs()
			if !isNil(err1) {
				errChan <- err1
			}
			close(errChan)
		}()
		return
	} else {
		return nil, nil, e
	}

}

// ElementAt returns the element at the specified index i.
// If i is a negative number or if no element exists at i-th index, found will
// be returned false.
//
// Example:
// i, found, err := From([]int{0,1,2}).ElementAt(2)
//		// i is 2
func (this *Queryable) ElementAt(i int) (result interface{}, found bool, err error) {
	return this.singleValue(func(ds DataSource, pOption *ParallelOption) (result interface{}, found bool, err error) {
		return getElementAt(ds, i, pOption)
	})
}

func (this *Queryable) singleValue(getVal func(DataSource, *ParallelOption) (result interface{}, found bool, err error)) (result interface{}, found bool, err error) {
	if ds, e := this.execute(); e == nil {
		//在Channel模式下，必须先取完全部的数据，否则stepErrs将死锁
		//e将被丢弃，因为e会被send到errChan并在this.stepErrs()中返回
		result, found, err = getVal(ds, &(this.ParallelOption))
	}

	stepErrs := this.stepErrs()
	if !isNil(stepErrs) {
		result, found = nil, false
		if err != nil {
			stepErrs.innerErrs = append(stepErrs.innerErrs,
				NewStepError(1000, ACT_SINGLEVALUE, err))
		}
		err = stepErrs
	} else if isNil(err) {
		err = nil
	}
	return
}

// First returns the first element in the data source that matchs the
// provided value. If source is empty or such element is not found, found
// value will be false, otherwise elem is returned.
// Example:
// 	r, found, err := From([]int{0,1,2,3}).FirstBy(func (i interface{})bool{
//		return i.(int) % 2 == 1
// 	})
// 	if err == nil && found {
//		// r is 1
// 	}
func (this *Queryable) First(val interface{}, chunkSizes ...int) (result interface{}, found bool, err error) {
	return this.FirstBy(func(item interface{}) bool { return equals(item, val) }, chunkSizes...)
}

// FirstBy returns the first element in the data source that matchs the
// provided predicate. If source is empty or such element is not found, found
// value will be false, otherwise elem is returned.
// Example:
// 	r, found, err := From([]int{0,1,2,3}).FirstBy(func (i interface{})bool{
//		return i.(int) % 2 == 1
// 	})
// 	if err == nil && found {
//		// r is 1
// 	}
func (this *Queryable) FirstBy(predicate PredicateFunc, chunkSizes ...int) (result interface{}, found bool, err error) {
	return this.singleValue(func(ds DataSource, pOption *ParallelOption) (result interface{}, found bool, err error) {
		option, chunkSize := this.ParallelOption, getChunkSizeArg(chunkSizes...)
		if chunkSize != 0 {
			option.ChunkSize = chunkSize
		}
		return getFirstBy(ds, predicate, &option)
	})
}

// Last returns the last element in the data source that matchs the
// provided value. If source is empty or such element is not found, found
// value will be false, otherwise elem is returned.
// Example:
// 	r, found, err := From([]int{0,1,2,3}).LasyBy(func (i interface{})bool{
//		return i.(int) % 2 == 1
// 	})
// 	if err == nil && found {
//		// r is 3
// 	}
func (this *Queryable) Last(val interface{}, chunkSizes ...int) (result interface{}, found bool, err error) {
	return this.LastBy(func(item interface{}) bool { return equals(item, val) }, chunkSizes...)
}

// LastBy returns the last element in the data source that matchs the
// provided predicate. If source is empty or such element is not found, found
// value will be false, otherwise elem is returned.
// Example:
// 	r, found, err := From([]int{0,1,2,3}).LasyBy(func (i interface{})bool{
//		return i.(int) % 2 == 1
// 	})
// 	if err == nil && found {
//		// r is 3
// 	}
func (this *Queryable) LastBy(predicate PredicateFunc, chunkSizes ...int) (result interface{}, found bool, err error) {
	return this.singleValue(func(ds DataSource, pOption *ParallelOption) (result interface{}, found bool, err error) {
		option, chunkSize := this.ParallelOption, getChunkSizeArg(chunkSizes...)
		if chunkSize != 0 {
			option.ChunkSize = chunkSize
		}
		return getLastBy(ds, predicate, &option)
	})
}

// Aggregate returns the results of aggregation operation
// Aggregation operation aggregates the result in the data source base on the AggregateOperation.
//
// Aggregate can return a slice includes multiple results if passes multiple aggregation operation once.
// If passes one aggregation operation, Aggregate will return single interface{}
//
// Noted:
// Aggregate supports the customized aggregation operation
// TODO: doesn't support the mixed type in aggregate now
//
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	aggResults, err := From(arr).Aggregate(Sum, Count, Max, Min) // return [18, 4, 9, 0]
//	// or
//	sum, err := From(arr).Aggregate(Sum) // sum is 18
func (this *Queryable) Aggregate(aggregateFuncs ...*AggregateOperation) (result interface{}, err error) {
	result, _, err = this.singleValue(func(ds DataSource, pOption *ParallelOption) (resultValue interface{}, found bool, err1 error) {
		results, e := getAggregate(ds, aggregateFuncs, &(this.ParallelOption))
		if e != nil {
			return nil, false, e
		}
		if len(aggregateFuncs) == 1 {
			resultValue = results[0]
		} else {
			resultValue = results
		}
		return
	})
	return
}

// Sum computes sum of numeric values in the data source.
// TODO: If sequence has non-numeric types or nil, should returns an error.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	sum, err := From(arr).Sum() // sum is 18
func (this *Queryable) Sum() (result interface{}, err error) {
	aggregateOprs := []*AggregateOperation{Sum}

	if result, err = this.Aggregate(aggregateOprs...); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

// Count returns number of elements in the data source.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	count, err := From(arr).Count() // count is 4
func (this *Queryable) Count() (result interface{}, err error) {
	aggregateOprs := []*AggregateOperation{Count}

	if result, err = this.Aggregate(aggregateOprs...); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

// Count returns number of elements in the data source.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	count, err := From(arr).Countby(func(i interface{}) bool {return i < 9}) // count is 3
func (this *Queryable) CountBy(predicate PredicateFunc) (result interface{}, err error) {
	if predicate == nil {
		predicate = PredicateFunc(func(interface{}) bool { return true })
	}
	aggregateOprs := []*AggregateOperation{getCountByOpr(predicate)}

	if result, err = this.Aggregate(aggregateOprs...); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

// Average computes average of numeric values in the data source.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	arg, err := From(arr).Average() // sum is 4.5
func (this *Queryable) Average() (result interface{}, err error) {
	aggregateOprs := []*AggregateOperation{Sum, Count}

	if results, err := this.Aggregate(aggregateOprs...); err == nil {
		count := float64(results.([]interface{})[1].(int))
		sum := results.([]interface{})[0]

		return divide(sum, count), nil
	} else {
		return nil, err
	}
}

// Max returns the maximum value in the data source.
// Max operation supports the numeric types, string and time.Time
// TODO: need more testing for string and time.Time
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	max, err := From(arr).Max() // max is 9
func (this *Queryable) Max(lesses ...func(interface{}, interface{}) bool) (result interface{}, err error) {
	var less func(interface{}, interface{}) bool
	if lesses == nil || len(lesses) == 0 {
		less = defLess
	} else {
		less = lesses[0]
	}

	aggregateOprs := []*AggregateOperation{getMaxOpr(less)}

	if results, err := this.Aggregate(aggregateOprs...); err == nil {
		return results, nil
	} else {
		return nil, err
	}
}

// Min returns the minimum value in the data source.
// Min operation supports the numeric types, string and time.Time
// TODO: need more testing for string and time.Time
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	min, err := From(arr).Max() // min is 0
func (this *Queryable) Min(lesses ...func(interface{}, interface{}) bool) (result interface{}, err error) {
	var less func(interface{}, interface{}) bool
	if lesses == nil || len(lesses) == 0 {
		less = defLess
	} else {
		less = lesses[0]
	}

	aggregateOprs := []*AggregateOperation{getMinOpr(less)}

	if results, err := this.Aggregate(aggregateOprs...); err == nil {
		return results, nil
	} else {
		return nil, err
	}
}

// Where returns a query includes the Where operation
// Where operation filters a sequence of values based on a predicate function.
//
// Example:
// 	q := From(users).Where(func (v interface{}) bool{
//		return v.(*User).Age > 18
// 	})
func (this *Queryable) Where(predicate PredicateFunc, chunkSizes ...int) *Queryable {
	mustNotNil(predicate, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_WHERE, predicate, getChunkSizeArg(chunkSizes...)})
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
func (this *Queryable) Select(selectFunc OneArgsFunc, chunkSizes ...int) *Queryable {
	mustNotNil(selectFunc, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_SELECT, selectFunc, getChunkSizeArg(chunkSizes...)})
	return this
}

func (this *Queryable) SelectMany(selectManyFunc func(interface{}) []interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(selectManyFunc, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_SELECTMANY, selectManyFunc, getChunkSizeArg(chunkSizes...)})
	return this
}

// Distinct returns a query includes the Distinct operation
// Distinct operation distinct elements from the data source.
//
// Example:
// 	q := From(users).Distinct()
func (this *Queryable) Distinct(chunkSizes ...int) *Queryable {
	return this.DistinctBy(self)
}

// DistinctBy returns a query includes the DistinctBy operation
// DistinctBy operation returns distinct elements from the data source using the
// provided key selector function.
//
// Noted: The before element may be filter in parallel mode, so cannot keep order
//
// Example:
// 	q := From(user).DistinctBy(func (p interface{}) interface{}{
//		return p.(*Person).FirstName
// 	})
func (this *Queryable) DistinctBy(distinctFunc OneArgsFunc, chunkSizes ...int) *Queryable {
	mustNotNil(distinctFunc, ErrNilAction)
	this.steps = append(this.steps, commonStep{ACT_DISTINCT, distinctFunc, getChunkSizeArg(chunkSizes...)})
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
func (this *Queryable) OrderBy(compare CompareFunc) *Queryable {
	if compare == nil {
		compare = defCompare
	}
	this.steps = append(this.steps, commonStep{ACT_ORDERBY, compare, this.Degree})
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
func (this *Queryable) GroupBy(keySelector OneArgsFunc, chunkSizes ...int) *Queryable {
	mustNotNil(keySelector, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_GROUPBY, keySelector, getChunkSizeArg(chunkSizes...)})
	return this
}

// Union returns a query includes the Union operation
// Union operation returns set union of the source and the provided
// secondary source using hash function comparer, hash(i)==hash(o). the secondary source must
// be a valid linq data source
//
// Noted: GroupBy will returns an unordered sequence.
//
// Example:
// 	q := From(int[]{1,2,3,4,5}).Union(int[]{3,4,5,6})
// 	// q.Results() returns {1,2,3,4,5,6}
func (this *Queryable) Union(source2 interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(source2, ErrUnionNilSource)

	this.steps = append(this.steps, commonStep{ACT_UNION, source2, getChunkSizeArg(chunkSizes...)})
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
	mustNotNil(source2, ErrConcatNilSource)

	this.steps = append(this.steps, commonStep{ACT_CONCAT, source2, this.Degree})
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
func (this *Queryable) Intersect(source2 interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(source2, ErrInterestNilSource)

	this.steps = append(this.steps, commonStep{ACT_INTERSECT, source2, getChunkSizeArg(chunkSizes...)})
	return this
}

// Except returns a query includes the Except operation
// Except operation returns set except of the source and the
// provided secondary source using hash function comparer, hash(i)==hash(o). the secondary source must
// be a valid linq data source.
//
// Example:
// 	q := From(int[]{1,2,3,4,5}).Except(int[]{3,4,5,6})
// 	// q.Results() returns {1,2}
func (this *Queryable) Except(source2 interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(source2, ErrExceptNilSource)

	this.steps = append(this.steps, commonStep{ACT_EXCEPT, source2, getChunkSizeArg(chunkSizes...)})
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
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	resultSelector TwoArgsFunc, chunkSizes ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, getChunkSizeArg(chunkSizes...)}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

// LeftJoin returns a query includes the LeftJoin operation
// LeftJoin operation is similar with Join operation,
// but LeftJoin returns all elements in outer source,
// the inner elements will be null if there is not matching element in inner source
func (this *Queryable) LeftJoin(inner interface{},
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	resultSelector TwoArgsFunc, chunkSizes ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, getChunkSizeArg(chunkSizes...)}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

// GroupJoin returns a query includes the GroupJoin operation
// GroupJoin operation is similar with Join operation,
// but GroupJoin will correlates the element of the outer source and
// the matching elements slice of the inner source.
func (this *Queryable) GroupJoin(inner interface{},
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	resultSelector func(interface{}, []interface{}) interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, getChunkSizeArg(chunkSizes...)}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

// LeftGroupJoin returns a query includes the LeftGroupJoin operation
// LeftGroupJoin operation is similar with GroupJoin operation,
// but LeftGroupJoin returns all elements in outer source,
// the inner elements will be [] if there is not matching element in inner source
func (this *Queryable) LeftGroupJoin(inner interface{},
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	resultSelector func(interface{}, []interface{}) interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, getChunkSizeArg(chunkSizes...)}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

// Reverse returns a query includes the Reverse operation
// Reverse operation returns a data source with a inverted order of the original source
//
// Example:
// 	q := From([]int{1,2,3,4,5}).Reverse()
// 	// q.Results() returns {5,4,3,2,1}
func (this *Queryable) Reverse(chunkSizes ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_REVERSE, nil, getChunkSizeArg(chunkSizes...)})
	return this
}

// Skip returns a query includes the Skip operation
// Skip operation bypasses a specified number of elements in a sequence
// and then returns the remaining elements..
//
// Example:
// 	arr, err := From([]int{1,2,3,4,5,6}).Skip(3).Results()
//		// arr will be 4, 5, 6
func (this *Queryable) Skip(count int) *Queryable {
	//this.act.(predicate predicateFunc)
	this.steps = append(this.steps, commonStep{ACT_SKIP, count, 0})
	return this
}

// SkipWhile returns a query includes the SkipWhile operation
// SkipWhile operation bypasses elements in a sequence as long as a specified condition
// is true and then returns the remaining elements.
//
// Example:
// 	arr, err := From([]int{1,2,3,4,5,6}).
// 				SkipWhile(func(v interface{}) bool { return v.(int)%3 == 0 }).Results()
//		// arr will be 3,4,5,6
func (this *Queryable) SkipWhile(predicate func(interface{}) bool, chunkSizes ...int) *Queryable {
	mustNotNil(predicate, ErrNilAction)
	//this.act.(predicate predicateFunc)
	this.steps = append(this.steps, commonStep{ACT_SKIPWHILE, PredicateFunc(predicate), getChunkSizeArg(chunkSizes...)})
	return this
}

// Take returns a query includes the Take operation
// Take operation Returns a specified number of contiguous elements
// from the start of a sequence.
//
// Example:
// 	arr, err := From([]int{1,2,3,4,5,6}).Take(3).Results()
//		// arr will be 1,2,3
//
func (this *Queryable) Take(count int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_TAKE, count, 0})
	return this
}

// TakeWhile returns a query includes the TakeWhile operation
// TakeWhile operation returns elements from a sequence as long as a specified condition
// is true, and then skips the remaining elements.
//
// Example:
// 	arr, err := From([]int{1,2,3,4,5,6}).
//				TakeWhile(func(v interface{}) bool { return v.(int)%3 == 0 }).Results()
//		// arr will be 1,2
func (this *Queryable) TakeWhile(predicate func(interface{}) bool, chunkSizes ...int) *Queryable {
	mustNotNil(predicate, ErrNilAction)
	//this.act.(predicate predicateFunc)
	this.steps = append(this.steps, commonStep{ACT_TAKEWHILE, PredicateFunc(predicate), getChunkSizeArg(chunkSizes...)})
	return this
}

// KeepOrder returns a query from the original query,
// the result slice will keep the order of origin query as much as possible
// Noted: Order operation will change the original order
// TODO: Distinct, Union, Join, Interest, Except operations need more testing
func (this *Queryable) SetKeepOrder(keep bool) *Queryable {
	this.KeepOrder = keep
	return this
}

// SetDegreeOfParallelism set the degree of parallelism, it is the
// count of Goroutines when executes the each operations.
// The degree can also be customized in each linq operation function.
func (this *Queryable) SetDegreeOfParallelism(degree int) *Queryable {
	this.Degree = degree
	return this
}

// SetSizeOfChunk set the size of chunk.
// chunk is the data unit of the parallelism, default size is DEFAULTCHUNKSIZE
func (this *Queryable) SetSizeOfChunk(size int) *Queryable {
	this.ChunkSize = size
	return this
}

func (this *Queryable) aggregate(aggregateFuncs ...TwoArgsFunc) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_AGGREGATE, aggregateFuncs, 0})
	return this
}

func (this *Queryable) hGroupBy(keySelector OneArgsFunc, chunkSizes ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_HGROUPBY, keySelector, 0})
	return this
}

// get be used in queryable internal.
// get will executes all linq operations included in Queryable
// and return the result
func (this *Queryable) execute() (data DataSource, err error) {
	//create a goroutines to collect the errors for the pipeline mode step
	stepErrsChan := make(chan error)
	go func() {
		stepFutures := make([]error, 0, len(this.steps))
		if len(this.steps) == 0 {
			this.errChan <- stepFutures
			return
		}

		i := 0
		for e := range stepErrsChan {
			if e != nil && !reflect.ValueOf(e).IsNil() {
				stepFutures = append(stepFutures, e)
			}
			i++
			if i >= len(this.steps) {
				this.errChan <- stepFutures
				return
			}
		}
	}()

	data = this.data
	pOption, keepOrder := this.ParallelOption, this.ParallelOption.KeepOrder

	for i, step := range this.steps {
		var f *promise.Future
		step1 := step

		//execute the operation
		executeStep := func() error {
			defer func() {
				if err := recover(); err != nil {
					//fmt.Println("err in step1----------", i, err)
					stepErrsChan <- NewStepError(i, step1.Typ(), newErrorWithStacks(err))
				}
			}()
			if data, f, keepOrder, err = step.Action()(data, step.POption(pOption), i == 0); err != nil {
				//fmt.Println("err in step2----------", i, err)
				stepErrsChan <- NewStepError(i, step1.Typ(), err)
				for j := i + 1; j < len(this.steps); j++ {
					stepErrsChan <- nil
				}
				return err
			} else if f != nil {
				j := i
				//add a fail callback to collect the errors in pipeline mode
				//because the steps will be paralle in piplline mode,
				//so cannot use return value of the function
				f.Fail(func(results interface{}) {
					//fmt.Println("err in step3----------", j, NewStepError(j, step1.Typ(), results))
					stepErrsChan <- NewStepError(j, step1.Typ(), results)
				}).Done(func(results interface{}) {
					stepErrsChan <- nil
				})
			} else {
				stepErrsChan <- nil
			}
			return nil
		}

		if err := executeStep(); err != nil {
			return nil, err
		}

		//fmt.Println("step=", i, step1.Typ(), "data=", data, "type=", reflect.ValueOf(data).Elem())
		//set the keepOrder for next step
		//some operation will enforce after operations keep the order,
		//e.g OrderBy operation
		pOption.KeepOrder = keepOrder
	}

	return data, nil
}

func (this *Queryable) stepErrs() (err *AggregateError) {
	if errs := <-this.errChan; len(errs) > 0 {
		err = NewAggregateError("Aggregate errors", errs)
	}
	if this.errChan != nil {
		close(this.errChan)
		this.errChan = make(chan []error)
	}
	return
}

func newDataSource(data interface{}) DataSource {
	mustNotNil(data, ErrNilSource)

	if _, ok := data.(Slicer); ok {
		return newListSource(data)
	}
	var ds DataSource
	if v := reflect.ValueOf(data); v.Kind() == reflect.Slice || v.Kind() == reflect.Map {
		ds = newListSource(data) //&listSource{data: data}
	} else if v.Kind() == reflect.Ptr {
		ov := v.Elem()
		if ov.Kind() == reflect.Slice || ov.Kind() == reflect.Map {
			ds = newListSource(data) //&listSource{data: data}
		} else {
			panic(ErrUnsupportSource)
		}
	} else if s, ok := data.(chan *Chunk); ok {
		ds = &chanSource{chunkChan: s}
	} else if v.Kind() == reflect.Chan {
		ds = &chanSource{new(sync.Once), data, nil, nil}
	} else {
		panic(ErrUnsupportSource)
	}
	return ds
}

func newQueryable(ds DataSource) (q *Queryable) {
	q = &Queryable{}
	q.KeepOrder = true
	q.steps = make([]step, 0, 4)
	q.Degree = numCPU
	q.ChunkSize = DEFAULTCHUNKSIZE
	q.errChan = make(chan []error)
	q.data = ds
	return
}

//The listsource and chanSource structs----------------------------------
// listSource presents the slice or map source
type listSource struct {
	//data interface{}
	data Slicer
}

func (this listSource) Typ() int {
	return SOURCE_LIST
}

// ToSlice returns the interface{} slice
func (this listSource) ToSlice(keepOrder bool) Slicer {
	return this.data
}

func (this listSource) ToChan() chan interface{} {
	out := make(chan interface{})
	go func() {
		forEachSlicer(this.data, func(i int, v interface{}) {
			out <- v
		})
		close(out)
	}()
	return out
}

func newListSource(data interface{}) *listSource {
	return &listSource{NewSlicer(data)}
}

// chanSource presents the channel source
// note: the channel must be closed by caller of linq,
// otherwise will be deadlock
type chanSource struct {
	//data1 chan *Chunk
	once      *sync.Once
	data      interface{}
	chunkChan chan *Chunk
	future    *promise.Future
}

func (this chanSource) Typ() int {
	return SOURCE_CHANNEL
}

func sendChunk(out chan *Chunk, c *Chunk) (closed bool) {
	defer func() {
		if e := recover(); e != nil {
			closed = true
		}
	}()
	out <- c
	closed = false
	return
}

// makeChunkChanSure make the channel of chunk for linq operations
// This function will only run once
func (this *chanSource) makeChunkChanSure(chunkSize int) {
	if this.chunkChan == nil {
		this.once.Do(func() {
			if this.chunkChan != nil {
				return
			}
			srcChan := reflect.ValueOf(this.data)
			this.chunkChan = make(chan *Chunk, numCPU)

			this.future = promise.Start(func() (r interface{}, e error) {
				defer func() {
					if err := recover(); err != nil {
						e = newErrorWithStacks(err)
					}
				}()

				chunkData := make([]interface{}, 0, chunkSize)
				lasti, i, order := 0, 0, 0
				for {
					if v, ok := srcChan.Recv(); ok {
						i++
						chunkData = append(chunkData, v.Interface())
						if len(chunkData) == cap(chunkData) {
							c := &Chunk{NewSlicer(chunkData), order, lasti}
							if closed := sendChunk(this.chunkChan, c); closed {
								return nil, nil
							}

							order++
							lasti = i
							chunkData = make([]interface{}, 0, chunkSize)
						}
					} else {
						break
					}
				}

				if len(chunkData) > 0 {
					sendChunk(this.chunkChan, &Chunk{NewSlicer(chunkData), order, lasti})
				}

				//this.Close()
				sendChunk(this.chunkChan, nil)
				return nil, nil
			})

		})
	}
}

func (this *chanSource) ChunkChan(chunkSize int) chan *Chunk {
	this.makeChunkChanSure(chunkSize)
	return this.chunkChan
}

//Close closes the channel of the chunk
func (this chanSource) Close() {
	defer func() {
		_ = recover()
	}()

	if this.chunkChan != nil {
		close(this.chunkChan)
	}
}

//ToSlice returns a slice included all elements in the channel source
func (this chanSource) ToSlice(keepOrder bool) Slicer {
	if this.chunkChan != nil {
		chunks := make([]interface{}, 0, 2)
		ordered := newChunkOrderedList()

		for c := range this.chunkChan {
			if isNil(c) {
				//if use the buffer channel, then must receive a nil as end flag
				if cap(this.chunkChan) > 0 {
					this.Close()
					break
				}
				continue
			}

			if keepOrder {
				ordered.Insert(c)
				chunks = appendToSlice(chunks, c)
			}
		}
		if keepOrder {
			chunks = ordered.ToSlice()
			//fmt.Println("latest chunks :", chunks, ordered.list)
		}

		//fmt.Println("toslice, result1===", chunks)
		return NewSlicer(expandChunks(chunks, false))
	} else {
		srcChan := reflect.ValueOf(this.data)
		if srcChan.Kind() != reflect.Chan {
			panic(ErrUnsupportSource)
		}

		result := make([]interface{}, 0, 10)
		for {
			if v, ok := srcChan.Recv(); ok {
				result = appendToSlice(result, v.Interface())
			} else {
				break
			}
		}
		return NewSlicer(result)
	}
}

//convert to a interface{} channel
func (this chanSource) ToChan() chan interface{} {
	out := make(chan interface{})
	if this.chunkChan != nil {
		go func() {
			for c := range this.chunkChan {
				if isNil(c) {
					if cap(this.chunkChan) > 0 {
						this.Close()
						break
					}
					continue
				}
				forEachSlicer(c.Data, func(i int, v interface{}) {
					out <- v
				})
			}
			close(out)
		}()
	} else if this.data != nil {
		srcChan := reflect.ValueOf(this.data)
		if srcChan.Kind() != reflect.Chan {
			panic(ErrUnsupportSource)
		}

		go func() {
			for {
				if v, ok := srcChan.Recv(); ok {
					out <- v.Interface()
				} else {
					close(out)
					break
				}
			}
		}()
	}
	return out
}

// hKeyValue be used in Distinct, Join, Union/Intersect operations
type hKeyValue struct {
	keyHash interface{}
	key     interface{}
	value   interface{}
}

//the struct and functions of each operation-------------------------------------------------------------------------
const (
	ACT_SELECT int = iota
	ACT_SELECTMANY
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
	ACT_EXCEPT
	ACT_AGGREGATE
	ACT_SKIP
	ACT_SKIPWHILE
	ACT_TAKE
	ACT_TAKEWHILE
	ACT_ELEMENTAT
	ACT_SINGLEVALUE
)

// stepAction presents a action related to a linq operation
// Arguments:
//    DataSource: the data source of the linq operation
//    *ParallelOption: the option of the paralleliam algorithm
//    bool: true if it is first step
// Returns:
//    DataSource: the operation result
//    *promise.Future: if the operation returns the channel,
//         the Future value will be used to get the error if the error appears after the stepAction returns
//    bool: true if the after operation need keep the order of data
//    error: if the operation returns the list mode, it present if the error appears
type stepAction func(DataSource, *ParallelOption, bool) (DataSource, *promise.Future, bool, error)

//step present a linq operation
type step interface {
	Action() stepAction
	Typ() int
	ChunkSize() int
	POption(option ParallelOption) *ParallelOption
}

type commonStep struct {
	typ       int
	act       interface{}
	chunkSize int
}

type joinStep struct {
	commonStep
	outerKeySelector OneArgsFunc
	innerKeySelector OneArgsFunc
	resultSelector   interface{}
	isLeftJoin       bool
}

func (this commonStep) Typ() int       { return this.typ }
func (this commonStep) ChunkSize() int { return this.chunkSize }

func (this commonStep) POption(option ParallelOption) *ParallelOption {
	if this.typ == ACT_DISTINCT || this.typ == ACT_JOIN {
		option.ChunkSize = DEFAULTMINCUNKSIZE
	} else if this.typ == ACT_REVERSE || this.Typ() == ACT_UNION || this.Typ() == ACT_INTERSECT || this.Typ() == ACT_EXCEPT {
		option.ChunkSize = LARGECHUNKSIZE
	}
	if this.chunkSize != 0 {
		option.ChunkSize = this.chunkSize
	}
	return &option
}

func (this commonStep) Action() (act stepAction) {
	switch this.typ {
	case ACT_SELECT:
		act = getSelect(this.act.(OneArgsFunc))
	case ACT_WHERE:
		act = getWhere(this.act.(PredicateFunc))
	case ACT_SELECTMANY:
		act = getSelectMany(this.act.(func(interface{}) []interface{}))
	case ACT_DISTINCT:
		act = getDistinct(this.act.(OneArgsFunc))
	case ACT_ORDERBY:
		act = getOrder(this.act.(CompareFunc))
	case ACT_GROUPBY:
		act = getGroupBy(this.act.(OneArgsFunc), false)
	case ACT_HGROUPBY:
		act = getGroupBy(this.act.(OneArgsFunc), true)
	case ACT_UNION:
		act = getUnion(this.act)
	case ACT_CONCAT:
		act = getConcat(this.act)
	case ACT_INTERSECT:
		act = getIntersect(this.act)
	case ACT_EXCEPT:
		act = getExcept(this.act)
	case ACT_REVERSE:
		act = getReverse()
	case ACT_SKIP:
		act = getSkipTakeCount(this.act.(int), false)
	case ACT_SKIPWHILE:
		act = getSkipTakeWhile(this.act.(PredicateFunc), false)
	case ACT_TAKE:
		act = getSkipTakeCount(this.act.(int), true)
	case ACT_TAKEWHILE:
		act = getSkipTakeWhile(this.act.(PredicateFunc), true)
	}
	return
}

func (this joinStep) Action() (act stepAction) {
	switch this.typ {
	case ACT_JOIN:
		act = getJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(TwoArgsFunc), this.isLeftJoin)
	case ACT_GROUPJOIN:
		act = getGroupJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(func(interface{}, []interface{}) interface{}), this.isLeftJoin)
	}
	return
}

// The functions get linq operation------------------------------------

// Get the action function for select operation
func getSelect(selectFunc OneArgsFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		mapChunk := getChunkOprFunc(mapSliceToSelf, selectFunc)
		if first {
			mapChunk = getChunkOprFunc(mapSlice, selectFunc)
			//mapChunk = getMapChunkFunc(selectFunc)
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
			return list, nil, option.KeepOrder, err
		} else if err != nil {
			//fmt.Println("get error!!!!")
			return nil, nil, option.KeepOrder, err
		}

		if src.Typ() == SOURCE_LIST && src.ToSlice(false).Len() <= option.ChunkSize {
			//fmt.Println("WARNING! parallel for small source, src=", src.ToSlice(false).ToInterfaces())
		}

		f, out := parallelMapToChan(src, nil, mapChunk, option)
		sf, dst, e = f, &chanSource{chunkChan: out}, nil

		return
	})

}

// Get the action function for select operation
func getSelectMany(selectManyFunc func(interface{}) []interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		mapChunk := func(c *Chunk) *Chunk {
			results := mapSliceToMany(c.Data, selectManyFunc)
			return &Chunk{NewSlicer(results), c.Order, c.StartIndex}
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
			return list, nil, option.KeepOrder, err
		}

		f, out := parallelMapToChan(src, nil, mapChunk, option)
		sf, dst, e = f, &chanSource{chunkChan: out}, nil

		return
	})

}

// Get the action function for where operation
func getWhere(predicate PredicateFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		mapChunk := getChunkOprFunc(filterSlice, predicate)
		//try to use sequentail if the size of the data is less than size of chunk
		if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
			return list, nil, option.KeepOrder, err
		}

		//always use channel mode in Where operation
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		return &chanSource{chunkChan: reduceSrc}, f, option.KeepOrder, nil
	})
}

// Get the action function for OrderBy operation
func getOrder(compare CompareFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		defer func() {
			if err := recover(); err != nil {
				e = newErrorWithStacks(err)
			}
		}()
		//order operation be sequentail
		option.Degree = 1

		switch s := src.(type) {
		case *listSource:
			//quick sort
			//TODO:How to avoid the reflect?
			sorteds := sortSlice(s.data.ToInterfaces(), func(this, that interface{}) bool {
				return compare(this, that) == -1
			})
			return newDataSource(sorteds), nil, true, nil
		case *chanSource:
			//AVL tree sort
			avl := newAvlTree(compare)
			f, _ := parallelMapChanToChan(s, nil,
				getChunkOprFunc(forEachSlicer, func(i int, v interface{}) {
					avl.Insert(v)
				}), option)

			dst, e = getFutureResult(f, func(r []interface{}) DataSource {
				return newDataSource(avl.ToSlice())
			})
			keep = true
			return
		}
		panic(ErrUnsupportSource)
	})
}

// Get the action function for DistinctBy operation
func getDistinct(distinctFunc OneArgsFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (DataSource, *promise.Future, bool, error) {
		var useDefHash uint32 = 0
		mapChunk := getMapChunkToKVChunkFunc(&useDefHash, distinctFunc)

		//test the size = 100, trySequentialMap only speed up 10%
		//if list, handled := trySequentialMap(src, &option, mapChunk); handled {
		//	c := &Chunk{list.ToSlice(false), 0}
		//	distKVs := make(map[uint64]int)
		//	c = distinctChunkValues(c, distKVs)
		//	return &listSource{c.Data}, nil, option.KeepOrder, nil
		//}
		//option.ChunkSize = DEFAULTMINCUNKSIZE
		//map the element to a keyValue that key is hash value and value is element
		f, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, option)

		f1, out := reduceDistinctValues(f, reduceSrcChan, option)
		return &chanSource{chunkChan: out}, f1, option.KeepOrder, nil
	})
}

// Get the action function for GroupBy operation
// note the groupby cannot keep order because the map cannot keep order
func getGroupBy(groupFunc OneArgsFunc, hashAsKey bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (DataSource, *promise.Future, bool, error) {

		var useDefHash uint32 = 0
		mapChunk := getMapChunkToKVChunkFunc(&useDefHash, groupFunc)

		//map the element to a keyValue that key is group key and value is element
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		groupKVs := make(map[interface{}]interface{})
		groupKV := func(v interface{}) {
			kv := v.(*hKeyValue)
			k := kv.keyHash

			if v, ok := groupKVs[k]; !ok {
				groupKVs[k] = []interface{}{kv.value}
			} else {
				list := v.([]interface{})
				groupKVs[k] = appendToSlice(list, kv.value)
			}
			//fmt.Println("groupKVs, ", k, v, groupKVs[k])
		}

		//reduce the keyValue map to get grouped slice
		//get key with group values values
		errs := reduceChan(f, reduceSrc, getChunkOprFunc(forEachSlicer, func(i int, v interface{}) {
			groupKV(v)
		}))

		//fmt.Println("groupKVs, return ===", groupKVs)
		if errs == nil {
			return newDataSource(groupKVs), nil, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, NewAggregateError("Group error", errs)
		}

	})
}

// Get the action function for Join operation
// note the Join cannot keep order because the map cannot keep order
func getJoin(inner interface{},
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	resultSelector TwoArgsFunc, isLeftJoin bool) stepAction {
	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *hKeyValue, innerList []interface{}, results *[]interface{}) {
			for _, iv := range innerList {
				*results = appendToSlice(*results, resultSelector(outerkv.value, iv))
			}
		}, func(outerkv *hKeyValue, results *[]interface{}) {
			*results = appendToSlice(*results, resultSelector(outerkv.value, nil))
		}, isLeftJoin)
}

// Get the action function for GroupJoin operation
func getGroupJoin(inner interface{},
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	resultSelector func(interface{}, []interface{}) interface{}, isLeftJoin bool) stepAction {

	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *hKeyValue, innerList []interface{}, results *[]interface{}) {
			*results = appendToSlice(*results, resultSelector(outerkv.value, innerList))
		}, func(outerkv *hKeyValue, results *[]interface{}) {
			*results = appendToSlice(*results, resultSelector(outerkv.value, []interface{}{}))
		}, isLeftJoin)
}

// The common Join function
func getJoinImpl(inner interface{},
	outerKeySelector OneArgsFunc,
	innerKeySelector OneArgsFunc,
	matchSelector func(*hKeyValue, []interface{}, *[]interface{}),
	unmatchSelector func(*hKeyValue, *[]interface{}), isLeftJoin bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		//option.ChunkSize = DEFAULTMINCUNKSIZE
		innerKVtask := promise.Start(func() (interface{}, error) {
			if innerKVsDs, err := From(inner).hGroupBy(innerKeySelector).execute(); err == nil {
				return innerKVsDs.(*listSource).data.(*mapSlicer).data, nil
			} else {
				return nil, err
			}
		})

		var useDefHash uint32 = 0
		//mapChunk := getMapChunkToKVChunkFunc(&useDefHash, outerKeySelector)
		mapChunk := func(c *Chunk) (r *Chunk) {
			outerKVs := getMapChunkToKVs(&useDefHash, outerKeySelector)(c).ToInterfaces()
			results := make([]interface{}, 0, 10)

			if r, err := innerKVtask.Get(); err != nil {
				panic(err)
			} else {
				innerKVs := r.(map[interface{}]interface{})

				for _, o := range outerKVs {
					outerkv := o.(*hKeyValue)
					if innerList, ok := innerKVs[outerkv.keyHash]; ok {
						matchSelector(outerkv, innerList.([]interface{}), &results)
					} else if isLeftJoin {
						unmatchSelector(outerkv, &results)
					}
				}
			}
			//fmt.Println("join map, return, ", c.Order, outerKVs, results)
			return &Chunk{NewSlicer(results), c.Order, c.StartIndex}
		}

		//always use channel mode in Where operation
		f, out := parallelMapToChan(src, nil, mapChunk, option)
		dst, sf, e = &chanSource{chunkChan: out}, f, nil
		return
	})
}

func canSequentialSet(src DataSource, src2 DataSource) bool {
	if src.Typ() == SOURCE_LIST && src2.Typ() == SOURCE_LIST {
		if src.ToSlice(false).Len() <= LARGECHUNKSIZE && src2.ToSlice(false).Len() <= LARGECHUNKSIZE {
			return true
		}
	}
	return false

}

// Get the action function for Union operation
// note the union cannot keep order because the map cannot keep order
func getUnion(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (DataSource, *promise.Future, bool, error) {
		src2 := From(source2).data
		if canSequentialSet(src, src2) {
			return sequentialUnion(src, src2, option, first)
		}
		reduceSrcChan := make(chan *Chunk, 2)
		//if !testCanUseDefaultHash(src, src2){
		var (
			useDefHash uint32
		//maxOrder int
		)

		//mapChunk := getMapChunkToKVChunk2(&useDefHash, &maxOrder, nil)
		mapChunk := getMapChunkToKVChunkFunc(&useDefHash, nil)

		//map the elements of source and source2 to the a KeyValue slice
		//includes the hash value and the original element
		f1, reduceSrcChan := parallelMapToChan(src, reduceSrcChan, mapChunk, option)
		f2, reduceSrcChan := parallelMapToChan(src2, reduceSrcChan, mapChunk, option)

		mapFuture := promise.WhenAll(f1, f2)
		addCallbackToCloseChan(mapFuture, reduceSrcChan)

		f3, out := reduceDistinctValues(mapFuture, reduceSrcChan, option)
		return &chanSource{chunkChan: out}, f3, option.KeepOrder, nil
	})
}

// Get the action function for Union operation
// note the union cannot keep order because the map cannot keep order
func sequentialUnion(src DataSource, src2 DataSource, option *ParallelOption, first bool) (ds DataSource, f *promise.Future, keep bool, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = newErrorWithStacks(err)
			//fmt.Println(e.Error())
		}
	}()
	s2 := src2.ToSlice(false)
	s1 := src.ToSlice(false)

	var useDefHash uint32 = 0
	mapChunk := getMapChunkToKVChunkFunc(&useDefHash, nil)

	c1 := mapChunk(&Chunk{s1, 0, 1})
	c2 := mapChunk(&Chunk{s2, 0, 1})
	//fmt.Println("\n-----c1=", c1, "c2=", c2)
	result := make([]interface{}, 0, s1.Len()+s2.Len())

	distKVs := make(map[interface{}]int)
	//count := 0
	distinctChunkValues(c1, distKVs, &result)
	distinctChunkValues(c2, distKVs, &result)
	//fmt.Println("\n-----result=", result)

	return &listSource{NewSlicer(result)}, nil, option.KeepOrder, nil
}

// Get the action function for Concat operation
func getConcat(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (DataSource, *promise.Future, bool, error) {
		//TODO: if the source is channel source, should use channel mode
		slice1 := src.ToSlice(option.KeepOrder).ToInterfaces()
		if slice2, err2 := From(source2).SetKeepOrder(option.KeepOrder).Results(); err2 == nil {
			result := make([]interface{}, len(slice1)+len(slice2))
			_ = copy(result[0:len(slice1)], slice1)
			_ = copy(result[len(slice1):len(slice1)+len(slice2)], slice2)
			return newDataSource(result), nil, option.KeepOrder, nil
		} else {
			//fmt.Println("concat return error2, ", err2)
			return nil, nil, option.KeepOrder, err2
		}

	})
}

// Get the action function for intersect operation
// note the intersect cannot keep order because the map cannot keep order
func getIntersect(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (result DataSource, f *promise.Future, keep bool, err error) {
		result, f, err = filterSet(src, source2, false, option)

		if err == nil {
			return result, f, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, err
		}
	})
}

// Get the action function for Except operation
// note the except cannot keep order because the map cannot keep order
func getExcept(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (result DataSource, f *promise.Future, keep bool, err error) {
		//result, f, err = filterSetA(src, source2, true, option)
		result, f, err = filterSet(src, source2, true, option)

		if err == nil {
			return result, f, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, err
		}
	})
}

// Get the action function for Reverse operation
func getReverse() stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		var (
			wholeSlice []interface{}
			slicer     Slicer
		)

		if listSrc, ok := src.(*listSource); ok && first {
			wholeSlice = make([]interface{}, listSrc.data.Len())
			slicer = listSrc.data
		} else {
			wholeSlice = src.ToSlice(true).ToInterfaces()
			slicer = NewSlicer(wholeSlice)
		}
		size := slicer.Len()
		srcSlice := slicer.Slice(0, size/2) //wholeSlice[0 : size/2]

		mapChunk := func(c *Chunk) *Chunk {
			forEachSlicer(c.Data, func(i int, v interface{}) {
				j := c.StartIndex + i
				t := slicer.Index(size - 1 - j)
				wholeSlice[size-1-j] = c.Data.Index(i)
				wholeSlice[j] = t
			})
			return c
		}

		reverseSrc := &listSource{NewSlicer(srcSlice)} //newDataSource(srcSlice)
		//fmt.Println("reverseSrc=", reverseSrc.data.ToInterfaces())

		//try to use sequentail if the size of the data is less than size of chunk
		if _, err, handled := trySequentialMap(reverseSrc, option, mapChunk); handled {
			return newDataSource(wholeSlice), nil, option.KeepOrder, err
		}

		f := parallelMapListToList(reverseSrc, func(c *Chunk) *Chunk {
			return mapChunk(c)
		}, option)
		dst, e = getFutureResult(f, func(r []interface{}) DataSource {
			return newDataSource(wholeSlice)
		})
		//fmt.Println("getReverse, wholeSlice2===", wholeSlice)
		return
	})
}

// Get the action function for Skip/Take operation
func getSkipTakeCount(count int, isTake bool) stepAction {
	if count < 0 {
		count = 0
	}
	return getSkipTake(func(c *Chunk, canceller promise.Canceller) (i int, found bool) {
		if c.StartIndex > count {
			i, found = 0, true
		} else if c.StartIndex+c.Data.Len() >= count {
			i, found = count-c.StartIndex, true
		} else {
			i, found = c.StartIndex+c.Data.Len(), false
		}
		return
	}, isTake, true)
}

// Get the action function for SkipWhile/TakeWhile operation
func getSkipTakeWhile(predicate PredicateFunc, isTake bool) stepAction {
	return getSkipTake(foundMatchFunc(invFunc(predicate), true), isTake, false)
}

// note the elementAt cannot keep order because the map cannot keep order
// 根据索引查找单个元素
func getElementAt(src DataSource, i int, option *ParallelOption) (element interface{}, found bool, err error) {
	return getFirstElement(src, func(c *Chunk, canceller promise.Canceller) (int, bool) {
		size := c.Data.Len()
		if c.StartIndex <= i && c.StartIndex+size-1 >= i {
			return i - c.StartIndex, true
		} else {
			return size, false
		}
	}, true, option)
}

// Get the action function for FirstBy operation
// 根据条件查找第一个符合的元素
func getFirstBy(src DataSource, predicate PredicateFunc, option *ParallelOption) (element interface{}, found bool, err error) {
	return getFirstElement(src, foundMatchFunc(predicate, true), false, option)
}

// Get the action function for LastBy operation
// 根据条件查找最后一个符合的元素
func getLastBy(src DataSource, predicate PredicateFunc, option *ParallelOption) (element interface{}, found bool, err error) {
	return getLastElement(src, foundMatchFunc(predicate, false), option)
}

// Get the action function for Aggregate operation
func getAggregate(src DataSource, aggregateFuncs []*AggregateOperation, option *ParallelOption) (result []interface{}, err error) {
	if isNil(aggregateFuncs) || len(aggregateFuncs) == 0 {
		return nil, newErrorWithStacks(errors.New("Aggregation function cannot be nil"))
	}
	keep := option.KeepOrder

	//try to use sequentail if the size of the data is less than size of chunk
	if rs, err, handled := trySequentialAggregate(src, option, aggregateFuncs); handled {
		return rs, err
	}

	rs := make([]interface{}, len(aggregateFuncs))
	mapChunk := func(c *Chunk) (r *Chunk) {
		r = &Chunk{aggregateSlice(c.Data, aggregateFuncs, false, true), c.Order, c.StartIndex}
		return
	}
	f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

	//reduce the keyValue map to get grouped slice
	//get key with group values values
	first := true
	agg := func(c *Chunk) {
		if first {
			for i := 0; i < len(rs); i++ {
				if aggregateFuncs[i].ReduceAction != nil {
					rs[i] = aggregateFuncs[i].Seed
				}
			}
		}
		first = false

		for i := 0; i < len(rs); i++ {
			if aggregateFuncs[i].ReduceAction != nil {
				rs[i] = aggregateFuncs[i].ReduceAction(c.Data.Index(i), rs[i])
			}
		}
	}

	avl := newChunkAvlTree()
	if errs := reduceChan(f, reduceSrc, func(c *Chunk) (r *Chunk) {
		if !keep {
			agg(c)
		} else {
			avl.Insert(c)
		}
		return
	}); errs != nil {
		err = getError(errs)
	}

	if keep {
		cs := avl.ToSlice()
		for _, v := range cs {
			c := v.(*Chunk)
			agg(c)
		}
	}

	if first {
		return rs, newErrorWithStacks(errors.New("cannot aggregate an empty slice"))
	} else {
		return rs, err
	}

}

func getSkipTake(findMatch func(*Chunk, promise.Canceller) (int, bool), isTake bool, useIndex bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, sf *promise.Future, keep bool, e error) {
		switch s := src.(type) {
		case *listSource:
			//rs := s.ToSlice(false)
			var (
				i     int
				found bool
			)
			//如果是根据索引查询列表，可以直接计算，否则要一个个判断
			if useIndex {
				i, _ = findMatch(&Chunk{s.data, 0, 0}, nil)
			} else {
				if i, found, e = getFirstOrLastIndex(s, findMatch, option, true); !found {
					i = s.data.Len()
				}
			}

			//根据Take还是Skip返回结果
			if isTake {
				return newDataSource(s.data.Slice(0, i)), nil, option.KeepOrder, e
			} else {
				return newDataSource(s.data.Slice(i, s.data.Len())), nil, option.KeepOrder, e
			}
		case *chanSource:
			out := make(chan *Chunk, option.Degree)
			sendMatchChunk := func(c *Chunk, idx int) {
				if isTake {
					sendChunk(out, &Chunk{c.Data.Slice(0, idx), c.Order, c.StartIndex})
				} else {
					sendChunk(out, &Chunk{c.Data.Slice(idx, c.Data.Len()), c.Order, c.StartIndex})
				}
			}

			//如果一个块的前置块都已经判断完成时，调用beforeMatchAct
			beforeMatchAct := func(c *chunkMatchResult) (while bool) {
				//如果useIndex，则只有等到前置块都判断完成时才能得出正确的起始索引号，所以在这里才判断是否匹配
				if useIndex {
					if i, found := findMatch(c.chunk, nil); found {
						c.matched = true
						c.matchIndex = i
					}
				}
				if c.matched {
					//如果发现满足条件的item，则必然是第一个满足条件的块
					sendMatchChunk(c.chunk, c.matchIndex)
					return true
				} else if isTake {
					//如果不满足条件，那可以take
					//fmt.Println("send", c.chunk)
					sendChunk(out, c.chunk)
				} else {
					//send a empty slicer is better, because the after operations may include Skip, it need the whole chunk list
					sendChunk(out, &Chunk{NewSlicer([]interface{}{}), c.chunk.Order, c.chunk.StartIndex})
				}
				return false
			}

			//如果一个块在某个匹配块的后面，将调用afterMatchAct，意味着可以作为Skip的输出
			afterMatchAct := func(c *chunkMatchResult) {
				if !isTake {
					sendChunk(out, c.chunk)
				}
			}

			//如果一个块是第一个匹配的块，将调用beMatchAct
			beMatchAct := func(c *chunkMatchResult) {
				sendMatchChunk(c.chunk, c.matchIndex)
			}

			//开始处理channel中的块
			srcChan := s.ChunkChan(option.ChunkSize)
			f := promise.Start(func() (interface{}, error) {
				//avl := newChunkMatchTree(beforeMatchAct, afterMatchAct, beMatchAct, useIndex)
				matchedList := newChunkMatchResultList(beforeMatchAct, afterMatchAct, beMatchAct, useIndex)
				return forEachChanByOrder(s, srcChan, func(c *Chunk, foundFirstMatch *bool) bool {
					if !*foundFirstMatch {
						//检查块是否存在匹配的数据，按Index计算的总是返回false，因为必须要等前面所有的块已经排好序后才能得到正确的索引
						//chunkResult := &chunkMatchResult{chunk: c}
						//if !useIndex {
						//	chunkResult.matchIndex, chunkResult.matched = foundMatch(c, nil)
						//}
						chunkResult := getChunkMatchResult(c, findMatch, useIndex)

						//判断是否找到了第一个匹配的块
						if *foundFirstMatch = matchedList.handleChunk(chunkResult); *foundFirstMatch {
							if isTake {
								s.Close()
								return true
							}
						}
					} else {
						//如果已经找到了第一个匹配的块，则此后的块直接处理即可
						if !isTake {
							sendChunk(out, c)
						} else {
							return true
						}
					}
					return false
				})
			}).Fail(func(err interface{}) {
				s.Close()
			})

			addCallbackToCloseChan(f, out)

			return &chanSource{chunkChan: out}, f, option.KeepOrder, nil
		}
		panic(ErrUnsupportSource)
	})
}

// Get the action function for ElementAt operation
func getFirstElement(src DataSource, findMatch func(c *Chunk, canceller promise.Canceller) (r int, found bool), useIndex bool, option *ParallelOption) (element interface{}, found bool, err error) {
	switch s := src.(type) {
	case *listSource:
		rs := s.data
		if useIndex {
			//使用索引查找列表非常简单，无需并行
			if i, found := findMatch(&Chunk{rs, 0, 0}, nil); found {
				return rs.Index(i), true, nil
			} else {
				return nil, false, nil
			}
		} else {
			//根据数据量大小进行并行或串行查找
			if i, found, err := getFirstOrLastIndex(newListSource(rs), findMatch, option, true); err != nil {
				return nil, false, err
			} else if !found {
				return nil, false, nil
			} else {
				return rs.Index(i), true, nil
			}
		}
	case *chanSource:
		beforeMatchAct := func(c *chunkMatchResult) (while bool) {
			if useIndex {
				//判断是否满足条件
				if idx, found := findMatch(c.chunk, nil); found {
					element = c.chunk.Data.Index(idx)
					return true
				}
			}
			if c.matched {
				element = c.chunk.Data.Index(c.matchIndex)
				return true
			}
			return false
		}
		afterMatchAct, beMatchAct := func(c *chunkMatchResult) {}, func(c *chunkMatchResult) {
			//处理第一个匹配块
			element = c.chunk.Data.Index(c.matchIndex)
		}

		srcChan := s.ChunkChan(option.ChunkSize)
		f := promise.Start(func() (interface{}, error) {
			matchedList := newChunkMatchResultList(beforeMatchAct, afterMatchAct, beMatchAct, useIndex)
			return forEachChanByOrder(s, srcChan, func(c *Chunk, foundFirstMatch *bool) bool {
				if !*foundFirstMatch {
					//chunkResult := &chunkMatchResult{chunk: c}
					//if !useIndex {
					//	chunkResult.matchIndex, chunkResult.matched = foundMatch(c, nil)
					//}
					chunkResult := getChunkMatchResult(c, findMatch, useIndex)
					//fmt.Println("check", c.Data, c.Order, chunkResult, *foundFirstMatch)
					*foundFirstMatch = matchedList.handleChunk(chunkResult)
					//fmt.Println("after check", c.Data, chunkResult, *foundFirstMatch)
					if *foundFirstMatch {
						//element = c.chunk.Data[idx]
						found = true
						s.Close()
						return true
					}
				} else {
					//如果已经找到了正确的块，则此后的块直接跳过
					found = true
					return true
				}
				return false
			})
		})

		if _, err := f.Get(); err != nil {
			s.Close()
			return nil, false, err
		}
		return
	}
	panic(ErrUnsupportSource)
}

func getChunkMatchResult(c *Chunk, findMatch func(c *Chunk, canceller promise.Canceller) (r int, found bool), useIndex bool) (r *chunkMatchResult) {
	r = &chunkMatchResult{chunk: c}
	if !useIndex {
		r.matchIndex, r.matched = findMatch(c, nil)
		//fmt.Println("\nfound no matched---", c, chunkResult.matched)
	}
	return
}

// Get the action function for ElementAt operation
func getLastElement(src DataSource, foundMatch func(c *Chunk, canceller promise.Canceller) (r int, found bool), option *ParallelOption) (element interface{}, found bool, err error) {
	switch s := src.(type) {
	case *listSource:
		rs := s.data
		//根据数据量大小进行并行或串行查找
		if i, found, err := getFirstOrLastIndex(newListSource(rs), foundMatch, option, false); err != nil {
			return nil, false, err
		} else if !found {
			return nil, false, nil
		} else {
			return rs.Index(i), true, nil
		}
	case *chanSource:

		srcChan := s.ChunkChan(option.ChunkSize)
		f := promise.Start(func() (interface{}, error) {
			var r interface{}
			maxOrder := -1
			_, _ = forEachChanByOrder(s, srcChan, func(c *Chunk, foundFirstMatch *bool) bool {
				index, matched := foundMatch(c, nil)
				if matched {
					if c.Order > maxOrder {
						maxOrder = c.Order
						r = c.Data.Index(index)
					}
				}
				return false
			})
			if maxOrder >= 0 {
				element = r
				found = true
			}
			return nil, nil
		})

		if _, err := f.Get(); err != nil {
			s.Close()
			return nil, false, err
		}
	}
	panic(ErrUnsupportSource)
}

//func forEachChanByOrder(s *chanSource, srcChan chan *Chunk,  action func(*Chunk, *bool) bool) (interface{}, error) {
func forEachChanByOrder(s *chanSource, srcChan chan *Chunk, action func(*Chunk, *bool) bool) (interface{}, error) {
	foundFirstMatch := false
	shouldBreak := false
	//Noted the order of sent from source chan maybe confused
	for c := range srcChan {
		if isNil(c) {
			if cap(srcChan) > 0 {
				s.Close()
				break
			} else {
				continue
			}
		}

		if shouldBreak = action(c, &foundFirstMatch); shouldBreak {
			break
		}
	}

	if s.future != nil {
		if _, err := s.future.Get(); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func filterSet(src DataSource, source2 interface{}, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	src2 := newDataSource(source2)

	if canSequentialSet(src, src2) {
		return filterSetByList2(src, src2, isExcept, option)
	}

	switch ds2 := src2.(type) {
	case *listSource:
		return filterSetByList(src, ds2, isExcept, option)
	case *chanSource:
		return filterSetByChan(src, ds2, isExcept, option)
	default:
		panic(ErrUnsupportSource)
	}
}

func toKeyValue(o interface{}) (k interface{}, val interface{}) {
	if kv, ok := o.(*hKeyValue); ok {
		k, val = kv.keyHash, kv.value
	} else {
		k, val = o, o
	}
	return
}

func addDistVal(k interface{}, val interface{},
	distKVs map[interface{}]bool,
	resultKVs map[interface{}]interface{}, isExcept bool) (added bool) {
	_, ok := distKVs[k]
	if (isExcept && !ok) || (!isExcept && ok) {
		if _, ok := resultKVs[k]; !ok {
			resultKVs[k] = val
			added = true
		}
	}
	return
}

func getFilterSetMapFuncs() (mapFunc1, mapFunc2 func(*Chunk) *Chunk) {
	var useDefHash uint32 = 0
	mapFunc1 = getMapChunkToKVChunkFunc(&useDefHash, nil)
	mapFunc2 = func(c *Chunk) (r *Chunk) {
		getResult := func(c *Chunk, useValAsKey bool) Slicer {
			return mapSlice(c.Data, hash64)
		}
		slicer := getMapChunkToKeyList(&useDefHash, nil, getResult)(c)
		return &Chunk{slicer, c.Order, c.StartIndex}
	}
	return
}

func filterSetByChan(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	checkChunkBeEnd := func(c *Chunk, ok bool, closed *bool, anotherClosed bool, ch chan *Chunk) (end bool, broken bool) {
		if isNil(c) || !ok {
			func() {
				defer func() {
					_ = recover()
				}()
				*closed = true
				close(ch)
			}()
			if anotherClosed {
				end = true
			} else {
				broken = true
			}
		}
		return
	}

	mapChunk, mapChunk2 := getFilterSetMapFuncs()
	//map the elements of source and source2 to the a KeyValue slice
	//includes the hash value and the original element
	f1, reduceSrcChan1 := parallelMapToChan(src, nil, mapChunk, option)
	f2, reduceSrcChan2 := parallelMapToChan(src2, nil, mapChunk2, option)

	distKVs1 := make(map[interface{}]bool, 100)
	distKVs2 := make(map[interface{}]bool, 100)
	resultKVs := make(map[interface{}]interface{}, 100)

	close1, close2 := false, false
	//循环2个Channel分别获取src和src2返回的KeyValue集合
	func() {
		for {
			select {
			case c1, ok := <-reduceSrcChan1:
				if end, broken := checkChunkBeEnd(c1, ok, &close1, close2, reduceSrcChan1); end {
					return
				} else if broken {
					break
				}

				forEachSlicer(c1.Data, func(i int, v interface{}) {
					k, val := toKeyValue(v)

					addDistVal(k, val, distKVs2, resultKVs, isExcept)
					if !isExcept {
						if _, ok := distKVs1[k]; !ok {
							distKVs1[k] = true
						}
					}
				})
			case c2, ok := <-reduceSrcChan2:
				if end, broken := checkChunkBeEnd(c2, ok, &close2, close1, reduceSrcChan2); end {
					return
				} else if broken {
					break
				}

				forEachSlicer(c2.Data, func(i int, v interface{}) {
					k := v

					if isExcept {
						delete(resultKVs, k)
					} else {
						if v, ok := distKVs1[k]; ok {
							if _, ok1 := resultKVs[k]; !ok1 {
								resultKVs[k] = v
							}
						}
					}
					if _, ok := distKVs2[k]; !ok {
						distKVs2[k] = true
					}
				})
			}
		}
	}()

	//检查f1和f2的错误
	if _, err1 := f1.Get(); err1 != nil {
		return nil, nil, err1
	} else if _, err2 := f2.Get(); err2 != nil {
		return nil, nil, err2
	}

	//获取结果集
	i, results := 0, make([]interface{}, len(resultKVs))
	for _, v := range resultKVs {
		results[i] = v
		i++
	}
	return newListSource(results), nil, nil

}

func filterSetByList2(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	mapChunk, mapChunk2 := getFilterSetMapFuncs()

	c1 := mapChunk(&Chunk{src.ToSlice(false), 0, 1})
	c2 := mapChunk2(&Chunk{src2.ToSlice(false), 0, 1})

	//获取src2对应的map用于筛选
	distKVs := make(map[interface{}]bool, c2.Data.Len())
	forEachSlicer(c2.Data, func(i int, v interface{}) {
		distKVs[v] = true
	})

	resultKVs := make(map[interface{}]interface{}, c1.Data.Len())
	//过滤src
	count := 0
	size := c1.Data.Len()
	results := make([]interface{}, size)
	mapDistinct := getChunkOprFunc(forEachSlicer, func(i int, v interface{}) {
		k, val := toKeyValue(v)

		if addDistVal(k, val, distKVs, resultKVs, isExcept) {
			results[count] = val
			count++
		}
	})
	_ = mapDistinct(c1)
	return &listSource{NewSlicer(results[0:count])}, nil, nil
}

func filterSetByList(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	ds2 := src2
	mapChunk, mapChunk2 := getFilterSetMapFuncs()
	f1, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, option)
	f2 := parallelMapListToList(ds2, mapChunk2, option)

	var distKVs map[interface{}]bool
	resultKVs := make(map[interface{}]interface{}, 100)
	mapDistinct := func(c *Chunk) *Chunk {
		//获取src2对应的map用于筛选
		if distKVs == nil {
			if rs, err := f2.Get(); err != nil {
				panic(err)
			} else {
				distKVs = make(map[interface{}]bool, 100)
				for _, c := range rs.([]interface{}) {
					chunk := c.(*Chunk)
					forEachSlicer(chunk.Data, func(i int, v interface{}) {
						distKVs[v] = true
					})
				}
			}
		}

		//过滤src
		count := 0
		size := c.Data.Len()
		results := make([]interface{}, size)
		forEachSlicer(c.Data, func(i int, v interface{}) {
			k, val := toKeyValue(v)

			if addDistVal(k, val, distKVs, resultKVs, isExcept) {
				results[count] = val
				count++
			}
		})
		return &Chunk{NewSlicer(results[0:count]), c.Order, c.StartIndex}
	}

	option.Degree = 1
	f, out := parallelMapToChan(&chanSource{chunkChan: reduceSrcChan, future: f1},
		nil, mapDistinct, option)
	return &chanSource{chunkChan: out}, f, nil
}

func getFirstOrLastIndex(src *listSource, predicate func(c *Chunk, canceller promise.Canceller) (r int, found bool), option *ParallelOption, before2after bool) (idx int, found bool, err error) {
	f := parallelMatchListByDirection(src, predicate, option, before2after)
	if i, e := f.Get(); e != nil {
		return -1, false, e
	} else if i == -1 {
		return -1, false, nil
	} else {
		return i.(int), true, nil
	}
}

func foundMatchFunc(predicate PredicateFunc, findFirst bool) func(c *Chunk, canceller promise.Canceller) (r int, found bool) {
	return func(c *Chunk, canceller promise.Canceller) (r int, found bool) {
		r = -1
		size := c.Data.Len()
		i, end := 0, size
		if !findFirst {
			i, end = size-1, -1
		}

		for {
			//for i := 0; i < size; i++ {
			if i == end {
				break
			}
			v := c.Data.Index(i)
			if canceller != nil && canceller.IsCancellationRequested() {
				canceller.SetCancelled()
				break
			}
			if predicate(v) {
				r = i
				//fmt.Println("firstof find", j, )
				found = true
				break
			}
			if findFirst {
				i++
			} else {
				i--
			}
		}
		return
	}
}

//paralleliam functions--------------------------------------------------

func parallelMapToChan(src DataSource, reduceSrcChan chan *Chunk, mapChunk func(c *Chunk) (r *Chunk), option *ParallelOption, startOrders ...int) (f *promise.Future, ch chan *Chunk) {
	//get all values and keys
	switch s := src.(type) {
	case *listSource:
		return parallelMapListToChan(s, reduceSrcChan, mapChunk, option)
	case *chanSource:
		return parallelMapChanToChan(s, reduceSrcChan, mapChunk, option, startOrders...)
	default:
		panic(ErrUnsupportSource)
	}
}

//在某些情况比如Union的操作下，后面的数据源的Order必须接着前面的Order，所以需要更改
func parallelMapChanToChan(src *chanSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption, startOrders ...int) (*promise.Future, chan *Chunk) {
	startOrder := 0
	if startOrders != nil && len(startOrders) > 0 {
		startOrder = startOrders[0]
	}

	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.Degree)
		createOutChan = true
	}

	srcChan := src.ChunkChan(option.ChunkSize)

	fs := make([]*promise.Future, option.Degree)
	for i := 0; i < option.Degree; i++ {
		f := promise.Start(func() (r interface{}, e error) {
			//var cc *Chunk
			//TODO: promise.Start seems cannot capture the error stack?
			defer func() {
				if err := recover(); err != nil {
					e = newErrorWithStacks(err)
					//fmt.Println("parallelMapChanToChan, get error===:", cc, e)
				}
			}()
			for c := range srcChan {
				if !isNil(c) {
					//cc = c
					d := task(c)
					if out != nil && d != nil {
						//fmt.Println("\nparallelMapChanToChan, from=", c.Order, c.Data.ToInterfaces(), "to=", d.Order, d.Data.ToInterfaces())
						d.Order += startOrder
						//out <- d
						sendChunk(out, d)
					}
				} else if cap(srcChan) > 0 {
					src.Close()
					break
				}
			}
			if src.future != nil {
				if _, err := src.future.Get(); err != nil {
					//fmt.Println("parallelMapChanToChan, return 1", nil, err)
					return nil, err
				}
			}
			//fmt.Println("parallelMapChanToChan, return 2")
			return
		})
		fs[i] = f
	}
	f := promise.WhenAll(fs...).Fail(func(err interface{}) { src.Close() })

	if createOutChan {
		addCallbackToCloseChan(f, out)
	}
	return f, out
}

func parallelMapListToChan(src DataSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption) (*promise.Future, chan *Chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.Degree)
		createOutChan = true
	}

	var f *promise.Future
	data := src.ToSlice(false)
	lenOfData := data.Len()
	//fmt.Println("parallelMapListToChan, data=", reflect.ValueOf(data).Type(), data.ToInterfaces())
	if lenOfData == 0 {
		f = promise.Wrap([]interface{}{})
	} else {
		size := option.ChunkSize
		if size < lenOfData/(numCPU*5) {
			size = lenOfData / (numCPU * 5)
		}
		//fmt.Println("parallelMapListToChan, size=", size, "len=", lenOfData)
		ch := make(chan *Chunk, option.Degree)
		go func() {
			for i := 0; i*size < lenOfData; i++ {
				end := (i + 1) * size
				if end >= lenOfData {
					end = lenOfData
				}
				c := &Chunk{data.Slice(i*size, end), i, i * size} //, end}
				//fmt.Println("parallelMapListToChan, send", i, size, data.Slice(i*size, end).ToInterfaces())
				//ch <- c
				sendChunk(ch, c)
			}
			func() {
				defer func() { _ = recover() }()
				close(ch)
			}()
		}()

		cs := &chanSource{chunkChan: ch}
		//fmt.Println("start parallelMapChanToChan")
		f, out = parallelMapChanToChan(cs, out, task, option)
	}
	if createOutChan {
		addCallbackToCloseChan(f, out)
	}
	return f, out

}

func parallelMapListToList(src DataSource, task func(*Chunk) *Chunk, option *ParallelOption) *promise.Future {
	return parallelMapList(src, func(c *Chunk) func() (interface{}, error) {
		return func() (interface{}, error) {
			r := task(c)
			return r, nil
		}
	}, option)
}

func parallelMapList(src DataSource, getAction func(*Chunk) func() (interface{}, error), option *ParallelOption) (f *promise.Future) {
	data := src.ToSlice(false)
	size := data.Len()
	if size == 0 {
		return promise.Wrap([]interface{}{})
	}
	lenOfData, size, j := size, ceilChunkSize(size, option.Degree), 0

	if size < option.ChunkSize {
		size = option.ChunkSize
	}

	fs := make([]*promise.Future, option.Degree)
	for i := 0; i < option.Degree && i*size < lenOfData; i++ {
		end := (i + 1) * size
		if end >= lenOfData {
			end = lenOfData
		}
		c := &Chunk{data.Slice(i*size, end), i, i * size} //, end}

		f = promise.Start(getAction(c))
		fs[i] = f
		j++
	}
	f = promise.WhenAll(fs[0:j]...)

	return
}

func parallelMatchListByDirection(src DataSource, getAction func(*Chunk, promise.Canceller) (int, bool), option *ParallelOption, befor2after bool) (f *promise.Future) {
	data := src.ToSlice(false)
	size := data.Len()
	if size == 0 {
		return promise.Wrap(-1)
	}
	lenOfData, size := size, ceilChunkSize(size, option.Degree)

	if size < option.ChunkSize {
		size = option.ChunkSize
	}

	if size >= lenOfData {
		f := promise.NewPromise()
		func() {
			defer func() {
				if e := recover(); e != nil {
					f.Reject(newErrorWithStacks(e))
				}
			}()
			c := &Chunk{data, 0, 0}
			if r, found := getAction(c, nil); found {
				f.Resolve(r)
			} else {
				f.Resolve(-1)
			}
		}()
		return f.Future
	}

	//生成并行任务来查找符合条件的数据
	fs := make([]*promise.Future, 0, option.Degree)
	for i := 0; i < option.Degree && i*size < lenOfData; i++ {
		end := (i + 1) * size
		if end >= lenOfData {
			end = lenOfData
		}
		c := &Chunk{data.Slice(i*size, end), i, i * size} //, end}

		startIndex := i * size
		f = promise.StartCanCancel(func(canceller promise.Canceller) (interface{}, error) {
			r, found := getAction(c, canceller)
			if found && r != -1 {
				r = r + startIndex
			}
			return r, nil
		})
		fs = append(fs, f)
		//count++
	}

	//根据查找的顺序来检查各任务查找的结果
	f = promise.Start(func() (interface{}, error) {
		var idx interface{}
		rs, errs := make([]interface{}, len(fs)), make([]error, len(fs))
		allOk, hasOk := true, false
		start, end, i := 0, len(fs), 0

		if !befor2after {
			start = len(fs) - 1
			end, i = 0, start
		}

		for {
			f := fs[i]
			//根据查找顺序，如果有Future失败或者找到了数据，则取消后面的Future
			if !allOk || hasOk {
				for j := i; j < len(fs); j++ {
					if c := fs[j].Canceller(); c != nil {
						fs[j].RequestCancel()
					}
				}
				break
			}

			//判断每个Future的结果
			rs[i], errs[i] = f.Get()
			if errs[i] != nil {
				allOk = false
			} else if rs[i].(int) != -1 {
				hasOk = true
			}
			idx = rs[i]

			if befor2after {
				i++
			} else {
				i--
			}

			//所有Future都判断完毕
			if i == end {
				break
			}
		}

		if !allOk {
			return -1, NewAggregateError("Error appears in WhenAll:", errs)
		}

		return idx, nil
	})

	return f
}

//The functions for check if should be Sequential and execute the Sequential mode ----------------------------

//if the data source is listSource, then computer the degree of paralleliam.
//if the degree is 1, the paralleliam is no need.
func singleDegree(src DataSource, option *ParallelOption) bool {
	if s, ok := src.(*listSource); ok {
		list := s.ToSlice(false)
		return list.Len() <= option.ChunkSize
	} else {
		//the channel source will always use paralleliam
		return false
	}
}

func trySequentialMap(src DataSource, option *ParallelOption, mapChunk func(c *Chunk) (r *Chunk)) (ds DataSource, err error, ok bool) {
	defer func() {
		if e := recover(); e != nil {
			err = newErrorWithStacks(e)
		}
	}()
	if useSingle := singleDegree(src, option); useSingle {
		c := &Chunk{src.ToSlice(false), 0, 0}
		r := mapChunk(c)
		//fmt.Println("single", r.Data.Len())
		return newListSource(r.Data), nil, true
	} else {
		return nil, nil, false
	}

}

func trySequentialAggregate(src DataSource, option *ParallelOption, aggregateFuncs []*AggregateOperation) (rs []interface{}, err error, handled bool) {
	defer func() {
		if e := recover(); e != nil {
			err = newErrorWithStacks(e)
			handled = true
			//return nil, newErrorWithStacks(e), true
		}
	}()
	if useSingle := singleDegree(src, option); useSingle || ifMustSequential(aggregateFuncs) {
		if len(aggregateFuncs) == 1 && aggregateFuncs[0] == Count {
			//for count operation, do not need to range the slice
			rs = []interface{}{src.ToSlice(false).Len()}
			return rs, nil, true
		}

		rs = aggregateSlice(src.ToSlice(false), aggregateFuncs, true, true).ToInterfaces()
		return rs, nil, true
	} else {
		return nil, nil, false
	}

}

func ifMustSequential(aggregateFuncs []*AggregateOperation) bool {
	for _, f := range aggregateFuncs {
		if f.ReduceAction == nil {
			return true
		}
	}
	return false
}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceChan(f *promise.Future, src chan *Chunk, reduce func(*Chunk) *Chunk) interface{} {
	for v := range src {
		if v != nil {
			reduce(v)
		} else if cap(src) > 0 {
			close(src)
			break
		}
	}
	if f != nil {
		if _, err := f.Get(); err != nil {
			return err
		}
	}
	return nil
}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceDistinctValues(mapFuture *promise.Future, reduceSrcChan chan *Chunk, option *ParallelOption) (f *promise.Future, out chan *Chunk) {
	//get distinct values
	distKVs := make(map[interface{}]int)
	option.Degree = 1
	return parallelMapChanToChan(&chanSource{chunkChan: reduceSrcChan, future: mapFuture}, nil, func(c *Chunk) *Chunk {

		r := distinctChunkValues(c, distKVs, nil)
		//fmt.Println("\nreduceDistinctValues", c, r.Data)
		return r
	}, option)
}

//util functions-----------------------------------------------------------------
func distinctChunkValues(c *Chunk, distKVs map[interface{}]int, pResults *[]interface{}) *Chunk {
	if pResults == nil {
		size := c.Data.Len()
		result := make([]interface{}, 0, size)
		pResults = &result
	}

	//count := 0
	forEachSlicer(c.Data, func(i int, v interface{}) {
		if kv, ok := v.(*hKeyValue); ok {
			//fmt.Println("distinctChunkValues get==", i, v, kv)
			if _, ok := distKVs[kv.keyHash]; !ok {
				distKVs[kv.keyHash] = 1
				*pResults = append(*pResults, kv.value)
				//result[count] = kv.value
				//count++
			}
		} else {
			if _, ok := distKVs[v]; !ok {
				distKVs[v] = 1
				//*pResults = append(*pResults, kv.value)
				*pResults = append(*pResults, v)
				//result[count] = kv.value
				//count++
			}
		}
	})
	c.Data = NewSlicer(*pResults)
	return c
}

func addCallbackToCloseChan(f *promise.Future, out chan *Chunk) {
	f.Always(func(results interface{}) {
		//must use goroutiner, else may deadlock when out is buffer chan
		//because it maybe called before the chan receiver be started.
		//if the buffer is full, out <- nil will be holder then deadlock
		go func() {
			if out != nil {
				if cap(out) == 0 {
					//fmt.Println("close chan")
					close(out)
				} else {
					//fmt.Println("send nil to chan")
					sendChunk(out, nil)
				}
			}
		}()
	})
}

func getFutureResult(f *promise.Future, dataSourceFunc func([]interface{}) DataSource) (DataSource, error) {
	if results, err := f.Get(); err != nil {
		//todo
		return nil, err
	} else {
		return dataSourceFunc(results.([]interface{})), nil
	}
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
			//dst = appendToSlice(dst, v)
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
	//var dst []interface{}
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
		if (asSequential && fs[j].ReduceAction == nil) || (asParallel && fs[j].ReduceAction != nil) {
			rs[j] = fs[j].Seed
		}
	}

	for i := 0; i < size; i++ {
		for j := 0; j < len(fs); j++ {
			if (asSequential && fs[j].ReduceAction == nil) || (asParallel && fs[j].ReduceAction != nil) {
				rs[j] = fs[j].AggAction(src.Index(i), rs[j])
			}
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
		//fmt.Println("range chunks,", i)
		if c == nil {
			//fmt.Println("range chunk!!!!!!!!!,", i, chunks)
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

func ceilChunkSize(a int, b int) int {
	if a%b != 0 {
		return a/b + 1
	} else {
		return a / b
	}
}

func chunkToKeyValues(c *Chunk, hashAsKey bool, keyFunc func(v interface{}) interface{}, KeyValues *[]interface{}) Slicer {
	return mapSlice(c.Data, func(v interface{}) interface{} {
		if v == nil || keyFunc == nil {
			//fmt.Println("chunkToKeyValues, v=", v, "keyFunc=", keyFunc)
		}
		k := keyFunc(v)
		if hashAsKey {
			return &hKeyValue{hash64(k), k, v}
		} else {
			//fmt.Println("use self as key")
			return &hKeyValue{k, k, v}
		}

	})
}

func iif(predicate bool, trueVal interface{}, falseVal interface{}) interface{} {
	if predicate {
		return trueVal
	} else {
		return falseVal
	}
}

func invFunc(predicate PredicateFunc) PredicateFunc {
	return func(v interface{}) bool {
		return !predicate(v)
	}
}

func getChunkSizeArg(chunkSizes ...int) int {
	chunkSize := 0
	if chunkSizes != nil && len(chunkSizes) > 0 {
		chunkSize = chunkSizes[0]
		if chunkSize == 0 {
			chunkSize = 1
		}
	}
	return chunkSize
}

type chunkMatchResult struct {
	chunk      *Chunk
	matched    bool
	matchIndex int
}

//the ordered list for Skip/SkipWhile/Take/TakeWhile operation----------------------------------------------------
//用来对乱序的channel数据源进行Skip/Take处理的列表
//channel输出的数据在排序后是一个连续的块的列表，而Skip/Take必须根据第一个匹配条件的数据位置进行处理，
//关键在于尽快对得到的块进行判断
//因为数据源可以乱序输出，所以用列表来对得到的块进行排序，chunk.Order就是块在列表中的索引，
//每当从channel中得到一个块时，都会判断是否列表从头开始的部分是否已经被填充，并对所有已经从头构成连续填充的块进行判断，
//判断是否块包含有匹配条件的数据，是否是第一个匹配的块，并通过startOrder记录头开始的顺序号
//如果一个块不在从头开始连续填充的区域中，则判断是否在前面有包含匹配数据的块，如果有的话，则可以执行Skip操作
//列表中始终记录了一个最靠前的匹配块，其后的块可以直接进行Skip操作，无需插入列表
//因为使用了chunk.Order来作为列表的索引并且要从第一块开始判断，
//所以要求所有的linq操作必须返回从0开始的所有的块，即使块不包含有效数据也必须返回空的列表，否则后续Skip/Take的判断会出问题
type chunkMatchResultList struct {
	list            []*chunkMatchResult //保留了待处理块的list
	startOrder      int                 //下一个头块的顺序号
	startIndex      int                 //下一个头块的起始索引号
	matchChunk      *chunkMatchResult   //当前最靠前的包含匹配数据的块
	beforeMatchAct  func(*chunkMatchResult) bool
	afterMatchAct   func(*chunkMatchResult)
	beMatchAct      func(*chunkMatchResult)
	useIndex        bool //是否根据索引进行判断，如果是的话，每个块都必须成为头块后才根据StartIndex进行计算。因为某些操作比如Where将导致块内的数据量不等于原始的数据量
	foundFirstMatch bool //是否已经发现第一个匹配的块
	maxOrder        int  //保存最大的Order,以备Order重复时创建新Order
}

func (this *chunkMatchResultList) Insert(node *chunkMatchResult) {
	order := node.chunk.Order
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
		newList := make([]*chunkMatchResult, order+1, max(2*len(this.list), order+1))
		_ = copy(newList[0:len(this.list)], this.list)
		newList[order] = node
		this.list = newList
	}
	//this.avl.Insert(node)
	if node.matched {
		this.matchChunk = node
	}

}

func (this *chunkMatchResultList) getMatchChunk() *chunkMatchResult {
	return this.matchChunk
}

func (this *chunkMatchResultList) handleChunk(chunkResult *chunkMatchResult) (foundFirstMatch bool) {
	//fmt.Println("check handleChunk=", chunkResult, chunkResult.chunk.Order)
	if chunkResult.matched {
		//如果块中发现匹配的数据
		foundFirstMatch = this.handleMatchChunk(chunkResult)
	} else {
		foundFirstMatch = this.handleNoMatchChunk(chunkResult)
	}
	//fmt.Println("after check handleChunk=", foundFirstMatch)
	return
}

//处理不符合条件的块，返回true表示该块和后续的连续块中发现了原始序列中第一个符合条件的块
//在Skip/Take和管道模式中，块是否不符合条件是在块被放置到正确顺序后才能决定的
func (this *chunkMatchResultList) handleNoMatchChunk(chunkResult *chunkMatchResult) bool {
	//如果不符合条件，则检查当前块order是否等于下一个order，如果是，则进行beforeMatch处理，并更新startOrder
	if chunkResult.chunk.Order == this.startOrder {
		chunkResult.chunk.StartIndex = this.startIndex
		//fmt.Println("call beforeMatchAct=", chunkResult.chunk.Order)
		if found := this.beforeMatchAct(chunkResult); found {
			//fmt.Println("call beforeMatchAct get", find)
			this.foundFirstMatch = true
			if !this.useIndex {
				return true
			}
		}
		this.startOrder += 1
		this.startIndex += chunkResult.chunk.Data.Len()
		//检查list的后续元素中是否还有符合顺序的块
		_ = this.handleOrderedChunks()

		if this.foundFirstMatch {
			return true
		}
	} else {
		//如果不是，则检查是否存在已经匹配的前置块
		if this.matchChunk != nil && this.matchChunk.chunk.Order < chunkResult.chunk.Order {
			//如果存在，则当前块是匹配块之后的块，根据take和skip进行处理
			this.afterMatchAct(chunkResult)
		} else {
			//如果不存在，则插入list
			this.Insert(chunkResult)
		}
	}
	return false
}

//处理符合条件的块，返回true表示是原始序列中第一个符合条件的块
func (this *chunkMatchResultList) handleMatchChunk(chunkResult *chunkMatchResult) bool {
	//检查avl是否存在已经满足条件的块
	if lastWhile := this.getMatchChunk(); lastWhile != nil {
		//如果存在符合的块，则检查当前块是在之前还是之后
		if chunkResult.chunk.Order < lastWhile.chunk.Order {
			//如果是之前，检查avl中所有在当前块之后的块，执行对应的take或while操作
			this.handleChunksAfterMatch(chunkResult)

			//替换原有的匹配块，检查当前块的order是否等于下一个order，如果是，则找到了第一个匹配块，并进行对应处理
			if this.putMatchChunk(chunkResult) {
				return true
			}
		} else {
			//如果是之后，则对当前块执行对应的take或while操作
			this.afterMatchAct(chunkResult)
		}
	} else {
		//如果avl中不存在匹配的块，则检查当前块order是否等于下一个order，如果是，则找到了第一个匹配块，并进行对应处理
		//如果不是下一个order，则插入list，以备后面的检查
		//fmt.Println("发现第一个当前while块")
		if this.putMatchChunk(chunkResult) {
			return true
		}
	}
	return false
}

func (this *chunkMatchResultList) handleChunksAfterMatch(c *chunkMatchResult) {
	result := make([]*chunkMatchResult, 0, 10)
	pResult := &result
	this.forEachAfterChunks(c.chunk.Order, pResult)
	for _, c := range result {
		this.afterMatchAct(c)
	}
}

func (this *chunkMatchResultList) handleOrderedChunks() (foundNotMatch bool) {
	result := make([]*chunkMatchResult, 0, 10)
	pResult := &result
	startOrder := this.startOrder
	foundNotMatch = this.forEachOrderedChunks(startOrder, pResult)
	return
}

func (this *chunkMatchResultList) forEachChunks(currentOrder int, handler func(*chunkMatchResult) (bool, bool), result *[]*chunkMatchResult) bool {
	if result == nil {
		r := make([]*chunkMatchResult, 0, 10)
		result = &r
	}

	if len(this.list) < 1 {
		return false
	}

	for i := currentOrder; i < len(this.list); i++ {
		current := this.list[i]
		if current == nil {
			continue
		}

		//处理节点
		if found, end := handler(current); end {
			return found
		}
	}
	return false
}

//查找list中Order在startOrder之后的块，一直找到发现一个匹配块为止
func (this *chunkMatchResultList) forEachAfterChunks(startOrder int, result *[]*chunkMatchResult) bool {
	return this.forEachChunks(startOrder, func(current *chunkMatchResult) (bool, bool) {
		currentOrder := current.chunk.Order

		if currentOrder >= startOrder {
			*result = append(*result, current)
		}
		//如果当前节点是匹配节点，则结束查找
		if current.matched {
			return true, true
		}
		return false, false
	}, result)
}

//从currentOrder开始查找已经按元素顺序排好的块，一直找到发现一个空缺的位置为止
//如果是Skip/Take，会在查找同时计算块的起始索引，判断是否符合条件。因为每块的长度未必等于原始长度，所以必须在得到正确顺序后才能计算
//如果是SkipWhile/TakeWhile，如果找到第一个符合顺序的匹配块，就会结束查找。因为SkipWhile/TakeWhile的avl中不会有2个匹配的块存在
func (this *chunkMatchResultList) forEachOrderedChunks(currentOrder int, result *[]*chunkMatchResult) bool {
	return this.forEachChunks(currentOrder, func(current *chunkMatchResult) (bool, bool) {
		//fmt.Println("check ordered----", this.startOrder, this.foundFirstMatch, this.useIndex, "chunk =", rootResult.chunk, rootResult)
		currentOrder := current.chunk.Order
		if current.chunk.Order < this.startOrder {
			return false, false
		}
		if this.foundFirstMatch && this.useIndex {
			//前面已经找到了匹配元素，那只有根据index查找才需要判断后面的块,并且所有后面的块都需要返回
			this.afterMatchAct(current)
		} else if currentOrder == this.startOrder { //currentOrder {
			//如果当前节点的order等于指定order，则找到了要遍历的第一个元素
			*result = append(*result, current)
			current.chunk.StartIndex = this.startIndex

			if this.useIndex || !current.matched {
				if find := this.beforeMatchAct(current); find {
					//fmt.Println("find before no matched-------", current.chunk)
					this.foundFirstMatch = true
				}
			}
			this.startOrder += 1
			this.startIndex += current.chunk.Data.Len()
		} else if currentOrder > this.startOrder { //currentOrder {
			//this.startOrder在列表中不存在
			//fmt.Println("check ordered return", false, true)
			return false, true
		}
		//如果是SkipWhile/TakeWhile，并且当前节点是匹配节点，则结束查找
		//如果是Skip/Take，则不能结束
		if current.matched && !this.useIndex {
			this.foundFirstMatch = true
			this.beMatchAct(current)
			//fmt.Println("find while", this.startOrder, this.foundFirstMatch, this.useIndex, rootResult.chunk, rootResult)
			return true, true
		}
		return false, false
	}, result)
}

func (this *chunkMatchResultList) putMatchChunk(c *chunkMatchResult) bool {
	//检查当前块order是否等于下一个order，如果是，则找到了匹配块，并进行对应处理
	//如果不是下一个order，则插入list，以备后面的检查
	if c.chunk.Order == this.startOrder {
		this.beMatchAct(c)
		return true
	} else {
		this.Insert(c)
		return false
	}
}

func newChunkMatchResultList(beforeMatchAct func(*chunkMatchResult) bool, afterMatchAct func(*chunkMatchResult), beMatchAct func(*chunkMatchResult), useIndex bool) *chunkMatchResultList {
	return &chunkMatchResultList{make([]*chunkMatchResult, 0), 0, 0, nil, beforeMatchAct, afterMatchAct, beMatchAct, useIndex, false, -1}
}
