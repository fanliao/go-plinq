/*
Package plinq implements a library for parallel querying and manipulating slice or channel.
A quick start sample:

ch := make(chan int)
go func() {
	for i := 0; i++; i < 10000 { ch <- i }
	close(ch)
}()

rs, err := From(ch).Where(func(v interface{}) bool {
	return v.(int) %2 == 0
}).Select(func(v interface{}) interface{} {
	return v.(int) * 10
}).Results()
*/
package plinq

import (
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"sync"
)

var _ = fmt.Println //for debugger

const (
	SOURCE_LIST    int = iota //presents the list source
	SOURCE_CHANNEL            //presents the channel source
)

var (
	DefaultChunkSize      = 200
	DefaultLargeChunkSize = 2000
)

var (
	ErrUnsupportSource    = errors.New("unsupport DataSource")
	ErrNilSource          = errors.New("datasource cannot be nil")
	ErrUnionNilSource     = errors.New("cannot union nil data source")
	ErrConcatNilSource    = errors.New("cannot concat nil data source")
	ErrInterestNilSource  = errors.New("cannot interest nil data source")
	ErrExceptNilSource    = errors.New("cannot Except nil data source")
	ErrJoinNilSource      = errors.New("cannot join nil data source")
	ErrNilAction          = errors.New("action cannot be nil")
	ErrOuterKeySelector   = errors.New("outerKeySelector cannot be nil")
	ErrInnerKeySelector   = errors.New("innerKeySelector cannot be nil")
	ErrResultSelector     = errors.New("resultSelector cannot be nil")
	ErrTaskFailure        = errors.New("ErrTaskFailure")
	countAggOpr           = getCountByOpr(nil)
)

var numCPU                int
func init() {
	numCPU = runtime.NumCPU()
}

type PredicateFunc func(interface{}) bool
type OneArgsFunc func(interface{}) interface{}
type TwoArgsFunc func(interface{}, interface{}) interface{}
type CompareFunc func(interface{}, interface{}) int

// Comparable presents the type can support the compare operation
type Comparable interface {
	CompareTo(interface{}) int
}

// the struct and interface about data DataSource---------------------------------------------------

// Chunk presents a data chunk, it is the minimal data unit for a task.
type Chunk struct {
	Data       Slicer //[]interface{}
	Order      int    //a index presents the order of chunk
	StartIndex int    //a index presents the start index in whole data
}

// DataSource presents the data of linq operation，
// Most linq operations usually convert a DataSource to another DataSource
type DataSource interface {
	Typ() int                 //list or chan?
	ToSlice(bool) Slicer      //Get a slice includes all datas
	ToChan() chan interface{} //will be implement in futures
}

// KeyValue presents a key value pair, it be used by GroupBy, Join and Set operations
type KeyValue struct {
	Key, Value interface{}
}

//Aggregate operation structs and functions-------------------------------

//AggregateOperation presents the customized aggregate operation.
//It enables intermediate aggregation over a chunk,
//with a final aggregation function to combine the results of all chunks.
//TODO: let user can set the size of chunk for Aggregate operation
type AggregateOperation struct {
	Seed         interface{} //initial seed
	AggAction    TwoArgsFunc //intermediate aggregation over a chunk
	ReduceAction TwoArgsFunc //final aggregation function to combine the results of all chunks
}

// The functions for getting Standard Sum, Count, Min and Max Aggregation operation

// Max return the operation for getting the maximum value. optionally, user can invokes a transform function on each element
func Max(converts ...OneArgsFunc) *AggregateOperation {
	if converts != nil && len(converts) > 0 {
		return getMaxOpr(converts[0])
	}
	return getMaxOpr(nil)
}

// Min return the operation for getting the minimum value. optionally, user can invokes a transform function on each element
func Min(converts ...OneArgsFunc) *AggregateOperation {
	if converts != nil && len(converts) > 0 {
		return getMinOpr(converts[0])
	}
	return getMinOpr(nil)
}

// Sum returns the operation that computes the sum of all elements. optionally, the value can be obtained by invoking a transform function on each element of the input sequence.
func Sum(converts ...OneArgsFunc) *AggregateOperation {
	if converts != nil && len(converts) > 0 {
		return getSumOpr(converts[0])
	}
	return getSumOpr(nil)
}

//Count returns the operation that returns number of elements in the data source.
func Count(predicates ...PredicateFunc) *AggregateOperation {
	if predicates == nil || len(predicates) == 0 {
		return countAggOpr
	} else {
		return getCountByOpr(predicates[0])
	}
}

//the queryable struct-------------------------------------------------------------------------

// ParallelOption presents the options of the paralleliam algorithm.
type ParallelOption struct {
	Degree    int  //The degree of the paralleliam algorithm
	ChunkSize int  //The size of chunk
	KeepOrder bool //whether need keep order of original data
}

// Queryable presents an object includes the data and query operations.
// All query functions will return Queryable.
// For getting the result slice of the query, use Results(). use ToChan() can get a chan presents the result.
type Queryable struct {
	data    DataSource
	steps   []step
	errChan chan []error
	ParallelOption
}

// From initializes a Queryable with slice or channel as the data source.
// input parameter must be a slice or channel. Otherwise panics ErrUnsupportSource.
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
		//在Channel模式下，必须先取到全部的数据，否则stepErrs将死锁
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
// 	ch, errChan, err := From([]interface{}{"Jack", "Rock"}).Select(something).ToChan()
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

// Select returns a query includes the Select operation.
// Select operation projects values that are based on the transform function.
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

// SelectMany returns a query includes the SelectMany operation.
// SelectMany operation projects values that are based on a transform function and
// then flattens them into one slice.
//
// Example:
// 	q := From(users).Select(func (v interface{}) interface{}{
//		return v.(*User).Name
// 	})
func (this *Queryable) SelectMany(selectManyFunc func(interface{}) []interface{}, chunkSizes ...int) *Queryable {
	mustNotNil(selectManyFunc, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_SELECTMANY, selectManyFunc, getChunkSizeArg(chunkSizes...)})
	return this
}

// Distinct returns a query includes the Distinct operation.
// Distinct operation distinct elements from the data source.
//
// Example:
// 	q := From(users).Distinct()
func (this *Queryable) Distinct(chunkSizes ...int) *Queryable {
	return this.DistinctBy(self)
}

// DistinctBy returns a query includes the DistinctBy operation.
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

// OrderBy returns a query includes the OrderBy operation.
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

// GroupBy returns a query includes the GroupBy operation.
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

// Union returns a query includes the Union operation.
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

// Concat returns a query includes the Concat operation.
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

// Intersect returns a query includes the Intersect operation.
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

// Except returns a query includes the Except operation.
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

// Join returns a query includes the Join operation.
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

// LeftJoin returns a query includes the LeftJoin operation.
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

// GroupJoin returns a query includes the GroupJoin operation.
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

// LeftGroupJoin returns a query includes the LeftGroupJoin operation.
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

// Reverse returns a query includes the Reverse operation.
// Reverse operation returns a data source with a inverted order of the original source.
//
// Example:
// 	q := From([]int{1,2,3,4,5}).Reverse()
// 	// q.Results() returns {5,4,3,2,1}
func (this *Queryable) Reverse(chunkSizes ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_REVERSE, nil, getChunkSizeArg(chunkSizes...)})
	return this
}

// Skip returns a query includes the Skip operation.
// Skip operation bypasses a specified number of elements in a sequence
// and then returns the remaining elements.
//
// Example:
// 	arr, err := From([]int{1,2,3,4,5,6}).Skip(3).Results()
//		// arr will be 4, 5, 6
func (this *Queryable) Skip(count int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_SKIP, count, 0})
	return this
}

// SkipWhile returns a query includes the SkipWhile operation.
// SkipWhile operation bypasses elements in a sequence as long as a specified condition
// is true and then returns the remaining elements.
//
// Example:
// 	arr, err := From([]int{1,2,3,4,5,6}).
// 				SkipWhile(func(v interface{}) bool { return v.(int)%3 == 0 }).Results()
//		// arr will be 3,4,5,6
func (this *Queryable) SkipWhile(predicate func(interface{}) bool, chunkSizes ...int) *Queryable {
	mustNotNil(predicate, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_SKIPWHILE, PredicateFunc(predicate), getChunkSizeArg(chunkSizes...)})
	return this
}

// Take returns a query includes the Take operation.
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

// ElementAt returns the element at the specified index i.
// If i is a negative number or if no element exists at i-th index, found will
// be false.
//
// Example:
// i, found, err := From([]int{0,1,2}).ElementAt(2)
//		// i is 2
func (this *Queryable) ElementAt(i int) (result interface{}, found bool, err error) {
	return this.singleValue(func(ds DataSource, pOption *ParallelOption) (result interface{}, found bool, err error) {
		return getElementAt(ds, i, pOption)
	})
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

// Any return true if any element matchs the provided predicate, otherwise return false.
// Example:
// 	found, err := From([]int{0,1,2,3}).Any(func (i interface{})bool{
//		return i.(int) % 2 == 1
// 	})
// 	if err == nil {
//		// found is true
// 	}
func (this *Queryable) Any(predicate PredicateFunc, chunkSizes ...int) (found bool, err error) {
	_, found, err = this.singleValue(func(ds DataSource, pOption *ParallelOption) (result interface{}, found bool, err error) {
		option, chunkSize := this.ParallelOption, getChunkSizeArg(chunkSizes...)
		if chunkSize != 0 {
			option.ChunkSize = chunkSize
		}
		return getAny(ds, predicate, &option)
	})
	return found, err
}

// All return true if all element matchs the provided predicate, otherwise return false.
// Example:
// 	found, err := From([]int{0,1,2,3}).All(func (i interface{})bool{
//		return i.(int) % 2 == 1
// 	})
// 	if err == nil {
//		// found is false
// 	}
func (this *Queryable) All(predicate PredicateFunc, chunkSizes ...int) (found bool, err error) {
	_, found, err = this.singleValue(func(ds DataSource, pOption *ParallelOption) (result interface{}, found bool, err error) {
		option, chunkSize := this.ParallelOption, getChunkSizeArg(chunkSizes...)
		if chunkSize != 0 {
			option.ChunkSize = chunkSize
		}
		return getAny(ds, invFunc(predicate), &option)
	})
	return !found, err
}

// Aggregate returns the results of aggregation operation.
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
	result, _, err = this.singleValue(func(ds DataSource, pOption *ParallelOption) (resultValue interface{}, found bool, e error) {
		rs, err1 := getAggregate(ds, aggregateFuncs, &(this.ParallelOption))
		if err1 != nil {
			e = err1
			return
		}
		if len(aggregateFuncs) == 1 {
			resultValue = rs[0]
		} else {
			resultValue = rs
		}
		return
	})
	return
}

// Sum computes sum of numeric values in the data source.
// Optionally, the value can be obtained by invoking a transform function on each element of the input sequence.
// TODO: If sequence has non-numeric types or nil, should returns an error.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	sum, err := From(arr).Sum() // sum is 18
func (this *Queryable) Sum(converts ...OneArgsFunc) (result interface{}, err error) {
	opr := getSumOpr(nil)
	if converts != nil && len(converts) > 0 {
		opr = getMinOpr(converts[0])
	}
	aggregateOprs := []*AggregateOperation{opr}

	return this.Aggregate(aggregateOprs...)
}

// Count returns number of elements in the data source.
// Optionally, can use a preficate func to filter the element.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	count, err := From(arr).Count() // count is 4
func (this *Queryable) Count(predicates ...PredicateFunc) (result interface{}, err error) {
	aggregateOprs := []*AggregateOperation{Count(predicates...)}

	return this.Aggregate(aggregateOprs...)
}

// Average computes the average of numeric values in the data source.
// Optionally, the value can be obtained by invoking a transform function on each element of the input sequence.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	arg, err := From(arr).Average() // sum is 4.5
func (this *Queryable) Average(converts ...OneArgsFunc) (result interface{}, err error) {
	sumOpr := getSumOpr(nil)
	if converts != nil && len(converts) > 0 {
		sumOpr = getMinOpr(converts[0])
	}
	aggregateOprs := []*AggregateOperation{sumOpr, countAggOpr}

	results, e := this.Aggregate(aggregateOprs...)
	if e != nil {
		return nil, err
	}
	
	count := float64(results.([]interface{})[1].(int))
	sum := results.([]interface{})[0]

	return divide(sum, count), nil
}

// Max returns the maximum value in the data source.
// Max operation supports the numeric types, string and time.Time.
// Optionally, the value can be obtained by invoking a transform function on each element of the input sequence.
// TODO: need more testing for string and time.Time.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	max, err := From(arr).Max() // max is 9
func (this *Queryable) Max(converts ...OneArgsFunc) (result interface{}, err error) {
	opr := getMaxOpr(nil)
	if converts != nil && len(converts) > 0 {
		opr = getMaxOpr(converts[0])
	}

	aggregateOprs := []*AggregateOperation{opr}

	return this.Aggregate(aggregateOprs...)
}

// Min returns the minimum value in the data source.
// Min operation supports the numeric types, string and time.Time.
// Optionally, the value can be obtained by invoking a transform function on each element of the input sequence.
// TODO: need more testing for string and time.Time.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	min, err := From(arr).Max(converts ...OneArgsFunc) // min is 0
func (this *Queryable) Min(converts ...OneArgsFunc) (result interface{}, err error) {
	opr := getMinOpr(nil)
	if converts != nil && len(converts) > 0 {
		opr = getMinOpr(converts[0])
	}

	aggregateOprs := []*AggregateOperation{opr}
	return this.Aggregate(aggregateOprs...)
}

// TakeWhile returns a query includes the TakeWhile operation.
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
// Noted: Order operation will change the original order.
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

// Executes the query and get latest data source
func (this *Queryable) execute() (data DataSource, err error) {
	if len(this.steps) == 0 {
		this.errChan = nil
		return this.data, nil
	} else {
		this.errChan = make(chan []error)
	}

	//create a goroutines to collect the errors for the pipeline mode step
	stepErrsChan := make(chan error)
	go func() {
		stepFutures := make([]error, 0, len(this.steps))

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
	if this.errChan != nil {
		if errs := <-this.errChan; len(errs) > 0 {
			err = NewAggregateError("Aggregate errors", errs)
		}
		close(this.errChan)
	} else {
		return nil
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
	q.ChunkSize = DefaultChunkSize
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
