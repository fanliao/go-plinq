package plinq

import (
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	DEFAULTCHUNKSIZE       = 100
	DEFAULTMINCUNKSIZE     = 20
	SOURCE_LIST        int = iota //presents the list source
	SOURCE_CHUNK                  //presents the channel source
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
	fmt.Println("ptrSize", ptrSize)

	Sum = &AggregateOpretion{0, sumOpr, sumOpr}
	Count = &AggregateOpretion{0, countOpr, sumOpr}
	Min = getMinOpr(defLess)
	Max = getMaxOpr(defLess)
}

type predicateFunc func(interface{}) bool
type oneArgsFunc func(interface{}) interface{}
type twoArgsFunc func(interface{}, interface{}) interface{}

// the struct and interface about data DataSource---------------------------------------------------
// A Chunk presents a data chunk, the paralleliam algorithm always handle a chunk in a paralleliam tasks.
type Chunk struct {
	Data       []interface{}
	Order      int //a index presents the order of chunk
	StartIndex int //a index presents the start index in whole data
}

// The DataSource presents the data of linq operation
// Most linq operations usually convert a DataSource to another DataSource
type DataSource interface {
	Typ() int                   //list or chan?
	ToSlice(bool) []interface{} //Get a slice includes all datas
	ToChan() chan interface{}   //will be implement in futures
}

// KeyValue presents a key value pair, it be used by GroupBy, Join and Set operations
type KeyValue struct {
	Key   interface{}
	Value interface{}
}

//Aggregate operation structs and functions-------------------------------
//
type AggregateOpretion struct {
	Seed         interface{}
	AggAction    twoArgsFunc
	ReduceAction twoArgsFunc
}

// Standard Sum, Count, Min and Max Aggregation operation
var (
	Sum   *AggregateOpretion
	Count *AggregateOpretion
	Min   *AggregateOpretion
	Max   *AggregateOpretion
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
		results = ds.ToSlice(this.KeepOrder)
	}
	//if len(this.stepErrs) > 0 {
	//	err = NewLinqError("Aggregate errors", this.stepErrs)
	//}
	err = this.stepErrs()
	if err != nil {
		results = nil
	}

	return
}

// ToChan evaluates the query and returns the results as interface{} channel.
// If the error occurred in during evaluation of the query, it will be returned.
//
// Example:
// 	ch, err := From([]interface{}{"Jack", "Rock"}).Select(something).toChan
func (this *Queryable) ToChan() (out chan interface{}, err error) {
	if ds, e := this.execute(); e == nil {
		out = ds.ToChan()
	}

	err = this.stepErrs()
	if err != nil {
		out = nil
	}

	return
}

// ElementAt returns the element at the specified index i.
// If i is a negative number or if no element exists at i-th index, found will
// be returned false.
//
// Example:
// i, found, err := From([]int{0,1,2}).ElementAt(2)
//		// i is 2
func (this *Queryable) ElementAt(i int) (result interface{}, found bool, err error) {
	if ds, e := this.execute(); e == nil {
		if err = this.stepErrs(); err != nil {
			return nil, false, err
		}

		return getElementAt(ds, i, &(this.ParallelOption))
	} else {
		return nil, false, e
	}
}

func (this *Queryable) FirstOf(predicate predicateFunc) (result interface{}, found bool, err error) {
	if ds, e := this.execute(); e == nil {
		if err = this.stepErrs(); err != nil {
			return nil, false, err
		}

		return getFirstOf(ds, predicate, &(this.ParallelOption))
	} else {
		return nil, false, e
	}
}

// Aggregate returns the results of aggregation operation
// Aggregation operation aggregates the result in the data source base on the AggregateOpretion.
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
// TODO: the customized aggregation function
func (this *Queryable) Aggregate(aggregateFuncs ...*AggregateOpretion) (result interface{}, err error) {
	if ds, e := this.execute(); e == nil {
		if err = this.stepErrs(); err != nil {
			return nil, err
		}

		results, e := getAggregate(ds, aggregateFuncs, &(this.ParallelOption))
		if e != nil {
			return nil, e
		}
		if len(aggregateFuncs) == 1 {
			result = results[0]
		} else {
			result = results
		}
		return
	} else {
		return nil, e
	}
}

// Sum computes sum of numeric values in the data source.
// TODO: If sequence has non-numeric types or nil, should returns an error.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	sum, err := From(arr).Sum() // sum is 18
func (this *Queryable) Sum() (result interface{}, err error) {
	aggregateOprs := []*AggregateOpretion{Sum}

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
	aggregateOprs := []*AggregateOpretion{Count}

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
func (this *Queryable) CountBy(predicate predicateFunc) (result interface{}, err error) {
	if predicate == nil {
		predicate = predicateFunc(func(interface{}) bool { return true })
	}
	aggregateOprs := []*AggregateOpretion{getCountByOpr(predicate)}

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
	aggregateOprs := []*AggregateOpretion{Sum, Count}

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

	aggregateOprs := []*AggregateOpretion{getMaxOpr(less)}

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

	aggregateOprs := []*AggregateOpretion{getMinOpr(less)}

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
func (this *Queryable) Where(predicate predicateFunc, degrees ...int) *Queryable {
	mustNotNil(predicate, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_WHERE, predicate, getDegreeArg(degrees...)})
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
func (this *Queryable) Select(selectFunc oneArgsFunc, degrees ...int) *Queryable {
	mustNotNil(selectFunc, ErrNilAction)

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
// Noted: The before element may be filter in parallel mode, so cannot keep order
//
// Example:
// 	q := From(user).DistinctBy(func (p interface{}) interface{}{
//		return p.(*Person).FirstName
// 	})
func (this *Queryable) DistinctBy(distinctFunc oneArgsFunc, degrees ...int) *Queryable {
	mustNotNil(distinctFunc, ErrNilAction)
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
func (this *Queryable) GroupBy(keySelector oneArgsFunc, degrees ...int) *Queryable {
	mustNotNil(keySelector, ErrNilAction)

	this.steps = append(this.steps, commonStep{ACT_GROUPBY, keySelector, getDegreeArg(degrees...)})
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
func (this *Queryable) Union(source2 interface{}, degrees ...int) *Queryable {
	mustNotNil(source2, ErrUnionNilSource)

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
func (this *Queryable) Intersect(source2 interface{}, degrees ...int) *Queryable {
	mustNotNil(source2, ErrInterestNilSource)

	this.steps = append(this.steps, commonStep{ACT_INTERSECT, source2, getDegreeArg(degrees...)})
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
func (this *Queryable) Except(source2 interface{}, degrees ...int) *Queryable {
	mustNotNil(source2, ErrExceptNilSource)

	this.steps = append(this.steps, commonStep{ACT_EXCEPT, source2, getDegreeArg(degrees...)})
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
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	resultSelector twoArgsFunc, degrees ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

// LeftJoin returns a query includes the LeftJoin operation
// LeftJoin operation is similar with Join operation,
// but LeftJoin returns all elements in outer source,
// the inner elements will be null if there is not matching element in inner source
func (this *Queryable) LeftJoin(inner interface{},
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	resultSelector twoArgsFunc, degrees ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

// GroupJoin returns a query includes the GroupJoin operation
// GroupJoin operation is similar with Join operation,
// but GroupJoin will correlates the element of the outer source and
// the matching elements slice of the inner source.
func (this *Queryable) GroupJoin(inner interface{},
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	resultSelector func(interface{}, []interface{}) interface{}, degrees ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

// LeftGroupJoin returns a query includes the LeftGroupJoin operation
// LeftGroupJoin operation is similar with GroupJoin operation,
// but LeftGroupJoin returns all elements in outer source,
// the inner elements will be [] if there is not matching element in inner source
func (this *Queryable) LeftGroupJoin(inner interface{},
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	resultSelector func(interface{}, []interface{}) interface{}, degrees ...int) *Queryable {
	mustNotNil(inner, ErrJoinNilSource)
	mustNotNil(outerKeySelector, ErrOuterKeySelector)
	mustNotNil(innerKeySelector, ErrInnerKeySelector)
	mustNotNil(resultSelector, ErrResultSelector)

	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, getDegreeArg(degrees...)}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

// Reverse returns a query includes the Reverse operation
// Reverse operation returns a data source with a inverted order of the original source
//
// Example:
// 	q := From([]int{1,2,3,4,5}).Reverse()
// 	// q.Results() returns {5,4,3,2,1}
func (this *Queryable) Reverse(degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_REVERSE, nil, getDegreeArg(degrees...)})
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
func (this *Queryable) SkipWhile(predicate func(interface{}) bool) *Queryable {
	mustNotNil(predicate, ErrNilAction)
	//this.act.(predicate predicateFunc)
	this.steps = append(this.steps, commonStep{ACT_SKIPWHILE, predicateFunc(predicate), 0})
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
func (this *Queryable) TakeWhile(predicate func(interface{}) bool) *Queryable {
	mustNotNil(predicate, ErrNilAction)
	//this.act.(predicate predicateFunc)
	this.steps = append(this.steps, commonStep{ACT_TAKEWHILE, predicateFunc(predicate), 0})
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

func (this *Queryable) aggregate(aggregateFuncs ...twoArgsFunc) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_AGGREGATE, aggregateFuncs, 0})
	return this
}

func (this *Queryable) hGroupBy(keySelector oneArgsFunc, degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_HGROUPBY, keySelector, 0})
	return this
}

func (this *Queryable) makeCopiedSrc(force bool) DataSource {
	copySrc := func() DataSource {
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
		return this.data
	}
	if len(this.steps) > 0 {
		if typ := this.steps[0].Typ(); typ == ACT_REVERSE || typ == ACT_SELECT || typ == ACT_INTERSECT || typ == ACT_EXCEPT {
			return copySrc()
		}
	} else if force {
		return copySrc()
	}
	return this.data
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

	data = this.makeCopiedSrc(false) //data
	pOption, keepOrder := this.ParallelOption, this.ParallelOption.KeepOrder

	for i, step := range this.steps {
		var f *promise.Future
		step1 := step

		executeStep := func() error {
			defer func() {
				if err := recover(); err != nil {
					stepErrsChan <- NewStepError(i, step1.Typ(), newErrorWithStacks(err))
				}
			}()
			//execute the step
			if data, f, keepOrder, err = step.Action()(data, step.POption(pOption)); err != nil {
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

		//set the keepOrder for next step
		//some operation will enforce after operations keep the order,
		//e.g OrderBy operation
		pOption.KeepOrder = keepOrder
	}

	return data, nil
}

func (this *Queryable) stepErrs() (err error) {
	if errs := <-this.errChan; len(errs) > 0 {
		err = NewLinqError("Aggregate errors", errs)
	}
	if this.errChan != nil {
		close(this.errChan)
		this.errChan = make(chan []error)
	}
	return
}

func newDataSource(data interface{}) DataSource {
	mustNotNil(data, ErrNilSource)

	var ds DataSource
	if v := reflect.ValueOf(data); v.Kind() == reflect.Slice || v.Kind() == reflect.Map {
		ds = &listSource{data: data}
	} else if v.Kind() == reflect.Ptr {
		ov := v.Elem()
		if ov.Kind() == reflect.Slice || ov.Kind() == reflect.Map {
			ds = &listSource{data: data}
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
	data interface{}
}

func (this listSource) Typ() int {
	return SOURCE_LIST
}

// ToSlice returns the interface{} slice
func (this listSource) ToSlice(keepOrder bool) []interface{} {
	switch data := this.data.(type) {
	case []interface{}:
		if data != nil && len(data) > 0 {
			if _, ok := data[0].(*Chunk); ok {
				return expandChunks(data, keepOrder)
			}
		}
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
		return getSlice(value)

		//return nil

	}
	return nil
}

func (this listSource) ToChan() chan interface{} {
	out := make(chan interface{})
	go func() {
		for _, v := range this.ToSlice(false) {
			out <- v
		}
		close(out)
	}()
	return out
}

func getSlice(v reflect.Value) []interface{} {
	switch v.Kind() {
	case reflect.Slice:
		size := v.Len()
		results := make([]interface{}, size)
		for i := 0; i < size; i++ {
			results[i] = v.Index(i).Interface()
		}
		return results
	case reflect.Map:
		size := v.Len()
		results := make([]interface{}, size)
		for i, k := range v.MapKeys() {
			results[i] = &KeyValue{k.Interface(), v.MapIndex(k).Interface()}
		}
		return results
	case reflect.Ptr:
		ov := v.Elem()
		return getSlice(ov)
	}
	return nil
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
	//chunkSize int
}

func (this chanSource) Typ() int {
	return SOURCE_CHUNK
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
			this.chunkChan = make(chan *Chunk)

			this.future = promise.Start(func() (r interface{}, e error) {
				defer func() {
					if err := recover(); err != nil {
						e = newErrorWithStacks(err)
					}
				}()

				chunkData := make([]interface{}, 0, chunkSize)
				lasti, i, j := 0, 0, 0
				for {
					if v, ok := srcChan.Recv(); ok {
						i++
						chunkData = append(chunkData, v.Interface())
						if len(chunkData) == cap(chunkData) {
							c := &Chunk{chunkData, j, lasti}
							if closed := sendChunk(this.chunkChan, c); closed {
								return nil, nil
							}

							j++
							lasti = i
							chunkData = make([]interface{}, 0, chunkSize)
						}
					} else {
						break
					}
				}

				if len(chunkData) > 0 {
					sendChunk(this.chunkChan, &Chunk{chunkData, j, lasti})
				}

				close(this.chunkChan)
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
		if e := recover(); e != nil {
			_ = e
		}
	}()

	if this.chunkChan != nil {
		close(this.chunkChan)
	}
}

//ToSlice returns a slice included all elements in the channel source
func (this chanSource) ToSlice(keepOrder bool) []interface{} {
	if this.chunkChan != nil {
		chunks := make([]interface{}, 0, 2)
		avl := newChunkAvlTree()

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
				avl.Insert(c)
			} else {
				chunks = appendSlice(chunks, c)
			}
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

				for _, v := range c.Data {
					out <- v
				}
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
	ACT_EXCEPT
	ACT_AGGREGATE
	ACT_SKIP
	ACT_SKIPWHILE
	ACT_TAKE
	ACT_TAKEWHILE
	ACT_ELEMENTAT
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
	outerKeySelector oneArgsFunc
	innerKeySelector oneArgsFunc
	resultSelector   interface{}
	isLeftJoin       bool
}

func (this commonStep) Typ() int    { return this.typ }
func (this commonStep) Degree() int { return this.degree }
func (this commonStep) POption(option ParallelOption) *ParallelOption {
	if this.degree != 0 {
		option.Degree = this.degree
	}
	return &option
}

func (this commonStep) Action() (act stepAction) {
	switch this.typ {
	case ACT_SELECT:
		act = getSelect(this.act.(oneArgsFunc))
	case ACT_WHERE:
		act = getWhere(this.act.(predicateFunc))
	case ACT_DISTINCT:
		act = getDistinct(this.act.(oneArgsFunc))
	case ACT_ORDERBY:
		act = getOrder(this.act.(func(interface{}, interface{}) int))
	case ACT_GROUPBY:
		act = getGroupBy(this.act.(oneArgsFunc), false)
	case ACT_HGROUPBY:
		act = getGroupBy(this.act.(oneArgsFunc), true)
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
		act = getSkipTakeWhile(this.act.(predicateFunc), false)
	case ACT_TAKE:
		act = getSkipTakeCount(this.act.(int), true)
	case ACT_TAKEWHILE:
		act = getSkipTakeWhile(this.act.(predicateFunc), true)
	}
	return
}

func (this joinStep) Action() (act stepAction) {
	switch this.typ {
	case ACT_JOIN:
		act = getJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(twoArgsFunc), this.isLeftJoin)
	case ACT_GROUPJOIN:
		act = getGroupJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(func(interface{}, []interface{}) interface{}), this.isLeftJoin)
	}
	return
}

// The functions get linq operation------------------------------------
func getSelect(selectFunc oneArgsFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		mapChunk := func(c *Chunk) *Chunk {
			mapSlice(c.Data, selectFunc, &(c.Data))
			return c
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

func getWhere(predicate predicateFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = filterChunk(c, predicate)
			return
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
			return list, nil, option.KeepOrder, err
		}

		//always use channel mode in Where operation
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		return &chanSource{chunkChan: reduceSrc}, f, option.KeepOrder, nil
	})
}

func getOrder(compare func(interface{}, interface{}) int) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
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
			sorteds := sortSlice(s.ToSlice(false), func(this, that interface{}) bool {
				return compare(this, that) == -1
			})
			return &listSource{sorteds}, nil, true, nil
		case *chanSource:
			//AVL tree sort
			avl := NewAvlTree(compare)
			f, _ := parallelMapChanToChan(s, nil, func(c *Chunk) *Chunk {
				for _, v := range c.Data {
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

func getDistinct(distinctFunc oneArgsFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, distinctFunc, nil), c.Order, c.StartIndex}
			return
		}

		//test the size = 100, trySequentialMap only speed up 10%
		//if list, handled := trySequentialMap(src, &option, mapChunk); handled {
		//	c := &Chunk{list.ToSlice(false), 0}
		//	distKVs := make(map[uint64]int)
		//	c = distinctChunkVals(c, distKVs)
		//	return &listSource{c.Data}, nil, option.KeepOrder, nil
		//}
		option.ChunkSize = DEFAULTMINCUNKSIZE
		//map the element to a keyValue that key is hash value and value is element
		f, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, option)

		f1, out := reduceDistinctVals(f, reduceSrcChan, option)
		return &chanSource{chunkChan: out}, f1, option.KeepOrder, nil
	})
}

//note the groupby cannot keep order because the map cannot keep order
func getGroupBy(groupFunc oneArgsFunc, hashAsKey bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, groupFunc, nil), c.Order, c.StartIndex}
			return
		}

		//map the element to a keyValue that key is group key and value is element
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		groupKVs := make(map[interface{}]interface{})
		groupKV := func(v interface{}) {
			kv := v.(*hKeyValue)
			k := iif(hashAsKey, kv.keyHash, kv.key)
			if v, ok := groupKVs[k]; !ok {
				groupKVs[k] = []interface{}{kv.value}
			} else {
				list := v.([]interface{})
				groupKVs[k] = appendSlice(list, kv.value)
			}
		}

		//reduce the keyValue map to get grouped slice
		//get key with group values values
		errs := reduceChan(f, reduceSrc, func(c *Chunk) {
			for _, v := range c.Data {
				groupKV(v)
			}
		})

		if errs == nil {
			return &listSource{groupKVs}, nil, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, NewLinqError("Group error", errs)
		}

	})
}

func getJoin(inner interface{},
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	resultSelector twoArgsFunc, isLeftJoin bool) stepAction {
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
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	resultSelector func(interface{}, []interface{}) interface{}, isLeftJoin bool) stepAction {

	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *hKeyValue, innerList []interface{}, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, innerList))
		}, func(outerkv *hKeyValue, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, []interface{}{}))
		}, isLeftJoin)
}

func getJoinImpl(inner interface{},
	outerKeySelector oneArgsFunc,
	innerKeySelector oneArgsFunc,
	matchSelector func(*hKeyValue, []interface{}, *[]interface{}),
	unmatchSelector func(*hKeyValue, *[]interface{}), isLeftJoin bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		option.ChunkSize = DEFAULTMINCUNKSIZE
		innerKVtask := promise.Start(func() (interface{}, error) {
			if innerKVsDs, err := From(inner).hGroupBy(innerKeySelector).execute(); err == nil {
				return innerKVsDs.(*listSource).data, nil
			} else {
				return nil, err
			}
		})

		mapChunk := func(c *Chunk) (r *Chunk) {
			outerKVs := getKeyValues(c, outerKeySelector, nil)
			results := make([]interface{}, 0, 10)

			if r, err := innerKVtask.Get(); err != nil {
				//todo:
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

			return &Chunk{results, c.Order, c.StartIndex}
		}

		//always use channel mode in Where operation
		f, out := parallelMapToChan(src, nil, mapChunk, option)
		dst, sf, e = &chanSource{chunkChan: out}, f, nil
		return
	})
}

func getUnion(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		reduceSrcChan := make(chan *Chunk)
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.Order, c.StartIndex}
			return
		}

		//map the elements of source and source2 to the a KeyValue slice
		//includes the hash value and the original element
		f1, reduceSrcChan := parallelMapToChan(src, reduceSrcChan, mapChunk, option)
		f2, reduceSrcChan := parallelMapToChan(From(source2).data, reduceSrcChan, mapChunk, option)

		mapFuture := promise.WhenAll(f1, f2)
		addCloseChanCallback(mapFuture, reduceSrcChan)

		f3, out := reduceDistinctVals(mapFuture, reduceSrcChan, option)
		return &chanSource{chunkChan: out}, f3, option.KeepOrder, nil
	})
}

func getConcat(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		//TODO: if the source is channel source, should use channel mode
		slice1 := src.ToSlice(option.KeepOrder)
		if slice2, err2 := From(source2).SetKeepOrder(option.KeepOrder).Results(); err2 == nil {
			result := make([]interface{}, len(slice1)+len(slice2))
			_ = copy(result[0:len(slice1)], slice1)
			_ = copy(result[len(slice1):len(slice1)+len(slice2)], slice2)
			return &listSource{result}, nil, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, err2
		}

	})
}

func getIntersect(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (result DataSource, f *promise.Future, keep bool, err error) {
		result, f, err = filterSet(src, source2, false, option)

		if err == nil {
			return result, f, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, err
		}
	})
}

func getExcept(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (result DataSource, f *promise.Future, keep bool, err error) {
		//result, f, err = filterSetA(src, source2, true, option)
		result, f, err = filterSet(src, source2, true, option)

		if err == nil {
			return result, f, option.KeepOrder, nil
		} else {
			return nil, nil, option.KeepOrder, err
		}
	})
}

func getReverse() stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.KeepOrder
		wholeSlice := src.ToSlice(true)
		srcSlice, size := wholeSlice[0:len(wholeSlice)/2], len(wholeSlice)

		mapChunk := func(c *Chunk) *Chunk {
			for i := 0; i < len(c.Data); i++ {
				j := c.StartIndex + i
				t := wholeSlice[size-1-j]
				wholeSlice[size-1-j] = c.Data[i]
				c.Data[i] = t
			}
			return c
		}

		reverseSrc := &listSource{srcSlice}

		//try to use sequentail if the size of the data is less than size of chunk
		if _, err, handled := trySequentialMap(reverseSrc, option, mapChunk); handled {
			return &listSource{wholeSlice}, nil, option.KeepOrder, err
		}

		f := parallelMapListToList(reverseSrc, func(c *Chunk) *Chunk {
			return mapChunk(c)
		}, option)
		dst, e = getFutureResult(f, func(r []interface{}) DataSource {
			return &listSource{wholeSlice}
		})
		return
	})
}

func getSkipTakeCount(count int, isTake bool) stepAction {
	if count < 0 {
		count = 0
	}
	return getSkipTake(func(c *Chunk) (i int, find bool) {
		if c.StartIndex > count {
			i, find = 0, true
		} else if c.StartIndex+len(c.Data) >= count {
			i, find = count-c.StartIndex, true
		} else {
			i, find = c.StartIndex+len(c.Data), false
		}
		return
	}, isTake, true)
}

func getSkipTakeWhile(predicate predicateFunc, isTake bool) stepAction {
	return getSkipTake(func(c *Chunk) (int, bool) {
		rs := c.Data
		for i, v := range rs {
			if !predicate(v) {
				return i, true
			}
		}
		return len(rs), false
	}, isTake, false)
}

type chunkWhileResult struct {
	chunk    *Chunk
	noMatch  bool
	whileIdx int
}

func getSkipTake(foundNoMatch func(*Chunk) (int, bool), isTake bool, useIndex bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		switch s := src.(type) {
		case *listSource:
			rs := s.ToSlice(false)
			var i int
			i, _ = foundNoMatch(&Chunk{rs, 0, 0})

			if isTake {
				return &listSource{rs[0:i]}, nil, option.KeepOrder, nil
			} else {
				return &listSource{rs[i:]}, nil, option.KeepOrder, nil
			}
		case *chanSource:
			out := make(chan *Chunk)
			//如果一个块的前置块都已经判断完成，而自身未发现不匹配数据时，调用beforeNoMatchAct
			beforeNoMatchAct := func(c *chunkWhileResult) (while bool) {
				//如果useIndex，则只有等到前置块都判断完成时才能得出正确的起始索引号，所以在这里判断是否不匹配
				if useIndex {
					if i, found := foundNoMatch(c.chunk); found {
						c.noMatch = true
						c.whileIdx = i
					}
				}

				if c.noMatch {
					//如果发现不满足条件的item，则必然是第一个不满足条件的块
					if isTake {
						sendChunk(out, &Chunk{c.chunk.Data[0:c.whileIdx], c.chunk.Order, c.chunk.StartIndex})
					} else {
						sendChunk(out, &Chunk{c.chunk.Data[c.whileIdx:], c.chunk.Order, c.chunk.StartIndex})
					}
					return true
				} else if isTake {
					//如果没有发现不满足条件的，那可以take
					//fmt.Println("send", c.chunk)
					sendChunk(out, c.chunk)
				}
				return false
			}

			//如果一个块在某个发现了不匹配数据的块的后面，将调用afterNoMatchAct，意味着可以作为Skip的输出
			afterNoMatchAct := func(c *chunkWhileResult) {
				if !isTake {
					sendChunk(out, c.chunk)
				}
			}

			//如果一个块是第一个发现不匹配的块，将调用beNoMatchAct
			beNoMatchAct := func(c *chunkWhileResult) {
				if isTake {
					sendChunk(out, &Chunk{c.chunk.Data[0:c.whileIdx], c.chunk.Order, c.chunk.StartIndex})
				} else {
					sendChunk(out, &Chunk{c.chunk.Data[c.whileIdx:], c.chunk.Order, c.chunk.StartIndex})
				}
			}

			//开始处理channel中的块
			srcChan := s.ChunkChan(option.ChunkSize)
			f := promise.Start(func() (interface{}, error) {
				foundFirstNoMatch := false
				avl := newChunkWhileResultTree(beforeNoMatchAct, afterNoMatchAct, beNoMatchAct, useIndex)

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

					if !foundFirstNoMatch {
						//检查块是否存在不匹配的数据，按Index计算的总是返回flase，因为必须要等前面的块准备好才能得到正确的索引
						chunkResult := &chunkWhileResult{chunk: c}
						if !useIndex {
							chunkResult.whileIdx, chunkResult.noMatch = foundNoMatch(c)
							//fmt.Println("\nfound no match---", c, chunkResult.noMatch)
						}

						//判断是否找到了第一个不匹配的块
						if foundFirstNoMatch = avl.handleChunk(chunkResult); foundFirstNoMatch {
							if isTake {
								s.Close()
								break
							}
						}
					} else {
						//如果已经找到了第一个不匹配的块，则此后的块直接处理即可
						if !isTake {
							sendChunk(out, c)
						} else {
							break
						}
					}
				}

				if s.future != nil {
					if _, err := s.future.Get(); err != nil {
						return nil, err
					}
				}
				return nil, nil
			})
			addCloseChanCallback(f, out)

			return &chanSource{chunkChan: out}, f, option.KeepOrder, nil
		}
		panic(ErrUnsupportSource)
	})
}

func getElementAt(src DataSource, i int, option *ParallelOption) (element interface{}, found bool, err error) {
	foundNoMatch := func(c *Chunk) (int, bool) {
		if c.StartIndex <= i && c.StartIndex+len(c.Data)-1 >= i {
			return i - c.StartIndex, true
		} else {
			return len(c.Data), false
		}
	}
	useIndex := true

	switch s := src.(type) {
	case *listSource:
		rs := s.ToSlice(false)
		if i, found := foundNoMatch(&Chunk{rs, 0, 0}); found {
			return rs[i], true, nil
		} else {
			return nil, false, nil
		}
	case *chanSource:
		beforeNoMatchAct := func(c *chunkWhileResult) (while bool) {
			//判断是否满足条件
			if idx, found := foundNoMatch(c.chunk); found {
				element = c.chunk.Data[idx]
				return true
			}
			return false
		}
		afterNoMatchAct, beNoMatchAct := func(c *chunkWhileResult) {}, func(c *chunkWhileResult) {}

		srcChan := s.ChunkChan(option.ChunkSize)
		f := promise.Start(func() (interface{}, error) {
			avl := newChunkWhileResultTree(beforeNoMatchAct, afterNoMatchAct, beNoMatchAct, useIndex)

			for c := range srcChan {
				if isNil(c) {
					if cap(srcChan) > 0 {
						s.Close()
						break
					} else {
						continue
					}
				}

				if !found {
					found = avl.handleMatchChunk(&chunkWhileResult{chunk: c})
					if found {
						s.Close()
						break
					}
				} else {
					//如果已经找到了正确的块，则此后的块直接跳过
					break
				}
			}

			return nil, nil
		})

		if s.future != nil {
			if _, err := s.future.Get(); err != nil {
				return nil, false, err
			}
		}

		if _, err := f.Get(); err != nil {
			return nil, false, err
		}
		return
	}
	panic(ErrUnsupportSource)
}

func getFirstOrLastIndex(src *listSource, predicate func(interface{}) bool, option *ParallelOption) (idx int, found bool, err error) {
	//rs := src.ToSlice(false)
	getAction := func(c *Chunk) func(promise.Canceller) (interface{}, error) {
		return func(canceller promise.Canceller) (r interface{}, e error) {
			//ok := false
			r = -1
			for i, v := range c.Data {
				if canceller != nil && canceller.IsCancellationRequested() {
					canceller.SetCancelled()
					break
				}
				if predicate(v) {
					r = i
					//ok = true
					break
				}
			}
			return r, nil
		}
	}
	f := parallelMatchListWithPriority(src, getAction, option, -1)
	if i, e := f.Get(); e != nil {
		return -1, false, e
	} else if i == -1 {
		return -1, false, nil
	} else {
		return i.(int), true, nil
	}
}

func getFirstOf(src DataSource, f func(interface{}) bool, option *ParallelOption) (element interface{}, found bool, err error) {
	foundMatch := func(c *Chunk) (r int, found bool) {
		j := -1
		for i, v := range c.Data {
			j = i
			if f(v) {
				//fmt.Println("firstof find", j, )
				found = true
				break
			}
		}
		r = j
		return
	}
	useIndex := false

	switch s := src.(type) {
	case *listSource:
		rs := s.ToSlice(false)
		if i, found, err := getFirstOrLastIndex(&listSource{rs}, f, option); err != nil {
			return nil, false, err
		} else if !found {
			return nil, false, nil
		} else {
			return rs[i], true, nil
		}
		//rs := s.ToSlice(false)
		//getAction := func(c *Chunk) func(promise.Canceller) (interface{}, error) {
		//	return func(canceller promise.Canceller) (r interface{}, e error) {
		//		//ok := false
		//		r = -1
		//		for i, v := range c.Data {
		//			if canceller != nil && canceller.IsCancellationRequested() {
		//				canceller.SetCancelled()
		//				break
		//			}
		//			if f(v) {
		//				r = i
		//				//ok = true
		//				break
		//			}
		//		}
		//		return r, nil
		//	}
		//}
		//f := parallelMatchListWithPriority(s, getAction, option, -1)
		//if i, e := f.Get(); e != nil {
		//	return nil, false, e
		//} else if i == -1 {
		//	return nil, false, nil
		//} else {
		//	return rs[i.(int)], true, nil
		//}
	case *chanSource:
		beforeNoMatchAct := func(c *chunkWhileResult) (while bool) {
			//判断是否满足条件
			if idx, found := foundMatch(c.chunk); found {
				element = c.chunk.Data[idx]
				return true
			}
			return false
		}
		afterNoMatchAct, beNoMatchAct := func(c *chunkWhileResult) {}, func(c *chunkWhileResult) {
			element = c.chunk.Data[c.whileIdx]
		}

		srcChan := s.ChunkChan(option.ChunkSize)
		f := promise.Start(func() (interface{}, error) {
			avl := newChunkWhileResultTree(beforeNoMatchAct, afterNoMatchAct, beNoMatchAct, useIndex)

			for c := range srcChan {
				if isNil(c) {
					if cap(srcChan) > 0 {
						s.Close()
						break
					} else {
						continue
					}
				}

				if !found {
					chunkResult := &chunkWhileResult{chunk: c}
					chunkResult.whileIdx, chunkResult.noMatch = foundMatch(c)
					//fmt.Println("check", c, chunkResult, found)
					found = avl.handleChunk(chunkResult)
					//fmt.Println("after check", c, chunkResult, found)
					if found {
						//element = c.chunk.Data[idx]
						s.Close()
						break
					}
				} else {
					//如果已经找到了正确的块，则此后的块直接跳过
					break
				}
			}
			return nil, nil
		})

		if s.future != nil {
			if _, err := s.future.Get(); err != nil {
				return nil, false, err
			}
		}
		if _, err := f.Get(); err != nil {
			return nil, false, err
		}
		return
	}
	panic(ErrUnsupportSource)
}

func getAggregate(src DataSource, aggregateFuncs []*AggregateOpretion, option *ParallelOption) (result []interface{}, err error) {
	if aggregateFuncs == nil || len(aggregateFuncs) == 0 {
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
				rs[i] = aggregateFuncs[i].ReduceAction(c.Data[i], rs[i])
			}
		}
	}

	avl := newChunkAvlTree()
	if errs := reduceChan(f, reduceSrc, func(c *Chunk) {
		if !keep {
			agg(c)
		} else {
			avl.Insert(c)
		}
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

var filteri int = 1

func filterSet(src DataSource, source2 interface{}, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	src2 := newDataSource(source2)
	switch ds2 := src2.(type) {
	case *listSource:
		return filterSetByList(src, ds2, isExcept, option)
	case *chanSource:
		return filterSetByChan(src, ds2, isExcept, option)
	default:
		panic(ErrUnsupportSource)
	}
	//return filter(src, source2, isExcept, option)

}

func filterSetByChan(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	mapChunk := func(c *Chunk) (r *Chunk) {
		//mapSlice(c.Data, func(v interface{}) interface{} { return &hKeyValue{hash64(v), v, v} }, &(c.Data))
		for i := 0; i < len(c.Data); i++ {
			v := c.Data[i]
			//fmt.Println("map", v, "to", hKeyValue{hash64(v), v, v})
			c.Data[i] = &KeyValue{hash64(v), v}
		}
		//r = &Chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.Order, c.StartIndex}
		return c
	}

	//map the elements of source and source2 to the a KeyValue slice
	//includes the hash value and the original element
	_, reduceSrcChan1 := parallelMapToChan(src, nil, mapChunk, option)
	_, reduceSrcChan2 := parallelMapToChan(newQueryable(src2).makeCopiedSrc(true), nil, func(c *Chunk) (r *Chunk) {
		for i := 0; i < len(c.Data); i++ {
			v := c.Data[i]
			c.Data[i] = hash64(v)
		}
		return c
	}, option)

	distinctKVs1 := make(map[uint64]bool, 100)
	distinctKVs2 := make(map[uint64]bool, 100)
	resultKVs := make(map[uint64]interface{}, 100)

	close1, close2 := false, false
L1:
	for {
		select {
		case c1, ok := <-reduceSrcChan1:
			if isNil(c1) || !ok {
				func() {
					defer func() {
						_ = recover()
					}()
					close1 = true
					close(reduceSrcChan1)
				}()
				if close2 {
					break L1
				} else {
					break
				}
			}

			for _, v := range c1.Data {
				kv := v.(*KeyValue)
				k := kv.Key.(uint64)
				//fmt.Println("c1 get", *kv)
				_, ok := distinctKVs2[k]

				if (isExcept && !ok) || (!isExcept && ok) {
					//resultKVs[kv.keyHash] = kv.value
					if _, ok1 := resultKVs[k]; !ok1 {
						resultKVs[k] = kv.Value
					}
				}
				if !isExcept {
					//distinctKVs1[kv.keyHash] = true
					if _, ok := distinctKVs1[k]; !ok {
						distinctKVs1[k] = true
					}
				}
			}

		case c2, ok := <-reduceSrcChan2:
			if isNil(c2) || !ok {
				func() {
					defer func() {
						_ = recover()
					}()
					close2 = true
					close(reduceSrcChan2)
				}()
				if close1 {
					break L1
				} else {
					break
				}
			}
			for _, v := range c2.Data {
				k := v.(uint64)
				//delete(resultKVs, kv.keyHash)
				//distinctKVs2[kv.keyHash] = true
				if isExcept {
					if _, ok1 := resultKVs[k]; ok1 {
						delete(resultKVs, k)
					}
				} else {
					if v, ok := distinctKVs1[k]; ok {
						if _, ok1 := resultKVs[k]; !ok1 {
							resultKVs[k] = v
						}
					}
				}
				if _, ok := distinctKVs2[k]; !ok {
					distinctKVs2[k] = true
				}
			}

		}
	}

	i, results := 0, make([]interface{}, len(resultKVs))
	for _, v := range resultKVs {
		results[i] = v
		i++
	}
	return &listSource{results}, nil, nil

}

func filterSetByList(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	ds2 := newQueryable(src2).makeCopiedSrc(true)
	f2 := parallelMapListToList(ds2, func(c *Chunk) (r *Chunk) {
		for i := 0; i < len(c.Data); i++ {
			c.Data[i] = hash64(c.Data[i])
		}
		return c
	}, option)

	mapChunk := func(c *Chunk) (r *Chunk) {
		r = &Chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.Order, c.StartIndex}
		return
	}
	f1, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, option)
	var distKVs map[uint64]bool

	resultKVs := make(map[uint64]bool, 100)
	mapDistinct := func(c *Chunk) *Chunk {
		if distKVs == nil {
			if rs, err := f2.Get(); err != nil {
				panic(err)
			} else {
				//kv2s := ds2.ToSlice(false)
				distKVs = make(map[uint64]bool, 100)
				for _, c := range rs.([]interface{}) {
					chunk := c.(*Chunk)
					for _, v := range chunk.Data {
						distKVs[v.(uint64)] = true
					}
				}
			}
		}

		//filter src
		i := 0
		results := make([]interface{}, len(c.Data))
		for _, v := range c.Data {
			kv := v.(*hKeyValue)
			k := kv.keyHash
			_, ok := distKVs[k]
			if (isExcept && !ok) || (!isExcept && ok) {
				if _, ok := resultKVs[k]; !ok {
					resultKVs[k] = true
					results[i] = kv.value
					i++
				}
			}
		}
		//fmt.Println("return:", results[0:i])

		return &Chunk{results[0:i], c.Order, c.StartIndex}
	}

	option.Degree = 1
	f, out := parallelMapToChan(&chanSource{chunkChan: reduceSrcChan, future: f1}, nil, mapDistinct, option)
	return &chanSource{chunkChan: out}, f, nil
}

func filterSet6(src DataSource, source2 interface{}, isExcept bool, option *ParallelOption) (DataSource, *promise.Future, error) {
	f1 := parallelMapListToList(src, func(c *Chunk) *Chunk {
		mapSlice(c.Data, func(v interface{}) interface{} {
			return &KeyValue{hash64(v), v}
		}, &c.Data)
		return c
	}, option)

	ds2 := From(source2).makeCopiedSrc(true)
	f2, _ := parallelMapListToList(ds2, func(c *Chunk) (r *Chunk) {
		for i := 0; i < len(c.Data); i++ {
			v := c.Data[i]
			c.Data[i] = hash64(v)
		}
		return c
	}, option).Pipe(func(r1 interface{}) *promise.Future {
		p := promise.NewPromise()

		var distKVs map[uint64]bool
		kv2s := ds2.ToSlice(false)
		distKVs = make(map[uint64]bool, len(kv2s))
		for _, v := range kv2s {
			distKVs[v.(uint64)] = true
		}

		if rs, err := f1.Get(); err != nil {
			p.Reject(err)
		} else {
			i, results := 0, make([]interface{}, 100000)
			resultKVs := make(map[uint64]bool, 100000)

			for _, c := range rs.([]interface{}) {
				chunk := c.(*Chunk)
				for _, v := range chunk.Data {
					kv := v.(*KeyValue)
					k := kv.Key.(uint64)
					_, ok := distKVs[kv.Key.(uint64)]
					if (isExcept && !ok) || (!isExcept && ok) {
						if _, ok := resultKVs[k]; !ok {
							resultKVs[k] = true
							results[i] = kv.Value
							i++
						}
					}
				}
			}
			p.Reslove(&listSource{results[:i]})
		}

		return p.Future
	})

	r2, err := f2.Get()
	return r2.(*listSource), nil, err

	//var distKVs map[uint64]bool
	//once := new(sync.Once)
	//mapChunk := func(c *Chunk) (r *Chunk) {
	//	if distKVs == nil {
	//		once.Do(func() {
	//			if _, err := f2.Get(); err != nil {
	//				panic(err)
	//			}
	//			kv2s := ds2.ToSlice(false)
	//			//fmt.Println("len(kv2s)=", kv2s, len(kv2s))
	//			distKVs = make(map[uint64]bool, len(kv2s))
	//			for _, v := range kv2s {
	//				distKVs[v.(uint64)] = true
	//			}
	//		})
	//	}

	//	//fmt.Println("len(distKVs)=", len(distKVs))
	//	i, results := 0, make([]interface{}, len(c.Data))
	//	for _, v := range c.Data {
	//		kv := &KeyValue{hash64(v), v}
	//		_, ok := distKVs[kv.Key.(uint64)]
	//		if (isExcept && !ok) || (!isExcept && ok) {
	//			results[i] = kv
	//			i++
	//		}
	//	}
	//	return &Chunk{results[:i], c.Order, c.StartIndex}
	//}

	//if rs, err := f1.Get(); err != nil {
	//	return nil, nil, err
	//} else {
	//	i, results := 0, make([]interface{}, 10000)
	//	resultKVs := make(map[uint64]bool, 10000)

	//	for _, c := range rs.([]interface{}) {
	//		chunk := c.(*Chunk)
	//		for _, v := range chunk.Data {
	//			kv := v.(*KeyValue)
	//			k := kv.Key.(uint64)
	//			if _, ok := resultKVs[k]; !ok {
	//				resultKVs[k] = true
	//				results[i] = kv.Value
	//				i++
	//			}
	//		}
	//	}
	//	return &listSource{results[:i]}, nil, nil
	//}
}

func distinctKVs(src interface{}, option *ParallelOption) (map[uint64]interface{}, error) {
	distKVs := make(map[uint64]interface{})

	if kvs, err := selectKVs(From(src)); err != nil {
		return nil, err
	} else {
		//get distinct values
		for _, v := range kvs {
			kv := v.(*KeyValue)
			k := kv.Key.(uint64)
			if _, ok := distKVs[k]; !ok {
				distKVs[k] = kv.Value
			}
		}

		return distKVs, nil
	}
}

func selectKVs(q *Queryable) ([]interface{}, error) {
	return q.Select(func(v interface{}) interface{} {
		return &KeyValue{hash64(v), v}
	}).Results()
}

//paralleliam functions--------------------------------------------------

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
		out = make(chan *Chunk, option.Degree)
		createOutChan = true
	}

	srcChan := src.ChunkChan(option.ChunkSize)

	fs := make([]*promise.Future, option.Degree)
	for i := 0; i < option.Degree; i++ {
		f := promise.Start(func() (r interface{}, e error) {
			//TODO: promise.Start seems cannot capture the error stack?
			defer func() {
				if err := recover(); err != nil {
					e = newErrorWithStacks(err)
					//fmt.Println("get error:", e)
				}
			}()
			for c := range srcChan {
				if !isNil(c) {
					d := task(c)
					if out != nil && d != nil {
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
					return nil, err
				}
			}
			return nil, nil
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
		out = make(chan *Chunk, 5*option.Degree)
		createOutChan = true
	}

	var f *promise.Future
	data := src.ToSlice(false)
	if len(data) == 0 {
		f = promise.Wrap([]interface{}{})
	} else {
		size, lenOfData := option.ChunkSize, len(data)
		if size < lenOfData/(numCPU*5) {
			size = lenOfData / (numCPU * 5)
		}
		ch := make(chan *Chunk)
		go func() {
			for i := 0; i*size < lenOfData; i++ {
				end := (i + 1) * size
				if end >= lenOfData {
					end = lenOfData
				}
				c := &Chunk{data[i*size : end], i * size, i * size} //, end}
				//ch <- c
				sendChunk(ch, c)
			}
			close(ch)
		}()

		cs := &chanSource{chunkChan: ch}
		f, out = parallelMapChanToChan(cs, out, task, option)
	}
	if createOutChan {
		addCloseChanCallback(f, out)
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
	if len(data) == 0 {
		return promise.Wrap([]interface{}{})
	}
	lenOfData, size, j := len(data), ceilChunkSize(len(data), option.Degree), 0

	if size < option.ChunkSize {
		size = option.ChunkSize
	}

	fs := make([]*promise.Future, option.Degree)
	for i := 0; i < option.Degree && i*size < lenOfData; i++ {
		end := (i + 1) * size
		if end >= lenOfData {
			end = lenOfData
		}
		c := &Chunk{data[i*size : end], i * size, i * size} //, end}

		f = promise.Start(getAction(c))
		fs[i] = f
		j++
	}
	f = promise.WhenAll(fs[0:j]...)

	return
}

func parallelMatchListWithPriority(src DataSource, getAction func(*Chunk) func(promise.Canceller) (interface{}, error), option *ParallelOption, priorityOrder int) (f *promise.Future) {
	data := src.ToSlice(false)
	if len(data) == 0 {
		return promise.Wrap(-1)
	}
	lenOfData, size := len(data), ceilChunkSize(len(data), option.Degree)

	if size < option.ChunkSize {
		size = option.ChunkSize
	}

	if size*2 >= lenOfData {
		f := promise.NewPromise()
		func() {
			defer func() {
				if e := recover(); e != nil {
					f.Reject(newErrorWithStacks(e))
				}
			}()
			c := &Chunk{data, 0, 0}
			if r, err := getAction(c)(nil); err != nil {
				f.Reject(err)
			} else {
				f.Reslove(r)
			}
		}()
		return f.Future
	}

	fs := make([]*promise.Future, 0, option.Degree)
	for i := 0; i < option.Degree && i*size < lenOfData; i++ {
		end := (i + 1) * size
		if end >= lenOfData {
			end = lenOfData
		}
		c := &Chunk{data[i*size : end], i, i * size} //, end}

		startIndex := i * size
		f = promise.StartCanCancel(func(canceller promise.Canceller) (interface{}, error) {
			r, err := getAction(c)(canceller)
			if err == nil && r.(int) != -1 {
				r = r.(int) + startIndex
			}
			return r, err
		})
		fs = append(fs, f)
		//count++
	}
	//1 presents the after chunk is priority
	f = promise.Start(func() (interface{}, error) {
		rs, errs := make([]interface{}, len(fs)), make([]error, len(fs))
		var idx interface{}
		allOk, hasOk := true, false
		start, end, i := 0, len(fs), 0

		if priorityOrder == 1 {
			start = len(fs) - 1
			end, i = 0, start
		}

		for {
			f := fs[i]
			//if a future be failure, then will try to cancel other futures
			if !allOk || hasOk {
				for j := i; j < len(fs); j++ {
					if c := fs[j].Canceller(); c != nil {
						fs[j].RequestCancel()
					}
				}
				break
			}

			rs[i], errs[i] = f.Get()
			//fmt.Println("first of get=", rs[i])
			if errs[i] != nil {
				allOk = false
			} else if rs[i].(int) != -1 {
				hasOk = true
			}
			idx = rs[i]

			if priorityOrder == -1 {
				i++
			} else {
				i--
			}

			//all task be done
			if i == end {
				break
			}
		}

		if !allOk {
			return -1, NewLinqError("Error appears in WhenAll:", errs)
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
		return len(list) <= option.ChunkSize
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
		return &listSource{r.Data}, nil, true
	} else {
		return nil, nil, false
	}

}

func trySequentialAggregate(src DataSource, option *ParallelOption, aggregateFuncs []*AggregateOpretion) (rs []interface{}, err error, handled bool) {
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
			rs = []interface{}{len(src.ToSlice(false))}
			return rs, nil, true
		}

		rs = aggregateSlice(src.ToSlice(false), aggregateFuncs, true, true)
		return rs, nil, true
	} else {
		return nil, nil, false
	}

}

func ifMustSequential(aggregateFuncs []*AggregateOpretion) bool {
	for _, f := range aggregateFuncs {
		if f.ReduceAction == nil {
			return true
		}
	}
	return false
}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceChan(f *promise.Future, src chan *Chunk, reduce func(*Chunk)) interface{} {
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
func reduceDistinctVals(mapFuture *promise.Future, reduceSrcChan chan *Chunk, option *ParallelOption) (f *promise.Future, out chan *Chunk) {
	//get distinct values
	distKVs := make(map[uint64]int)
	option.Degree = 1
	return parallelMapChanToChan(&chanSource{chunkChan: reduceSrcChan, future: mapFuture}, nil, func(c *Chunk) *Chunk {
		r := distinctChunkVals(c, distKVs)
		return r
	}, option)
}

//util functions-----------------------------------------------------------------
func distinctChunkVals(c *Chunk, distKVs map[uint64]int) *Chunk {
	result := make([]interface{}, len(c.Data))
	i := 0
	for _, v := range c.Data {
		kv := v.(*hKeyValue)
		if _, ok := distKVs[kv.keyHash]; !ok {
			distKVs[kv.keyHash] = 1
			result[i] = kv.value
			i++
		}
	}
	c.Data = result[0:i]
	return c
}

func addCloseChanCallback(f *promise.Future, out chan *Chunk) {
	f.Always(func(results interface{}) {
		//must use goroutiner, else may deadlock when out is buffer chan
		//because it maybe called before the chan receiver be started.
		//if the buffer is full, out <- nil will be holder then deadlock
		go func() {
			if out != nil {
				if cap(out) == 0 {
					close(out)
				} else {
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

func filterChunk(c *Chunk, f predicateFunc) *Chunk {
	result := filterSlice(c.Data, f)
	return &Chunk{result, c.Order, c.StartIndex}
}

func filterSlice(src []interface{}, f predicateFunc) []interface{} {
	dst := make([]interface{}, 0, 10)

	for _, v := range src {
		if f(v) {
			dst = append(dst, v)
		}
	}
	return dst
}

func mapSlice(src []interface{}, f oneArgsFunc, out *[]interface{}) []interface{} {
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

//TODO: the code need be restructured
func aggregateSlice(src []interface{}, fs []*AggregateOpretion, asSequential bool, asParallel bool) []interface{} {
	if len(src) == 0 {
		panic(errors.New("Cannot aggregate empty slice"))
	}

	rs := make([]interface{}, len(fs))
	for j := 0; j < len(fs); j++ {
		if (asSequential && fs[j].ReduceAction == nil) || (asParallel && fs[j].ReduceAction != nil) {
			rs[j] = fs[j].Seed
		}
	}

	for i := 0; i < len(src); i++ {
		for j := 0; j < len(fs); j++ {
			if (asSequential && fs[j].ReduceAction == nil) || (asParallel && fs[j].ReduceAction != nil) {
				rs[j] = fs[j].AggAction(src[i], rs[j])
			}
		}
	}
	return rs
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
			return a1.Order < b1.Order
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
		count += len(chunks[i].Data)
	}

	result := make([]interface{}, count)
	start := 0
	for _, c := range chunks {
		size := len(c.Data)
		copy(result[start:start+size], c.Data)
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

func ceilChunkSize(a int, b int) int {
	if a%b != 0 {
		return a/b + 1
	} else {
		return a / b
	}
}

func getKeyValues(c *Chunk, keyFunc func(v interface{}) interface{}, KeyValues *[]interface{}) []interface{} {
	if KeyValues == nil {
		list := (make([]interface{}, len(c.Data)))
		KeyValues = &list
	}
	mapSlice(c.Data, func(v interface{}) interface{} {
		k := keyFunc(v)
		return &hKeyValue{hash64(k), k, v}
	}, KeyValues)
	return *KeyValues
}

func iif(predicate bool, trueVal interface{}, falseVal interface{}) interface{} {
	if predicate {
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

//the AVL tree for Skip/SkipWhile/Take/TakeWhile operation----------------------------------------------------
//用来对乱序的channel数据源进行Skip/Take处理的avl树
//channel输出的数据在排序后实际是一个连续的块的列表，而Skip/Take必须根据第一个不匹配条件的数据进行处理，
//所以关键在于尽快对得到的块进行判断
//因为数据源可以乱序输出，所以用avl树来对得到的块进行排序，每当从channel中得到一个块时，
//都会判断是否列表从头开始的部分是否已经被填充，并对所有已经从头构成连续填充的块进行判断，
//判断是否块包含有不匹配条件的数据，是否是第一个不匹配的块，并通过startOrder记录头开始的顺序号
//如果一个块不在从头开始连续填充的区域中，则判断是否在前面有包含不匹配数据的块，如果有的话，则可以执行Skip操作
//avl中始终只保留一个最靠前的不匹配的块，其后的块可以直接进行Skip操作，无需插入avl树
type chunkWhileTree struct {
	avl              *avlTree          //保留了待处理块的avl树
	startOrder       int               //下一个头块的顺序号
	startIndex       int               //下一个头块的起始索引号
	whileChunk       *chunkWhileResult //当前最靠前的包含不匹配数据的块
	beforeNoMatchAct func(*chunkWhileResult) bool
	afterNoMatchAct  func(*chunkWhileResult)
	beNoMatchAct     func(*chunkWhileResult)
	useIndex         bool //是否根据索引进行判断，如果是的话，每个块都必须成为头块后才根据StartIndex进行计算。因为某些操作比如Where将导致块内的数据量不等于原始的数据量
	foundNoMatch     bool //是否已经发现第一个不匹配的块
}

func (this *chunkWhileTree) Insert(node *chunkWhileResult) {
	this.avl.Insert(node)
	if node.noMatch {
		this.whileChunk = node
	}
}

func (this *chunkWhileTree) getWhileChunk() *chunkWhileResult {
	return this.whileChunk
}

func (this *chunkWhileTree) handleChunk(chunkResult *chunkWhileResult) (foundFirstNoMatch bool) {
	//fmt.Println("check handleChunk=", chunkResult, chunkResult.chunk.Order)
	if chunkResult.noMatch {
		//如果块中发现不匹配的数据
		foundFirstNoMatch = this.handleNoMatchChunk(chunkResult)
	} else {
		foundFirstNoMatch = this.handleMatchChunk(chunkResult)
	}
	//fmt.Println("after check handleChunk=", foundFirstNoMatch)
	return
}

//处理符合条件的块，返回true表示该块和后续的连续块中发现了原始序列中第一个不符合条件的块
//在Skip/Take和管道模式中，块是否不符合条件是在块被放置到正确顺序后才能决定的
func (this *chunkWhileTree) handleMatchChunk(chunkResult *chunkWhileResult) bool {
	//如果不满足，则检查当前块order是否等于下一个order，如果是，则进行beforeWhile处理，并更新startOrder
	if chunkResult.chunk.Order == this.startOrder {
		chunkResult.chunk.StartIndex = this.startIndex
		//fmt.Println("call beforeNoMatchAct=", chunkResult.chunk.Order)
		if find := this.beforeNoMatchAct(chunkResult); find {
			//fmt.Println("call beforeNoMatchAct get", find)
			this.foundNoMatch = true
			if !this.useIndex {
				return true
			}
		}
		this.startOrder += 1
		this.startIndex += len(chunkResult.chunk.Data)
		//检查avl中是否还有符合顺序的块
		_ = this.handleOrderedChunks()

		if this.foundNoMatch {
			return true
		}
	} else {
		//如果不是，则检查是否存在已经满足while条件的前置块
		if this.whileChunk != nil && this.whileChunk.chunk.Order < chunkResult.chunk.Order {
			//如果存在，则当前块是while之后的块，根据take和skip进行处理
			this.afterNoMatchAct(chunkResult)
		} else {
			//如果不存在，则插入avl
			this.Insert(chunkResult)
		}
	}
	return false
}

//处理不符合条件的块，返回true表示是原始序列中第一个不符合条件的块
func (this *chunkWhileTree) handleNoMatchChunk(chunkResult *chunkWhileResult) bool {
	//检查avl是否存在已经满足while条件的块
	if lastWhile := this.getWhileChunk(); lastWhile != nil {
		//如果存在符合while的块，则检查当前块是在之前还是之后
		if chunkResult.chunk.Order < lastWhile.chunk.Order {
			//如果是之前，检查avl中所有在当前块之后的块，执行对应的take或while操作
			this.handleWhileAfterChunks(chunkResult)

			//替换原有的while块，检查当前块的order是否等于下一个order，如果是，则找到了while块，并进行对应处理
			if this.setWhileChunk(chunkResult) {
				return true
			}
		} else {
			//如果是之后，则对当前块执行对应的take或while操作
			this.afterNoMatchAct(chunkResult)
		}
	} else {
		//如果avl中不存在符合while的块，则检查当前块order是否等于下一个order，如果是，则找到了while块，并进行对应处理
		//如果不是下一个order，则插入AVL，以备后面的检查
		//fmt.Println("发现第一个当前while块")
		if this.setWhileChunk(chunkResult) {
			return true
		}
	}
	return false
}

func (this *chunkWhileTree) handleWhileAfterChunks(c *chunkWhileResult) {
	result := make([]*chunkWhileResult, 0, 10)
	pResult := &result
	this.forEachAfterChunks(c.chunk.Order, this.avl.root, pResult)
	for _, c := range result {
		this.afterNoMatchAct(c)
	}
}

func (this *chunkWhileTree) handleOrderedChunks() (foundNotMatch bool) {
	result := make([]*chunkWhileResult, 0, 10)
	pResult := &result
	startOrder := this.startOrder
	foundNotMatch = this.forEachOrderedChunks(startOrder, this.avl.root, pResult)
	return
}

func (this *chunkWhileTree) forEachChunks(currentOrder int, root *avlNode, handler func(*chunkWhileResult) (bool, bool), result *[]*chunkWhileResult) bool {
	if result == nil {
		r := make([]*chunkWhileResult, 0, 10)
		result = &r
	}

	if root == nil {
		return false
	}

	rootResult := root.data.(*chunkWhileResult)
	rootOrder := rootResult.chunk.Order
	if rootOrder > currentOrder {
		//如果当前节点的Order大于要查找的order，则先查找左子树
		if lc := (root).lchild; lc != nil {
			l := root.lchild
			//if this.forEachAfterChunks(currentOrder, l, result) {
			if this.forEachChunks(currentOrder, l, handler, result) {
				return true
			}
		}
	}

	//处理节点
	if found, end := handler(rootResult); end {
		return found
	}

	if (root).rchild != nil {
		// 查找右子树
		r := (root.rchild)
		if this.forEachChunks(currentOrder, r, handler, result) {
			return true
		}
	}
	return false
}

//查找avl中Order在currentOrder之后的块，一直找到发现一个while块为止
func (this *chunkWhileTree) forEachAfterChunks(currentOrder int, root *avlNode, result *[]*chunkWhileResult) bool {
	return this.forEachChunks(currentOrder, root, func(rootResult *chunkWhileResult) (bool, bool) {
		rootOrder := rootResult.chunk.Order
		//查找左子树完成，判断当前节点
		if rootOrder >= currentOrder {
			*result = append(*result, rootResult)
			if root.sameList != nil {
				for _, v := range root.sameList {
					*result = append(*result, v.(*chunkWhileResult))
				}
			}
		}
		//如果当前节点是while节点，则结束查找
		if rootResult.noMatch {
			return true, true
		}
		return false, false
	}, result)
}

//从currentOrder开始查找已经按元素顺序排好的块，一直找到发现一个空缺的位置为止
//如果是Skip/Take，会在查找同时计算块的起始索引，判断是否符合while条件。因为每块的长度未必等于原始长度，所以必须在得到正确顺序后才能计算
//如果是SkipWhile/TakeWhile，如果找到第一个符合顺序的while块，就会结束查找。因为SkipWhile/TakeWhile的avl中不会有2个符合while的块存在
func (this *chunkWhileTree) forEachOrderedChunks(currentOrder int, root *avlNode, result *[]*chunkWhileResult) bool {
	return this.forEachChunks(currentOrder, root, func(rootResult *chunkWhileResult) (bool, bool) {
		//fmt.Println("check ordered----", this.startOrder, this.foundNoMatch, this.useIndex, "chunk =", rootResult.chunk, rootResult)
		rootOrder := rootResult.chunk.Order
		if rootResult.chunk.Order < this.startOrder {
			return false, false
		}
		if this.foundNoMatch && this.useIndex {
			//前面已经找到了while元素，那只有根据index查找才需要判断后面的块,并且所有后面的块都需要返回
			this.afterNoMatchAct(rootResult)
		} else if rootOrder == this.startOrder { //currentOrder {
			//如果当前节点的order等于指定order，则找到了要遍历的第一个元素
			*result = append(*result, rootResult)
			rootResult.chunk.StartIndex = this.startIndex

			if this.useIndex || !rootResult.noMatch {
				if find := this.beforeNoMatchAct(rootResult); find {
					//fmt.Println("find before no match-------", rootResult.chunk)
					this.foundNoMatch = true
				}
			}
			if root.sameList != nil {
				panic(errors.New("Order cannot be same as" + strconv.Itoa(rootOrder)))
			}
			this.startOrder += 1
			this.startIndex += len(rootResult.chunk.Data)
		} else if rootOrder > this.startOrder { //currentOrder {
			//如果左子树和节点自身没有找到指定order，rootOrder的右子树又比rootOrder大，则指定Order不存在
			//fmt.Println("check ordered return", false, true)
			return false, true
		}
		//如果是SkipWhile/TakeWhile，并且当前节点是while节点，则结束查找
		//如果是Skip/Take，则不能结束
		if rootResult.noMatch && !this.useIndex {
			this.foundNoMatch = true
			this.beNoMatchAct(rootResult)
			//fmt.Println("find while", this.startOrder, this.foundNoMatch, this.useIndex, rootResult.chunk, rootResult)
			return true, true
		}
		return false, false
	}, result)
}

func (this *chunkWhileTree) setWhileChunk(c *chunkWhileResult) bool {
	//检查当前块order是否等于下一个order，如果是，则找到了while块，并进行对应处理
	//如果不是下一个order，则插入AVL，以备后面的检查
	if c.chunk.Order == this.startOrder {
		this.beNoMatchAct(c)
		//this.beforeNoMatchAct(c)
		return true
	} else {
		this.Insert(c)
		return false
	}
}

func newChunkWhileResultTree(beforeNoMatchAct func(*chunkWhileResult) bool, afterNoMatchAct func(*chunkWhileResult), beNoMatchAct func(*chunkWhileResult), useIndex bool) *chunkWhileTree {
	return &chunkWhileTree{NewAvlTree(func(a interface{}, b interface{}) int {
		c1, c2 := a.(*chunkWhileResult), b.(*chunkWhileResult)
		if c1.chunk.Order < c2.chunk.Order {
			return -1
		} else if c1.chunk.Order == c2.chunk.Order {
			return 0
		} else {
			return 1
		}
	}), 0, 0, nil, beforeNoMatchAct, afterNoMatchAct, beNoMatchAct, useIndex, false}
}
