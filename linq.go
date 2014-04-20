package plinq

import (
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"sync"
	"time"
)

const (
	DEFAULTCHUNKSIZE     = 200
	SOURCE_LIST      int = iota //presents the list source
	SOURCE_CHUNK                //presents the channel source
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

	Sum = &AggregateOpr{0, sumOpr, sumOpr}
	Count = &AggregateOpr{0, countOpr, sumOpr}
	Min = getMinOpr(defLess)
	Max = getMaxOpr(defLess)
}

// the struct and interface about data DataSource---------------------------------------------------
// A Chunk presents a data chunk, the paralleliam algorithm always handle a chunk in a paralleliam tasks.
type Chunk struct {
	Data  []interface{}
	Order int //a index presents the order of chunk
}

// The DataSource presents the data of linq operation
// Most linq operations usually convert a DataSource to another DataSource
type DataSource interface {
	Typ() int                   //list or chan?
	ToSlice(bool) []interface{} //Get a slice includes all datas
	//ToChan() chan interface{}   //will be implement in futures
}

// KeyValue presents a key value pair, it be used by GroupBy, Join and Set operations
type KeyValue struct {
	key   interface{}
	value interface{}
}

//Aggregate operation structs and functions-------------------------------
//
type AggregateOpr struct {
	Seed         interface{}
	AggAction    func(interface{}, interface{}) interface{}
	ReduceAction func(interface{}, interface{}) interface{}
}

// Standard Sum, Count, Min and Max Aggregation operation
var (
	Sum   *AggregateOpr
	Count *AggregateOpr
	Min   *AggregateOpr
	Max   *AggregateOpr
)

//the queryable struct-------------------------------------------------------------------------
// A ParallelOption presents the options of the paralleliam algorithm.
type ParallelOption struct {
	degree    int  //The degree of the paralleliam algorithm
	chunkSize int  //The size of chunk
	keepOrder bool //whether need keep order of original data
}

// A Queryable presents an object includes the data and query operations
// All query functions will return Queryable.
// For getting the result slice of the query, use Results().
type Queryable struct {
	data  DataSource
	steps []step
	//stepErrs []interface{}
	errChan chan []stepErr
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

func newDataSource(data interface{}) DataSource {
	isNotNil(data, ErrNilSource)

	var ds DataSource
	if k := reflect.ValueOf(data).Kind(); k == reflect.Slice || k == reflect.Map {
		ds = &listSource{data: data}
	} else if s, ok := data.(chan *Chunk); ok {
		ds = &chanSource{chunkChan: s}
	} else if k == reflect.Chan {
		ds = &chanSource{new(sync.Once), data, nil, nil}
	} else {
		panic(ErrUnsupportSource)
	}
	return ds
}

func newQueryable(ds DataSource) (q *Queryable) {

	q = &Queryable{}
	q.keepOrder = true
	q.steps = make([]step, 0, 4)
	q.degree = numCPU
	q.chunkSize = DEFAULTCHUNKSIZE
	q.errChan = make(chan []stepErr)

	q.data = ds
	return
}

// Results evaluates the query and returns the results as interface{} slice.
// If the error occurred in during evaluation of the query, it will be returned.
//
// Example:
// 	results, err := From([]interface{}{"Jack", "Rock"}).Select(something).Results()
func (this *Queryable) Results() (results []interface{}, err error) {
	if ds, e := this.get(); e == nil {
		results = ds.ToSlice(this.keepOrder)
	}
	//if len(this.stepErrs) > 0 {
	//	err = NewLinqError("Aggregate errors", this.stepErrs)
	//}
	err = this.stepErrs()

	return
}

// Aggregate returns the results of aggregation operation
// Aggregation operation aggregates the result in the data source base on the AggregateOpr.
//
// Aggregate can return a slice includes multiple results if passes multiple aggregation operation once.
// If passes one aggregation operation, Aggregate will return single interface{}
//
// Noted:
// Aggregate supports the customized aggregation function
// TODO: doesn't support the mixed type in aggregate now
// TODO: Will panic if the error appears in aggregate function, but should return an error
//
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	aggResults, err := From(arr).Aggregate(Sum, Count, Max, Min) // return [18, 4, 9, 0]
//	// or
//	sum, err := From(arr).Aggregate(Sum) // sum is 18
// TODO: the customized aggregation function
func (this *Queryable) Aggregate(aggregateFuncs ...*AggregateOpr) (result interface{}, err error) {
	if ds, e := this.get(); e == nil {
		//if errs := <-this.errChan; len(errs) > 0 {
		//	err = NewLinqError("Aggregate errors", errs)
		//}
		//if this.errChan != nil {
		//	close(this.errChan)
		//}
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
	aggregateOprs := []*AggregateOpr{Sum}

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
	aggregateOprs := []*AggregateOpr{Count}

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
func (this *Queryable) CountBy(predicate func(interface{}) bool) (result interface{}, err error) {
	if predicate == nil {
		predicate = func(interface{}) bool { return true }
	}
	aggregateOprs := []*AggregateOpr{getCountByOpr(predicate)}

	if result, err = this.Aggregate(aggregateOprs...); err == nil {
		return result, nil
	} else {
		return nil, err
	}
}

// Average computes average of numeric values in the data source.
// TODO: If sequence has non-numeric types or nil, should returns an error.
// Example:
//	arr = []interface{}{0, 3, 6, 9}
//	arg, err := From(arr).Average() // sum is 4.5
func (this *Queryable) Average() (result interface{}, err error) {
	aggregateOprs := []*AggregateOpr{Sum, Count}

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
// TODO: If sequence has other types or nil, should returns an error.
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

	aggregateOprs := []*AggregateOpr{getMaxOpr(less)}

	if results, err := this.Aggregate(aggregateOprs...); err == nil {
		return results, nil
	} else {
		return nil, err
	}
}

// Min returns the minimum value in the data source.
// Min operation supports the numeric types, string and time.Time
// TODO: need more testing for string and time.Time
// TODO: If sequence has other types or nil, should returns an error.
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

	aggregateOprs := []*AggregateOpr{getMinOpr(less)}

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
func (this *Queryable) Where(predicate func(interface{}) bool, degrees ...int) *Queryable {
	isNotNil(predicate, ErrNilAction)

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
func (this *Queryable) Select(selectFunc func(interface{}) interface{}, degrees ...int) *Queryable {
	isNotNil(selectFunc, ErrNilAction)

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
func (this *Queryable) DistinctBy(distinctFunc func(interface{}) interface{}, degrees ...int) *Queryable {
	isNotNil(distinctFunc, ErrNilAction)
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
	isNotNil(keySelector, ErrNilAction)

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
	isNotNil(source2, ErrUnionNilSource)

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
	isNotNil(source2, ErrConcatNilSource)

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
	isNotNil(source2, ErrInterestNilSource)

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
	isNotNil(source2, ErrExceptNilSource)

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
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, interface{}) interface{}, degrees ...int) *Queryable {
	isNotNil(inner, ErrJoinNilSource)
	isNotNil(outerKeySelector, ErrOuterKeySelector)
	isNotNil(innerKeySelector, ErrInnerKeySelector)
	isNotNil(resultSelector, ErrResultSelector)

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
	isNotNil(inner, ErrJoinNilSource)
	isNotNil(outerKeySelector, ErrOuterKeySelector)
	isNotNil(innerKeySelector, ErrInnerKeySelector)
	isNotNil(resultSelector, ErrResultSelector)

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
	isNotNil(inner, ErrJoinNilSource)
	isNotNil(outerKeySelector, ErrOuterKeySelector)
	isNotNil(innerKeySelector, ErrInnerKeySelector)
	isNotNil(resultSelector, ErrResultSelector)

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
	isNotNil(inner, ErrJoinNilSource)
	isNotNil(outerKeySelector, ErrOuterKeySelector)
	isNotNil(innerKeySelector, ErrInnerKeySelector)
	isNotNil(resultSelector, ErrResultSelector)

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

// KeepOrder returns a query from the original query,
// the result slice will keep the order of origin query as much as possible
// Noted: Order operation will change the original order
// TODO: Distinct, Union, Join, Interest, Except operations need more testing
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
// chunk is the data unit of the parallelism, default size is DEFAULTCHUNKSIZE
func (this *Queryable) SetSizeOfChunk(size int) *Queryable {
	this.chunkSize = size
	return this
}

func (this *Queryable) aggregate(aggregateFuncs ...func(interface{}, interface{}) interface{}) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_AGGREGATE, aggregateFuncs, 0})
	return this
}

func (this *Queryable) hGroupBy(keySelector func(interface{}) interface{}, degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_HGROUPBY, keySelector, 0})
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
	errsChan := make(chan *stepErr)
	go func() {
		stepFutures := make([]stepErr, 0, len(this.steps))
		if len(this.steps) == 0 {
			this.errChan <- stepFutures
			return
		}

		i := 0
		//fmt.Println("start receive errors")
		for e := range errsChan {
			if !reflect.ValueOf(e).IsNil() {
				stepFutures = append(stepFutures, *e)
				//fmt.Println("get a step error", e.errs)
			}
			i++
			if i >= len(this.steps) {
				this.errChan <- stepFutures
				return
			}
		}
	}()

	data = this.makeCopiedSrc() //data
	pOption, keepOrder := this.ParallelOption, this.ParallelOption.keepOrder

	for i, step := range this.steps {
		var f *promise.Future
		step1 := step

		//execute the step
		if data, f, keepOrder, err = step.Action()(data, step.POption(pOption)); err != nil {
			errsChan <- NewStepError(i, step1.Typ(), err)
			for j := i + 1; j < len(this.steps); j++ {
				errsChan <- nil
			}
			return nil, err
		} else if f != nil {
			j := i
			//add a fail callback to collect the errors in pipeline mode
			//because the steps will be paralle in piplline mode,
			//so cannot use return value of the function
			f.Fail(func(results interface{}) {
				errsChan <- NewStepError(j, step1.Typ(), results)
			}).Done(func(results interface{}) {
				errsChan <- nil
			})
		} else {
			errsChan <- nil
		}

		//set the keepOrder for next step
		//some operation will enforce after operations keep the order,
		//e.g OrderBy operation
		pOption.keepOrder = keepOrder
	}

	return data, nil
}

func (this *Queryable) stepErrs() (err error) {
	if errs := <-this.errChan; len(errs) > 0 {
		err = NewLinqError("Aggregate errors", errs)
	}
	if this.errChan != nil {
		close(this.errChan)
		this.errChan = make(chan []stepErr)
	}
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
	future    *promise.Future
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

			this.future = promise.Start(func() (r interface{}, e error) {
				defer func() {
					if err := recover(); err != nil {
						//fmt.Println("chunk source get error", e)
						e = newErrorWithStacks(err)
					}
				}()
				chunkData := make([]interface{}, 0, chunkSize)
				i := 0
				for {
					if v, ok := srcChan.Recv(); ok {
						//fmt.Println("chunk source receie", v.Interface())
						chunkData = append(chunkData, v.Interface())
						if len(chunkData) == cap(chunkData) {
							//fmt.Println("chunk source send", chunkData)
							this.chunkChan <- &Chunk{chunkData, i}
							i++
							chunkData = make([]interface{}, 0, chunkSize)
						}
					} else {
						break
					}
				}

				if len(chunkData) > 0 {
					//fmt.Println("chunk source send", chunkData)
					this.chunkChan <- &Chunk{chunkData, i}
				}

				this.chunkChan <- nil
				return nil, nil
			})

			//fmt.Println("this.future=", this.future)
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

//Itr returns a function that returns a pointer of chunk every be called
func (this *chanSource) ChunkChan(chunkSize int) chan *Chunk {
	this.makeChunkChanSure(chunkSize)
	return this.chunkChan
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
//		for c := range this.Data {
//			for _, v := range c.Data {
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
	ACT_EXCEPT
	ACT_AGGREGATE
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
	case ACT_EXCEPT:
		act = getExcept(this.act)
	case ACT_REVERSE:
		act = getReverse()
		//case ACT_AGGREGATE:
		//	act = getAggregate(this.act.([]func(interface{}, interface{}) interface{}))
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
			//result := make([]interface{}, len(c.Data)) //c.end-c.start+2)
			mapSlice(c.Data, selectFunc, &(c.Data))
			return c //&Chunk{result, c.Order}
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
			return list, nil, option.keepOrder, err
		}

		switch s := src.(type) {
		case *listSource:
			//size := len(s.ToSlice(false))
			//results := make([]interface{}, size)
			//results := s.ToSlice(false)
			//f := parallelMapListToList(s, func(c *Chunk) *Chunk {
			//	out := results[c.Order : c.Order+len(c.Data)]
			//	mapSlice(c.Data, selectFunc, &out)
			//	return nil
			//}, option)

			f, out := parallelMapListToChan(s, nil, mapChunk, option)
			//dst, e = getFutureResult(f, func(r []interface{}) DataSource {
			//	//fmt.Println("results=", results)
			//	return &listSource{results}
			//})
			sf, dst, e = f, &chanSource{chunkChan: out, future: f}, nil
			return
		case *chanSource:
			//use channel mode if the source is a channel
			f, out := parallelMapChanToChan(s, nil, mapChunk, option)

			sf, dst, e = f, &chanSource{chunkChan: out, future: f}, nil
			//noted when use pipeline mode to start next step, the future of current step must be returned
			//otherwise the errors in current step will be missed
			return
		}

		panic(ErrUnsupportSource)
	})

}

func getWhere(predicate func(interface{}) bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = filterChunk(c, predicate)
			return
		}

		//try to use sequentail if the size of the data is less than size of chunk
		if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
			return list, nil, option.keepOrder, err
		}

		//always use channel mode in Where operation
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, option)

		//fmt.Println("return where")
		return &chanSource{chunkChan: reduceSrc, future: f}, f, option.keepOrder, nil
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

func getDistinct(distinctFunc func(interface{}) interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, distinctFunc, nil), c.Order}
			return
		}

		//test the size = 100, trySequentialMap only speed up 10%
		//if list, handled := trySequentialMap(src, &option, mapChunk); handled {
		//	c := &Chunk{list.ToSlice(false), 0}
		//	distKVs := make(map[uint64]int)
		//	c = distinctChunkVals(c, distKVs)
		//	return &listSource{c.Data}, nil, option.keepOrder, nil
		//}

		//map the element to a keyValue that key is hash value and value is element
		f, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, option)

		//reduce the keyValue map to get distinct values
		if chunks, err := reduceDistinctVals(f, reduceSrcChan); err == nil {
			//get distinct values
			//fmt.Println("\nbefore expandChunks(),", option.keepOrder, ":")
			//for _, v := range chunks {
			//	fmt.Print(v.(*Chunk).Order, ", ")
			//}
			//fmt.Println()
			result := expandChunks(chunks, false)
			//fmt.Println("\nafter expandChunks():")
			//for _, v := range result {
			//	fmt.Print(v, ", ")
			//}
			//fmt.Println()
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
			r = &Chunk{getKeyValues(c, groupFunc, nil), c.Order}
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
		errs := reduceChan(f.GetChan(), reduceSrc, func(c *Chunk) {
			for _, v := range c.Data {
				groupKV(v)
			}
		})

		if errs == nil {
			return &listSource{groupKVs}, nil, option.keepOrder, nil
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
		innerKVtask := promise.Start(func() (interface{}, error) {
			if innerKVsDs, err := From(inner).hGroupBy(innerKeySelector).get(); err == nil {
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
						//fmt.Println("outer", *outerkv, "inner", innerList)
						matchSelector(outerkv, innerList.([]interface{}), &results)
					} else if isLeftJoin {
						unmatchSelector(outerkv, &results)
					}
				}
			}

			return &Chunk{results, c.Order}
		}

		switch s := src.(type) {
		case *listSource:
			//Todo:
			//can use channel mode like Where operation?
			outerKeySelectorFuture := parallelMapListToList(s, mapChunk, option)

			dst, e = getFutureResult(outerKeySelectorFuture, func(results []interface{}) DataSource {
				result := expandChunks(results, option.keepOrder)
				return &listSource{result}
			})
			return
		case *chanSource:
			f, out := parallelMapChanToChan(s, nil, mapChunk, option)
			dst, sf, e = &chanSource{chunkChan: out, future: f}, f, nil
			return
		}

		panic(ErrUnsupportSource)
	})
}

func getUnion(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (DataSource, *promise.Future, bool, error) {
		reduceSrcChan := make(chan *Chunk)
		mapChunk := func(c *Chunk) (r *Chunk) {
			r = &Chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.Order}
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
		results, _, err := filterSet(src, source2, func(hashKey1 uint64, distKVs map[uint64]interface{}) bool {
			_, ok := distKVs[hashKey1]
			return ok
		}, option)

		if err == nil {
			return &listSource{results}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, err
		}
	})
}

func getExcept(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (result DataSource, f *promise.Future, keep bool, err error) {
		results, _, err := filterSet(src, source2, func(hashKey1 uint64, distKVs map[uint64]interface{}) bool {
			_, ok := distKVs[hashKey1]
			return !ok
		}, option)

		if err == nil {
			return &listSource{results}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, err
		}
	})
}

func getReverse() stepAction {
	return stepAction(func(src DataSource, option *ParallelOption) (dst DataSource, sf *promise.Future, keep bool, e error) {
		keep = option.keepOrder
		wholeSlice := src.ToSlice(true)
		srcSlice, size := wholeSlice[0:len(wholeSlice)/2], len(wholeSlice)

		mapChunk := func(c *Chunk) *Chunk {
			for i := 0; i < len(c.Data); i++ {
				j := c.Order + i
				t := wholeSlice[size-1-j]
				wholeSlice[size-1-j] = c.Data[i]
				c.Data[i] = t
			}
			return c
		}

		reverseSrc := &listSource{srcSlice}

		//try to use sequentail if the size of the data is less than size of chunk
		if _, err, handled := trySequentialMap(reverseSrc, option, mapChunk); handled {
			return &listSource{wholeSlice}, nil, option.keepOrder, err
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

func getAggregate(src DataSource, aggregateFuncs []*AggregateOpr, option *ParallelOption) (result []interface{}, err error) {
	if aggregateFuncs == nil || len(aggregateFuncs) == 0 {
		return nil, newErrorWithStacks(errors.New("Aggregation function cannot be nil"))
	}
	keep := option.keepOrder

	//var (
	//	rs      []interface{} = make([]interface{}, len(aggregateFuncs))
	//	handled bool
	//)
	//try to use sequentail if the size of the data is less than size of chunk
	if rs, err, handled := trySequentialAggregate(src, option, aggregateFuncs); handled {
		return rs, err
	}

	rs := make([]interface{}, len(aggregateFuncs))
	mapChunk := func(c *Chunk) (r *Chunk) {
		r = &Chunk{aggregateSlice(c.Data, aggregateFuncs, false, true), c.Order}
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
	if errs := reduceChan(f.GetChan(), reduceSrc, func(c *Chunk) {
		if !keep {
			//datas := c.Data
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

func filterSet(src DataSource, source2 interface{}, filter func(uint64, map[uint64]interface{}) bool, option *ParallelOption) ([]interface{}, map[uint64]interface{}, error) {
	f1, f2 := promise.Start(func() (interface{}, error) {
		return distinctKVs(source2, option)
	}), promise.Start(func() (interface{}, error) {
		return selectKVs(newQueryable(src))
	})

	if r1, err := f1.Get(); err != nil {
		return nil, nil, NewLinqError("Set error", err)
	} else if r2, err := f2.Get(); err != nil {
		return nil, nil, NewLinqError("Set error", err)
	} else {
		distKVs := r1.(map[uint64]interface{})
		kvs := r2.([]interface{})

		//filter src
		i := 0
		results, resultKVs := make([]interface{}, len(kvs)),
			make(map[uint64]interface{}, len(kvs))
		for _, v := range kvs {
			kv := v.(*KeyValue)
			k := kv.key.(uint64)
			if filter(k, distKVs) {
				if _, ok := resultKVs[k]; !ok {
					resultKVs[k] = kv.value
					results[i] = kv.value
					i++
				}
			}
		}

		return results[0:i], resultKVs, nil
	}
}

func distinctKVs(src interface{}, option *ParallelOption) (map[uint64]interface{}, error) {
	distKVs := make(map[uint64]interface{})

	if kvs, err := selectKVs(From(src)); err != nil {
		return nil, err
	} else {
		//get distinct values
		for _, v := range kvs {
			kv := v.(*KeyValue)
			k := kv.key.(uint64)
			if _, ok := distKVs[k]; !ok {
				distKVs[k] = kv.value
				//fmt.Println("add", kv.value, "to distKVs")
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

//paralleliam functions------------------------------------------
//func parallelAggregate(src DataSource, aggregateFuncs []func(interface{}, interface{}) interface{}, option *ParallelOption)(f *promise.Future, interface{}){
//	//get all values and keys
//	switch s := src.(type) {
//	case *listSource:
//    	fs := make([]*promise.Future, option.degree)
//    	data := src.ToSlice(false)
//    	lenOfData, size, j := len(data), ceilChunkSize(len(data), option.degree), 0

//    	if size < option.chunkSize {
//    		size = option.chunkSize
//    	}

//    	for i := 0; i < option.degree && i*size < lenOfData; i++ {
//    		end := (i + 1) * size
//    		if end >= lenOfData {
//    			end = lenOfData
//    		}
//    		c := &Chunk{data[i*size : end], i * size} //, end}

//    		f = promise.Start(func() (interface{}, error){
//                return aggregateSlice(src.ToSlice(false), aggregateFuncs), nil
//            })
//    		fs[i] = f
//    		j++
//    	}
//    	if rs, err := promise.WhenAll(fs[0:j]...); err == nil{
//             c:= &chunk{rs.([]interface{})}
//            r := aggregateSlice(c)
//            return r, nil
//        } else {
//            return nil, err
//        }
//	case *chanSource:
//		out = make(chan *Chunk, option.degree)

//	itr := src.Itr(option.chunkSize)
//	fs := make([]*promise.Future, option.degree)
//	//for i := 0; i < option.degree; i++ {
//		f := promise.Start(func() (r interface{}, error) {
//                var r interface{}
//			for {
//				//fmt.Println("begin select receive")
//				if c, ok := itr(); ok {
//					//fmt.Println("select receive", c)
//					if reflect.ValueOf(c).IsNil() {
//						src.Close()
//						//fmt.Println("select receive a nil-----------------")
//						break
//					}
//					//fmt.Println("select receive", *c)
//					r := aggregateChunk(c)
//					if r != nil {
//                    out <- r
//                    }
//				} else {
//					//fmt.Println("end receive--------------")
//					break
//				}
//			}
//			return nil, nil
//			//fmt.Println("r=", r)
//		})
//		//fs[i] = f
//	//}
//	f := promise.WhenAll(fs...)

//	addCloseChanCallback(f, out)
//	return f, out
//	default:
//		panic(ErrUnsupportSource)
//	}
//}

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

	srcChan := src.ChunkChan(option.chunkSize)

	var chEndFlag chan *promise.PromiseResult
	if src.future == nil {
		chEndFlag = nil
		//close(chEndFlag)
		//TODO: it still need be updated
		fmt.Println("make a temp future")
	} else {
		chEndFlag = src.future.GetChan()
	}

	fs := make([]*promise.Future, option.degree)
	for i := 0; i < option.degree; i++ {
		f := promise.Start(func() (r interface{}, e error) {
			//TODO: promise.Start seems cannot capture the error stack?
			defer func() {
				if err := recover(); err != nil {
					e = newErrorWithStacks(err)
					//fmt.Println("parallelMapChanToChan", e)
				}
			}()
			for {
				select {
				case r := <-chEndFlag:
					//fmt.Println("return reduceChan")
					if r != nil && r.Typ != promise.RESULT_SUCCESS {
						//return r.Result
						//fmt.Println("return parallelMapChanToChan 1")
						return nil, nil
					} else if cap(srcChan) == 0 {
						//return nil
						//fmt.Println("return parallelMapChanToChan 2")
						return nil, nil
					}
				case c, ok := <-srcChan:
					//fmt.Println("begin select receive")
					//if c, ok := itr(); ok {
					if ok {
						//fmt.Println("select receive", c)
						if !reflect.ValueOf(c).IsNil() {
							//fmt.Println("select receive", c)
							d := task(c)
							if out != nil && d != nil {
								out <- d
								//fmt.Println("parallelMapChanToChan end select send", *d)
							}
						} else if cap(srcChan) > 0 {
							src.Close()
							//fmt.Println("select receive a nil-----------------")
							return nil, nil

						}
					} else {
						//fmt.Println("parallelMapChanToChan end receive--------------")
						return nil, nil
					}
				}
			}
			return nil, nil
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

//func reduceChan(chEndFlag chan *promise.PromiseResult, src chan *Chunk, reduce func(*Chunk)) interface{} {
//	//if cap(src) == 0 {
//	//for no buffer channel,
//	//receiving the end flag from the chEndFlag presents the the all data be sent
//	for {
//		select {
//		case r := <-chEndFlag:
//			//fmt.Println("return reduceChan")
//			if r != nil && r.Typ != promise.RESULT_SUCCESS {
//				return r.Result
//			} else if cap(src) == 0 {
//				return nil
//			}
//		case v, ok := <-src:
//			//if ok && v != nil {
//			//	reduce(v)
//			//}
//			if ok {
//				if v != nil {
//					reduce(v)
//				} else if cap(src) > 0 {
//					close(src)
//					return nil
//				}
//			}
//		}
//	}
//}

func parallelMapListToChan(src DataSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption) (*promise.Future, chan *Chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.degree)
		createOutChan = true
	}

	f := parallelMapList(src, func(c *Chunk) func() (interface{}, error) {
		return func() (interface{}, error) {
			r := task(c)
			if out != nil {
				out <- r
				//fmt.Println("\n send", r.Order, len(r.Data))
			}
			return nil, nil
		}
	}, option)
	if createOutChan {
		addCloseChanCallback(f, out)
	}
	//fmt.Println("return f")
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
	lenOfData, size, j := len(data), ceilChunkSize(len(data), option.degree), 0

	if size < option.chunkSize {
		size = option.chunkSize
	}

	fs := make([]*promise.Future, option.degree)
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

func trySequentialMap(src DataSource, option *ParallelOption, mapChunk func(c *Chunk) (r *Chunk)) (ds DataSource, err error, ok bool) {
	defer func() {
		if e := recover(); e != nil {
			err = newErrorWithStacks(e)
			//return nil, newErrorWithStacks(e), true
		}
	}()
	if useSingle := singleDegree(src, option); useSingle {
		c := &Chunk{src.ToSlice(false), 0}
		r := mapChunk(c)
		return &listSource{r.Data}, nil, true
	} else {
		return nil, nil, false
	}

}

func trySequentialAggregate(src DataSource, option *ParallelOption, aggregateFuncs []*AggregateOpr) (rs []interface{}, err error, handled bool) {
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

func ifMustSequential(aggregateFuncs []*AggregateOpr) bool {
	for _, f := range aggregateFuncs {
		if f.ReduceAction == nil {
			return true
		}
	}
	return false
}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceChan(chEndFlag chan *promise.PromiseResult, src chan *Chunk, reduce func(*Chunk)) interface{} {
	//if cap(src) == 0 {
	//for no buffer channel,
	//receiving the end flag from the chEndFlag presents the the all data be sent
	for {
		select {
		case r := <-chEndFlag:
			if r != nil && r.Typ != promise.RESULT_SUCCESS {
				//fmt.Println("return reduceChan 1")
				return r.Result
			} else if cap(src) == 0 {
				//fmt.Println("return reduceChan 2")
				return nil
			}
		case v, ok := <-src:
			//if ok && v != nil {
			//	reduce(v)
			//}
			if ok {
				if v != nil {
					//fmt.Println("reduceChan receive", v)
					reduce(v)
				} else if cap(src) > 0 {
					close(src)
					//fmt.Println("return reduceChan 3")
					return nil
				}
			}
		}
	}
	//} else {
	//	//for buffer channel,
	//	//receiving a nil presents the all data be sent
	//	for {
	//		select {
	//		case r := <-chEndFlag:
	//			if r != nil && r.Typ != promise.RESULT_SUCCESS {
	//				//the future tasks related to source channel gets the error
	//				return r.Result
	//			}
	//		case v, ok := <-src:
	//			if ok {
	//				if v != nil {
	//					reduce(v)
	//				} else {
	//					close(src)
	//					return nil
	//				}
	//			}
	//		}
	//	}
	//}

}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceDistinctVals(mapFuture *promise.Future, reduceSrcChan chan *Chunk) ([]interface{}, error) {
	//get distinct values
	chunks := make([]interface{}, 0, 2)
	distKVs := make(map[uint64]int)
	errs := reduceChan(mapFuture.GetChan(), reduceSrcChan, func(c *Chunk) {
		chunks = appendSlice(chunks, distinctChunkVals(c, distKVs))
	})
	if errs == nil {
		return chunks, nil
	} else {
		return nil, NewLinqError("reduceDistinctVals error", errs)
	}
	parallelMapChanToChan(&chanSource{data: reduceSrcChan, future: f}, nil, func (c *Chunk) *Chunk{
		return distinctChunkVals(c, distKVs))
	}, )
}

//util functions--------------------------------------------------------
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
	//fmt.Println("addCloseChanCallback")
	f.Always(func(results interface{}) {
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
	if results, err := f.Get(); err != nil {
		//todo
		//fmt.Println("getFutureResult get", err)
		return nil, err
	} else {
		//fmt.Println("(results)=", (results))
		return dataSourceFunc(results.([]interface{})), nil
	}
}

func filterChunk(c *Chunk, f func(interface{}) bool) *Chunk {
	result := filterSlice(c.Data, f)
	//fmt.Println("c=", c)
	//fmt.Println("result=", result)
	return &Chunk{result, c.Order}
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

//TODO: the code need be restructured
func aggregateSlice(src []interface{}, fs []*AggregateOpr, asSequential bool, asParallel bool) []interface{} {
	//fmt.Println("aggregateSlice0", src)
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
	//fmt.Println("aggregateSlice ", src, "return", rs)
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

	//fmt.Println("count", count)
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

//aggregate functions---------------------------------------------------
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

func getMinOpr(less func(interface{}, interface{}) bool) *AggregateOpr {
	fun := func(a interface{}, b interface{}) interface{} {
		return minOpr(a, b, less)
	}
	return &AggregateOpr{0, fun, fun}
}

func getMaxOpr(less func(interface{}, interface{}) bool) *AggregateOpr {
	fun := func(a interface{}, b interface{}) interface{} {
		return maxOpr(a, b, less)
	}
	return &AggregateOpr{0, fun, fun}
}

func getCountByOpr(predicate func(interface{}) bool) *AggregateOpr {
	fun := func(v interface{}, t interface{}) interface{} {
		if predicate(v) {
			t = t.(int) + 1
		}
		return t
	}
	return &AggregateOpr{0, fun, sumOpr}
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
