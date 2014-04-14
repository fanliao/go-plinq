package main

import (
	"fmt"
	//"time"
	"errors"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

const (
	ptrSize          = unsafe.Sizeof((*byte)(nil))
	kindMask         = 0x7f
	kindNoPointers   = 0x80
	DEFAULTCHUNKSIZE = 200
)

var (
	numCPU               int
	ErrUnsupportSource   = errors.New("unsupport dataSource")
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

// the struct and interface about data dataSource---------------------------------------------------
type chunk struct {
	data  []interface{}
	order int
}

const (
	SOURCE_BLOCK int = iota
	SOURCE_CHUNK
)

type dataSource interface {
	Typ() int //block or chan?
	ToSlice(bool) []interface{}
	//ToChan() chan interface{}
}

type listSource struct {
	data interface{}
}

func (this listSource) Typ() int {
	return SOURCE_BLOCK
}

func (this listSource) ToSlice(keepOrder bool) []interface{} {
	switch data := this.data.(type) {
	case []interface{}:
		return data
	case map[interface{}]interface{}:
		i := 0
		results := make([]interface{}, len(data), len(data))
		for k, v := range data {
			results[i] = &KeyValue{k, v}
			i++
		}
		return results
	default:
		value := reflect.ValueOf(this.data)
		switch value.Kind() {
		case reflect.Slice:
			l := value.Len()
			results := make([]interface{}, l, l)
			for i := 0; i < l; i++ {
				results[i] = value.Index(i).Interface()
			}
			return results
		case reflect.Map:
			l := value.Len()
			results := make([]interface{}, l, l)
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

type chanSource struct {
	//data1 chan *chunk
	once      *sync.Once
	data1     interface{}
	chunkChan chan *chunk
	//chunkSize int
}

func (this chanSource) Typ() int {
	return SOURCE_CHUNK
}

func (this *chanSource) makeChunkChanSure(chunkSize int) {
	if this.chunkChan == nil {
		this.once.Do(func() {
			if this.chunkChan != nil {
				return
			}
			srcChan := reflect.ValueOf(this.data1)
			//chunkChan := make(chan *chunk)
			this.chunkChan = make(chan *chunk)
			go func() {
				chunkData := make([]interface{}, 0, chunkSize)
				i := 0
				for {
					if v, ok := srcChan.Recv(); ok {
						chunkData = append(chunkData, v.Interface())
						if len(chunkData) == cap(chunkData) {
							this.chunkChan <- &chunk{chunkData, i}
							i++
							chunkData = make([]interface{}, 0, chunkSize)
						}
					} else {
						break
					}
				}
				if len(chunkData) > 0 {
					this.chunkChan <- &chunk{chunkData, i}
				}
				this.chunkChan <- nil
			}()
		})
	}
}

func (this *chanSource) Itr(chunkSize int) func() (*chunk, bool) {
	this.makeChunkChanSure(chunkSize)
	ch := this.chunkChan
	return func() (*chunk, bool) {
		c, ok := <-ch
		//fmt.Println("chanSource receive", c)
		return c, ok
	}
}

func (this chanSource) Close() {
	if this.chunkChan != nil {
		close(this.chunkChan)
	}
	//if chunkChan, ok := this.data1.(chan *chunk); ok {
	//} else {
	//	srcChan := reflect.ValueOf(this.data1)
	//	if srcChan.Kind() != reflect.Chan {
	//		panic(ErrUnsupportSource)
	//	}
	//	srcChan.Close()
	//}
}

//receive all chunks from chan and return a slice includes all items
func (this chanSource) ToSlice(keepOrder bool) []interface{} {
	if this.chunkChan != nil {
		chunks := make([]interface{}, 0, 2)
		avl := newChunkAvlTree()

		//fmt.Println("ToSlice start receive chunk")
		for c := range this.chunkChan {
			//if use the buffer channel, then must receive a nil as end flag
			if reflect.ValueOf(c).IsNil() {
				//fmt.Println("ToSlice receive a nil")
				this.Close()
				//fmt.Println("close chan")
				break
			}

			//fmt.Println("ToSlice receive a chunk", *c)
			if keepOrder {
				avl.Insert(c)
			} else {
				chunks = appendSlice(chunks, c)
			}
			//fmt.Println("ToSlice end receive a chunk")
		}
		if keepOrder {
			chunks = avl.ToSlice()
		}

		return expandChunks(chunks, false)
	} else {
		srcChan := reflect.ValueOf(this.data1)
		if srcChan.Kind() != reflect.Chan {
			panic(ErrUnsupportSource)
		}
		result := make([]interface{}, 2)
		for {
			if v, ok := srcChan.Recv(); ok {
				result = append(result, v)
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

type KeyValue struct {
	key   interface{}
	value interface{}
}

type HKeyValue struct {
	keyHash uint64
	key     interface{}
	value   interface{}
}

//the queryable struct-------------------------------------------------------------------------
type parallelOption struct {
	degree    int
	chunkSize int
	keepOrder bool
}

type Queryable struct {
	data     dataSource
	steps    []step
	stepErrs []interface{}
	parallelOption
	//degree    int
	//chunkSize int
}

func From(src interface{}) (q *Queryable) {
	if src == nil {
		panic(ErrNilSource)
	}

	q = &Queryable{}
	q.keepOrder = true
	q.steps = make([]step, 0, 4)
	q.degree = numCPU
	q.chunkSize = DEFAULTCHUNKSIZE

	if k := reflect.ValueOf(src).Kind(); k == reflect.Slice || k == reflect.Map {
		q.data = &listSource{data: src}
	} else if s, ok := src.(chan *chunk); ok {
		q.data = &chanSource{chunkChan: s}
	} else if k == reflect.Chan {
		q.data = &chanSource{new(sync.Once), src, nil}
	} else {
		panic(ErrUnsupportSource)
	}
	return
}

func (this *Queryable) Results() (results []interface{}, err error) {
	if ds, e := this.get(); e == nil {
		results = ds.ToSlice(this.keepOrder)
		if len(this.stepErrs) > 0 {
			err = NewLinqError("Aggregate errors", this.stepErrs)
		}
		return
	} else {
		return nil, e
	}
}

func (this *Queryable) Where(sure func(interface{}) bool, degrees ...int) *Queryable {
	if sure == nil {
		panic(ErrNilAction)
	}

	this.steps = append(this.steps, commonStep{ACT_WHERE, sure, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) Select(selectFunc func(interface{}) interface{}, degrees ...int) *Queryable {
	if selectFunc == nil {
		panic(ErrNilAction)
	}
	this.steps = append(this.steps, commonStep{ACT_SELECT, selectFunc, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) Distinct(distinctFunc func(interface{}) interface{}, degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_DISTINCT, distinctFunc, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) Order(compare func(interface{}, interface{}) int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_ORDERBY, compare, this.degree})
	return this
}

func (this *Queryable) GroupBy(keySelector func(interface{}) interface{}, degrees ...int) *Queryable {
	if keySelector == nil {
		panic(ErrNilAction)
	}
	this.steps = append(this.steps, commonStep{ACT_GROUPBY, keySelector, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) hGroupBy(keySelector func(interface{}) interface{}, degrees ...int) *Queryable {
	this.steps = append(this.steps, commonStep{ACT_HGROUPBY, keySelector, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) Union(source2 interface{}, degrees ...int) *Queryable {
	if source2 == nil {
		panic(ErrUnionNilSource)
	}
	this.steps = append(this.steps, commonStep{ACT_UNION, source2, getDegreeArg(degrees...)})
	return this
}

func (this *Queryable) Concat(source2 interface{}) *Queryable {
	if source2 == nil {
		panic(ErrConcatNilSource)
	}
	this.steps = append(this.steps, commonStep{ACT_CONCAT, source2, this.degree})
	return this
}

func (this *Queryable) Intersect(source2 interface{}, degrees ...int) *Queryable {
	if source2 == nil {
		panic(ErrInterestNilSource)
	}
	this.steps = append(this.steps, commonStep{ACT_INTERSECT, source2, getDegreeArg(degrees...)})
	return this
}

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

func (this *Queryable) KeepOrder(keep bool) *Queryable {
	this.keepOrder = keep
	return this
}

func (this *Queryable) SetDegreeOfParallelism(degree int) *Queryable {
	this.degree = degree
	return this
}

func (this *Queryable) SetSizeOfChunk(size int) *Queryable {
	this.chunkSize = size
	return this
}

func (this *Queryable) get() (data dataSource, err error) {
	//collect the errors for the pipeline mode step
	errChan := make(chan *stepErr)
	go func() {
		//fmt.Println("start receive errors")
		for e := range errChan {
			this.stepErrs = appendSlice(this.stepErrs, e)
		}
		//fmt.Println("end receive errors")
	}()

	var keepOrder bool = this.parallelOption.keepOrder
	data = this.data
	parallelOption := this.parallelOption
	for _, step := range this.steps {
		var f *promise.Future
		data, f, keepOrder, err = step.stepAction()(data, parallelOption)
		if err != nil {
			return nil, err
		}

		step1 := step
		if f != nil {
			f.Fail(func(results ...interface{}) {
				errChan <- NewStepError(step1.getTyp(), results)
				//fmt.Println("end fail", stepErr{step.getTyp(), results})
			})
		}

		parallelOption.keepOrder = keepOrder
	}

	return data, nil
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
)

type stepAction func(dataSource, parallelOption) (dataSource, *promise.Future, bool, error)
type step interface {
	stepAction() stepAction
	getTyp() int
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

func (this commonStep) getTyp() int { return this.typ }

func (this commonStep) stepAction() (act stepAction) {
	switch this.typ {
	case ACT_SELECT:
		act = getSelect(this.act.(func(interface{}) interface{}), this.degree)
	case ACT_WHERE:
		act = getWhere(this.act.(func(interface{}) bool), this.degree)
	case ACT_DISTINCT:
		act = getDistinct(this.act.(func(interface{}) interface{}), this.degree)
	case ACT_ORDERBY:
		act = getOrder(this.act.(func(interface{}, interface{}) int))
	case ACT_GROUPBY:
		act = getGroupBy(this.act.(func(interface{}) interface{}), false, this.degree)
	case ACT_HGROUPBY:
		act = getGroupBy(this.act.(func(interface{}) interface{}), true, this.degree)
	case ACT_UNION:
		act = getUnion(this.act, this.degree)
	case ACT_CONCAT:
		act = getConcat(this.act, this.degree)
	case ACT_INTERSECT:
		act = getIntersect(this.act, this.degree)
	}
	return
}

func (this joinStep) stepAction() (act stepAction) {
	switch this.typ {
	case ACT_JOIN:
		act = getJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(func(interface{}, interface{}) interface{}), this.isLeftJoin, this.degree)
	case ACT_GROUPJOIN:
		act = getGroupJoin(this.act, this.outerKeySelector, this.innerKeySelector,
			this.resultSelector.(func(interface{}, []interface{}) interface{}), this.isLeftJoin, this.degree)
	}
	return
}

func getSelect(selectFunc func(interface{}) interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dst dataSource, sf *promise.Future, keep bool, e error) {
		stepParallelOption(&option, degree)
		keep = option.keepOrder

		switch s := src.(type) {
		case *listSource:
			l := len(s.ToSlice(false))
			results := make([]interface{}, l, l)
			f := parallelMapListToList(s, func(c *chunk) *chunk {
				out := results[c.order : c.order+len(c.data)]
				mapSlice(c.data, selectFunc, &out)
				return nil
			}, &option)
			dst, e = getFutureResult(f, func(r []interface{}) dataSource {
				//fmt.Println("results=", results)
				return &listSource{results}
			})
			return
		case *chanSource:
			//out := make(chan *chunk)

			f, out := parallelMapChanToChan(s, nil, func(c *chunk) *chunk {
				result := make([]interface{}, len(c.data)) //c.end-c.start+2)
				mapSlice(c.data, selectFunc, &result)
				return &chunk{result, c.order}
			}, &option)

			sf = f
			//todo: how to handle error in promise?
			//fmt.Println("select return out chan")
			dst, e = &chanSource{chunkChan: out}, nil
			//noted when use pipeline mode to start next step, the future of current step must be returned
			//otherwise the errors in current step will be missed
			return
		}

		panic(ErrUnsupportSource)
	})

}

func getOrder(compare func(interface{}, interface{}) int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dst dataSource, sf *promise.Future, keep bool, e error) {
		//order be not parallel
		option.degree = 1

		switch s := src.(type) {
		case *listSource:
			sorteds := sortSlice(s.ToSlice(false), func(this, that interface{}) bool {
				return compare(this, that) == -1
			})
			return &listSource{sorteds}, nil, true, nil
		case *chanSource:
			avl := NewAvlTree(compare)
			f, _ := parallelMapChanToChan(s, nil, func(c *chunk) *chunk {
				for _, v := range c.data {
					avl.Insert(v)
				}
				return nil
			}, &option)

			dst, e = getFutureResult(f, func(r []interface{}) dataSource {
				return &listSource{avl.ToSlice()}
			})
			keep = true
			return
		}
		panic(ErrUnsupportSource)
	})
}

func getWhere(sure func(interface{}) bool, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dst dataSource, sf *promise.Future, keep bool, e error) {
		stepParallelOption(&option, degree)
		mapChunk := func(c *chunk) (r *chunk) {
			r = filterChunk(c, sure)
			return
		}

		if list, oneDegree := isOneDegree(src, option); oneDegree {
			c := &chunk{list.ToSlice(false), 0}
			r := mapChunk(c)
			return &listSource{r.data}, nil, option.keepOrder, nil
		} else if list != nil {
			src = list
		}
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, &option)

		//fmt.Println("return where")
		return &chanSource{chunkChan: reduceSrc}, f, option.keepOrder, nil
	})
}

func getDistinct(distinctFunc func(interface{}) interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dataSource, *promise.Future, bool, error) {
		stepParallelOption(&option, degree)
		mapChunk := func(c *chunk) (r *chunk) {
			r = &chunk{getKeyValues(c, distinctFunc, nil), c.order}
			return
		}

		f, reduceSrcChan := parallelMapToChan(src, nil, mapChunk, &option)

		//get distinct values
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
func getGroupBy(groupFunc func(interface{}) interface{}, hashAsKey bool, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dataSource, *promise.Future, bool, error) {
		stepParallelOption(&option, degree)
		mapChunk := func(c *chunk) (r *chunk) {
			r = &chunk{getKeyValues(c, groupFunc, nil), c.order}
			return
		}

		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, &option)

		groupKvs := make(map[interface{}]interface{})
		groupKv := func(v interface{}) {
			kv := v.(*HKeyValue)
			k := iif(hashAsKey, kv.keyHash, kv.key)
			if v, ok := groupKvs[k]; !ok {
				groupKvs[k] = []interface{}{kv.value}
			} else {
				list := v.([]interface{})
				groupKvs[k] = appendSlice(list, kv.value)
			}
		}

		//get key with group values values
		errs := reduceChan(f.GetChan(), reduceSrc, func(c *chunk) {
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
	resultSelector func(interface{}, interface{}) interface{}, isLeftJoin bool, degree int) stepAction {
	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *HKeyValue, innerList []interface{}, results *[]interface{}) {
			for _, iv := range innerList {
				*results = appendSlice(*results, resultSelector(outerkv.value, iv))
			}
		}, func(outerkv *HKeyValue, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, nil))
		}, isLeftJoin, degree)
}

func getGroupJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, []interface{}) interface{}, isLeftJoin bool, degree int) stepAction {

	return getJoinImpl(inner, outerKeySelector, innerKeySelector,
		func(outerkv *HKeyValue, innerList []interface{}, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, innerList))
		}, func(outerkv *HKeyValue, results *[]interface{}) {
			*results = appendSlice(*results, resultSelector(outerkv.value, []interface{}{}))
		}, isLeftJoin, degree)
}

func getJoinImpl(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	matchSelector func(*HKeyValue, []interface{}, *[]interface{}),
	unmatchSelector func(*HKeyValue, *[]interface{}), isLeftJoin bool, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dst dataSource, sf *promise.Future, keep bool, e error) {
		stepParallelOption(&option, degree)
		keep = option.keepOrder
		innerKVtask := promise.Start(func() []interface{} {
			if innerKvsDs, err := From(inner).hGroupBy(innerKeySelector).get(); err == nil {
				return []interface{}{innerKvsDs.(*listSource).data, true}
			} else {
				return []interface{}{err, false}
			}
		})

		mapChunk := func(c *chunk) (r *chunk) {
			outerKvs := getKeyValues(c, outerKeySelector, nil)
			results := make([]interface{}, 0, 10)

			if r, ok := innerKVtask.Get(); ok != promise.RESULT_SUCCESS {
				//todo:
				fmt.Println("innerKV get error", ok, r)
			} else {
				innerKvs := r[0].(map[interface{}]interface{})

				for _, o := range outerKvs {
					outerkv := o.(*HKeyValue)
					if innerList, ok := innerKvs[outerkv.keyHash]; ok {
						//fmt.Println("outer", *outerkv, "inner", innerList)
						matchSelector(outerkv, innerList.([]interface{}), &results)
					} else if isLeftJoin {
						//fmt.Println("outer", *outerkv)
						unmatchSelector(outerkv, &results)
					}
				}
			}

			return &chunk{results, c.order}
		}

		switch s := src.(type) {
		case *listSource:
			outerKeySelectorFuture := parallelMapListToList(s, mapChunk, &option)
			dst, e = getFutureResult(outerKeySelectorFuture, func(results []interface{}) dataSource {
				result := expandChunks(results, false)
				return &listSource{result}
			})
			return
		case *chanSource:
			//out := make(chan *chunk)
			f, out := parallelMapChanToChan(s, nil, mapChunk, &option)
			dst, sf, e = &chanSource{chunkChan: out}, f, nil
			return
		}

		panic(ErrUnsupportSource)
	})
}

func getUnion(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dataSource, *promise.Future, bool, error) {
		stepParallelOption(&option, degree)
		reduceSrcChan := make(chan *chunk)
		mapChunk := func(c *chunk) (r *chunk) {
			r = &chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
			return
		}

		f1, reduceSrcChan := parallelMapToChan(src, reduceSrcChan, mapChunk, &option)

		f2, reduceSrcChan := parallelMapToChan(From(source2).data, reduceSrcChan, mapChunk, &option)

		mapFuture := promise.WhenAll(f1, f2)

		if chunks, err := reduceDistinctVals(mapFuture, reduceSrcChan); err == nil {
			//get distinct values
			result := expandChunks(chunks, false)

			return &listSource{result}, nil, option.keepOrder, nil
		} else {
			return nil, nil, option.keepOrder, err
		}

	})
}

func getConcat(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dataSource, *promise.Future, bool, error) {
		stepParallelOption(&option, degree)
		slice1 := src.ToSlice(option.keepOrder)
		slice2, err2 := From(source2).KeepOrder(option.keepOrder).Results()

		if err2 != nil {
			return nil, nil, option.keepOrder, err2
		}

		result := make([]interface{}, len(slice1)+len(slice2))
		_ = copy(result[0:len(slice1)], slice1)
		_ = copy(result[len(slice1):len(slice1)+len(slice2)], slice2)
		return &listSource{result}, nil, option.keepOrder, nil
	})
}

func getIntersect(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, option parallelOption) (dataSource, *promise.Future, bool, error) {
		stepParallelOption(&option, degree)
		distKvs := make(map[uint64]bool)

		f1 := promise.Start(func() []interface{} {
			mapChunk := func(c *chunk) (r *chunk) {
				r = &chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
				return
			}

			f, reduceSrc := parallelMapToChan(src, nil, mapChunk, &option)
			//get distinct values of src1
			errs := reduceChan(f.GetChan(), reduceSrc, func(c *chunk) {
				for _, v := range c.data {
					kv := v.(*HKeyValue)
					if _, ok := distKvs[kv.keyHash]; !ok {
						distKvs[kv.keyHash] = true
						//fmt.Println("add", kv.value, "to distKvs")
					}
				}
				//fmt.Println("receive", len(c.data))
				//fmt.Println("len(distKvs)", len(distKvs))
			})
			if errs == nil {
				return nil
			} else {
				return []interface{}{NewLinqError("Group error", errs), false}
			}
		})

		dataSource2, err := From(source2).Select(func(v interface{}) interface{} {
			return &KeyValue{tHash(v), v}
		}).Results()

		if err != nil {
			return nil, nil, option.keepOrder, err
		}

		if r, typ := f1.Get(); typ != promise.RESULT_SUCCESS {
			return nil, nil, option.keepOrder, NewLinqError("Intersect error", r)
		}

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

		//get distinct values
		result := make([]interface{}, len(resultKVs))
		i := 0
		for _, v := range resultKVs {
			result[i] = v
			i++
		}

		return &listSource{result[0:i]}, nil, option.keepOrder, nil

	})
}

//util funcs------------------------------------------
func reduceDistinctVals(mapFuture *promise.Future, reduceSrcChan chan *chunk) ([]interface{}, error) {
	//get distinct values
	chunks := make([]interface{}, 0, 2)
	distKvs := make(map[uint64]int)
	errs := reduceChan(mapFuture.GetChan(), reduceSrcChan, func(c *chunk) {
		chunks = appendSlice(chunks, c)
		result := make([]interface{}, len(c.data))
		i := 0
		for _, v := range c.data {
			kv := v.(*HKeyValue)
			if _, ok := distKvs[kv.keyHash]; !ok {
				distKvs[kv.keyHash] = 1
				result[i] = kv.value
				i++
			}
		}
		c.data = result[0:i]
	})
	if errs == nil {
		return chunks, nil
	} else {
		return nil, NewLinqError("reduceDistinctVals error", errs)
	}
}

func parallelMapToChan(src dataSource, reduceSrcChan chan *chunk, mapChunk func(c *chunk) (r *chunk), option *parallelOption) (f *promise.Future, ch chan *chunk) {
	//get all values and keys
	//var f *promise.Future
	switch s := src.(type) {
	case *listSource:
		return parallelMapListToChan(s, reduceSrcChan, mapChunk, option)
	case *chanSource:
		return parallelMapChanToChan(s, reduceSrcChan, mapChunk, option)
	default:
		panic(ErrUnsupportSource)
	}
	//return f, reduceSrcChan

}

func parallelMapChanToChan(src *chanSource, out chan *chunk, task func(*chunk) *chunk, option *parallelOption) (*promise.Future, chan *chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *chunk, option.degree)
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

func parallelMapListToChan(src dataSource, out chan *chunk, task func(*chunk) *chunk, option *parallelOption) (*promise.Future, chan *chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *chunk, option.degree)
		createOutChan = true
	}

	f := parallelMapList(src, func(c *chunk) func() []interface{} {
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

func addCloseChanCallback(f *promise.Future, out chan *chunk) {
	//fmt.Println("addCloseChanCallback")
	f.Always(func(results ...interface{}) {
		//must use gorouter, else may deadlock when out is buffer chan
		//because it maybe called before the chan receiver be started.
		//if the buffer is full, out <- nil will be holder then deadlock
		//fmt.Println("always")
		//fmt.Println(results...)
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

func parallelMapListToList(src dataSource, task func(*chunk) *chunk, option *parallelOption) *promise.Future {
	return parallelMapList(src, func(c *chunk) func() []interface{} {
		return func() []interface{} {
			r := task(c)
			return []interface{}{r, true}
		}
	}, option)
}

func parallelMapList(src dataSource, getAction func(*chunk) func() []interface{}, option *parallelOption) (f *promise.Future) {
	fs := make([]*promise.Future, option.degree)
	data := src.ToSlice(false)
	len, size, j := len(data), ceilSplitSize(len(data), option.degree), 0

	if size < option.chunkSize {
		size = option.chunkSize
	}

	if size >= len {
		for i := 0; i < option.degree && i*size < len; i++ {
			end := (i + 1) * size
			if end >= len {
				end = len
			}
			c := &chunk{data[i*size : end], i * size} //, end}

			f = promise.Start(getAction(c))
			fs[i] = f
			j++
		}
		//fmt.Println("begin when all")
		f = promise.WhenAll(fs[0:j]...)
		//fmt.Println("when all")
	} else {
		c := &chunk{data[0:len], 0}
		getAction(c)()
		f = promise.Wrap(nil)
	}

	return
}

func reduceChan(chEndFlag chan *promise.PromiseResult, src chan *chunk, reduce func(*chunk)) []interface{} {
	if cap(src) == 0 {
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
		for {
			select {
			case r := <-chEndFlag:
				//fmt.Println("return reduceChan")
				if r != nil && r.Typ != promise.RESULT_SUCCESS {
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

func getFutureResult(f *promise.Future, dataSourceFunc func([]interface{}) dataSource) (dataSource, error) {
	if results, typ := f.Get(); typ != promise.RESULT_SUCCESS {
		//todo
		return nil, nil
	} else {
		//fmt.Println("(results)=", (results))
		return dataSourceFunc(results), nil
	}
}

func filterChunk(c *chunk, f func(interface{}) bool) *chunk {
	result := filterSlice(c.data, f)
	//fmt.Println("c=", c)
	//fmt.Println("result=", result)
	return &chunk{result, c.order}
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
		dst = make([]interface{}, len(src), len(src))
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
				a1, b1 *chunk
			)
			switch v := a.(type) {
			case []interface{}:
				a1, b1 = v[0].(*chunk), b.([]interface{})[0].(*chunk)
			case *chunk:
				a1, b1 = v, b.(*chunk)
			}
			//a1, b1 := a.([]interface{})[0].(*chunk), b.([]interface{})[0].(*chunk)
			return a1.order < b1.order
		})
	}

	count := 0
	chunks := make([]*chunk, len(src), len(src))
	for i, c := range src {
		switch v := c.(type) {
		case []interface{}:
			chunks[i] = v[0].(*chunk)
		case *chunk:
			chunks[i] = v
		}
		count += len(chunks[i].data)
	}

	//fmt.Println("count", count)
	result := make([]interface{}, count, count)
	start := 0
	for _, c := range chunks {
		size := len(c.data)
		copy(result[start:start+size], c.data)
		start += size
	}
	return result
}

//
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

func appendChunkSlice(src []*chunk, v *chunk) []*chunk {
	c, l := cap(src), len(src)
	if c >= l+1 {
		return append(src, v)
	} else {
		//reslice
		newSlice := make([]*chunk, l+1, 2*c)
		_ = copy(newSlice[0:l], src)
		return append(newSlice, v)
	}
}

func ceilSplitSize(a int, b int) int {
	if a%b != 0 {
		return a/b + 1
	} else {
		return a / b
	}
}

//func getHKeyValues(c *chunk, keyFunc func(v interface{}) interface{}, hKeyValues *[]interface{}) []interface{} {
//	return getKeyValues(c, func(v interface{}) interface{} {
//		return tHash(keyFunc(v))
//	}, hKeyValues)
//}

func getKeyValues(c *chunk, keyFunc func(v interface{}) interface{}, KeyValues *[]interface{}) []interface{} {
	if KeyValues == nil {
		list := (make([]interface{}, len(c.data)))
		KeyValues = &list
	}
	mapSlice(c.data, func(v interface{}) interface{} {
		k := keyFunc(v)
		return &HKeyValue{tHash(k), k, v}
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

func stepParallelOption(option *parallelOption, stepDegree int) {
	if stepDegree != 0 {
		option.degree = stepDegree
	}
}

//if the data source is listSource, then computer the degree of paralleliam.
//if the degree is 1, the paralleliam is no need.
func isOneDegree(src dataSource, option parallelOption) (dataSource, bool) {
	if s, ok := src.(*listSource); ok {
		list := s.ToSlice(false)
		s.data = list
		if len(list) <= option.chunkSize {
			return s, true
		} else {
			return s, false
		}
	} else {
		return nil, false
	}
}
