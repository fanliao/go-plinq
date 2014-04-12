package main

import (
	"fmt"
	//"time"
	"errors"
	"github.com/fanliao/go-promise"
	"reflect"
	"runtime"
	"unsafe"
)

const (
	ptrSize        = unsafe.Sizeof((*byte)(nil))
	kindMask       = 0x7f
	kindNoPointers = 0x80
)

var (
	numCPU             int
	ErrUnsupportSource = errors.New("unsupport dataSource")
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
	ToChan() chan interface{}
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

func (this listSource) ToChan() chan interface{} {
	out := make(chan interface{})
	go func() {
		for _, v := range this.ToSlice(true) {
			out <- v
		}
		close(out)
	}()
	return out
}

type chanSource struct {
	data chan *chunk
}

func (this chanSource) Typ() int {
	return SOURCE_CHUNK
}

func (this chanSource) Itr() func() (*chunk, bool) {
	ch := this.data
	return func() (*chunk, bool) {
		c, ok := <-ch
		return c, ok
	}
}

func (this chanSource) Close() {
	close(this.data)
}

func (this chanSource) ToSlice(keepOrder bool) []interface{} {
	chunks := make([]interface{}, 0, 2)
	for c := range this.data {
		chunks = appendSlice(chunks, c)
	}
	return expandChunks(chunks, keepOrder)
}

func (this chanSource) ToChan() chan interface{} {
	out := make(chan interface{})
	go func() {
		for c := range this.data {
			for _, v := range c.data {
				out <- v
			}
		}
	}()
	return out
}

type KeyValue struct {
	key   interface{}
	value interface{}
}

type HKeyValue struct {
	keyHash uint64
	key     interface{}
	value   interface{}
}

//type hKeyValue struct {
//	keyHash uint64
//	KeyValue
//}

//the queryable struct-------------------------------------------------------------------------
type Queryable struct {
	data      dataSource
	steps     []step
	keepOrder bool
}

func From(src interface{}) (q Queryable) {
	q = Queryable{}
	q.keepOrder = true
	q.steps = make([]step, 0, 4)

	if k := reflect.ValueOf(src).Kind(); k == reflect.Slice || k == reflect.Map {
		q.data = &listSource{data: src}
	} else if s, ok := src.(chan *chunk); ok {
		q.data = &chanSource{data: s}
	} else {
		typ := reflect.TypeOf(src)
		switch typ.Kind() {
		case reflect.Slice:

		case reflect.Chan:
		case reflect.Map:
		default:
		}
		panic(ErrUnsupportSource)
	}
	return
}

func (this Queryable) get() dataSource {
	data := this.data
	for _, step := range this.steps {
		data, this.keepOrder, _ = step.stepAction()(data, this.keepOrder)
	}
	return data
}

func (this Queryable) Results() []interface{} {
	return this.get().ToSlice(this.keepOrder)
}

func (this Queryable) Where(sure func(interface{}) bool) Queryable {
	this.steps = append(this.steps, commonStep{ACT_WHERE, sure, numCPU})
	return this
}

func (this Queryable) Select(selectFunc func(interface{}) interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_SELECT, selectFunc, numCPU})
	return this
}

func (this Queryable) Distinct(distinctFunc func(interface{}) interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_DISTINCT, distinctFunc, numCPU})
	return this
}

func (this Queryable) Order(compare func(interface{}, interface{}) int) Queryable {
	this.steps = append(this.steps, commonStep{ACT_ORDERBY, compare, numCPU})
	return this
}

func (this Queryable) GroupBy(keySelector func(interface{}) interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_GROUPBY, keySelector, numCPU})
	return this
}

func (this Queryable) hGroupBy(keySelector func(interface{}) interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_HGROUPBY, keySelector, numCPU})
	return this
}

func (this Queryable) Union(source2 interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_UNION, source2, numCPU})
	return this
}

func (this Queryable) Concat(source2 interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_CONCAT, source2, numCPU})
	return this
}

func (this Queryable) Intersect(source2 interface{}) Queryable {
	this.steps = append(this.steps, commonStep{ACT_INTERSECT, source2, numCPU})
	return this
}

func (this Queryable) Join(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, interface{}) interface{}) Queryable {
	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, numCPU}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

func (this Queryable) LeftJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, interface{}) interface{}) Queryable {
	this.steps = append(this.steps, joinStep{commonStep{ACT_JOIN, inner, numCPU}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

func (this Queryable) GroupJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, []interface{}) interface{}) Queryable {
	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, numCPU}, outerKeySelector, innerKeySelector, resultSelector, false})
	return this
}

func (this Queryable) LeftGroupJoin(inner interface{},
	outerKeySelector func(interface{}) interface{},
	innerKeySelector func(interface{}) interface{},
	resultSelector func(interface{}, []interface{}) interface{}) Queryable {
	this.steps = append(this.steps, joinStep{commonStep{ACT_GROUPJOIN, inner, numCPU}, outerKeySelector, innerKeySelector, resultSelector, true})
	return this
}

func (this Queryable) KeepOrder(keep bool) Queryable {
	this.keepOrder = keep
	return this
}

//the struct and functions of step-------------------------------------------------------------------------
type stepAction func(dataSource, bool) (dataSource, bool, error)
type step interface {
	stepAction() stepAction
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

func getWhere(sure func(interface{}) bool, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dst dataSource, keep bool, e error) {
		var f *promise.Future
		mapChunk := func(c *chunk) *chunk {
			return filterChunk(c, sure)
			//fmt.Println("src=", c, "result=", result)
		}

		switch s := src.(type) {
		case *listSource:
			f = parallelMapList(s, mapChunk, degree)
		case *chanSource:
			reduceSrc := make(chan *chunk)

			f = parallelMapChan(s, reduceSrc, mapChunk, degree)

			results := make([]interface{}, 0, 1)
			if keepOrder {
				avl := newChunkAvlTree()
				reduceChan(f.GetChan(), reduceSrc, func(v *chunk) { avl.Insert(v) })
				results = avl.ToSlice()
				keepOrder = false
			} else {
				reduceChan(f.GetChan(), reduceSrc, func(v *chunk) { results = appendSlice(results, v) })
			}

			f = promise.Wrap(results)
		}

		dst, e = getFutureResult(f, func(results []interface{}) dataSource {
			result := expandChunks(results, false)
			return &listSource{result}
		})
		keep = keepOrder
		return
	})
}

func getSelect(selectFunc func(interface{}) interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dst dataSource, keep bool, e error) {
		var f *promise.Future
		keep = keepOrder

		switch s := src.(type) {
		case *listSource:
			l := len(s.ToSlice(false))
			results := make([]interface{}, l, l)
			f = parallelMapList(s, func(c *chunk) *chunk {
				out := results[c.order : c.order+len(c.data)]
				mapSlice(c.data, selectFunc, &out)
				return nil
			}, degree)
			dst, e = getFutureResult(f, func(r []interface{}) dataSource {
				//fmt.Println("results=", results)
				return &listSource{results}
			})
			return
		case *chanSource:
			out := make(chan *chunk)

			_ = parallelMapChan(s, out, func(c *chunk) *chunk {
				result := make([]interface{}, 0, len(c.data)) //c.end-c.start+2)
				mapSlice(c.data, selectFunc, &result)
				return &chunk{result, c.order}
			}, degree)

			//todo: how to handle error in promise?
			dst, e = &chanSource{out}, nil
			return
		}

		panic(ErrUnsupportSource)
	})

}

func getOrder(compare func(interface{}, interface{}) int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dst dataSource, keep bool, e error) {
		switch s := src.(type) {
		case *listSource:
			sorteds := sortSlice(s.ToSlice(false), func(this, that interface{}) bool {
				return compare(this, that) == -1
			})
			return &listSource{sorteds}, true, nil
		case *chanSource:
			avl := NewAvlTree(compare)
			f := parallelMapChan(s, nil, func(c *chunk) *chunk {
				for _, v := range c.data {
					avl.Insert(v)
				}
				return nil
			}, 1)

			dst, e = getFutureResult(f, func(r []interface{}) dataSource {
				return &listSource{avl.ToSlice()}
			})
			keep = true
			return
		}
		panic(ErrUnsupportSource)
	})
}

func getDistinct(distinctFunc func(interface{}) interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {
		reduceSrc := make(chan *chunk)
		mapChunk := func(c *chunk) (r *chunk) {
			reduceSrc <- &chunk{getKeyValues(c, distinctFunc, nil), c.order}
			return
		}

		//get all values and keys
		var f *promise.Future
		switch s := src.(type) {
		case *listSource:
			f = parallelMapList(s, mapChunk, degree)
		case *chanSource:
			f = parallelMapChan(s, nil, mapChunk, degree)
		}

		//get distinct values
		distKvs := make(map[uint64]int)
		chunks := make([]interface{}, 0, degree)
		reduceChan(f.GetChan(), reduceSrc, func(c *chunk) {
			chunks = appendSlice(chunks, c)
			result := make([]interface{}, len(c.data))
			i := 0
			for _, v := range c.data {
				kv := v.(*HKeyValue)
				if _, ok := distKvs[kv.keyHash]; !ok {
					distKvs[kv.keyHash] = 1
					//result = appendSlice(result, kv.value)
					result[i] = kv.value
					i++
				}
			}
			c.data = result[0:i]
		})

		//get distinct values
		result := expandChunks(chunks, false)
		return &listSource{result}, keepOrder, nil
	})
}

//note the groupby cannot keep order because the map cannot keep order
func getGroupBy(groupFunc func(interface{}) interface{}, hashAsKey bool, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {
		var getKey func(*HKeyValue) interface{}
		if hashAsKey {
			getKey = func(kv *HKeyValue) interface{} { return kv.keyHash }
		} else {
			getKey = func(kv *HKeyValue) interface{} { return kv.key }
		}

		groupKvs := make(map[interface{}]interface{})
		groupKv := func(v interface{}) {
			kv := v.(*HKeyValue)
			k := getKey(kv)
			if v, ok := groupKvs[k]; !ok {
				groupKvs[k] = []interface{}{kv.value}
			} else {
				list := v.([]interface{})
				groupKvs[k] = appendSlice(list, kv.value)
			}
		}

		reduceSrc := make(chan *chunk)
		mapChunk := func(c *chunk) (r *chunk) {
			reduceSrc <- &chunk{getKeyValues(c, groupFunc, nil), c.order}
			return
		}

		//get all values and keys
		var f *promise.Future
		switch s := src.(type) {
		case *listSource:
			f = parallelMapList(s, mapChunk, degree)
		case *chanSource:
			f = parallelMapChan(s, nil, mapChunk, degree)
		}

		//get key with group values values
		reduceChan(f.GetChan(), reduceSrc, func(c *chunk) {
			for _, v := range c.data {
				groupKv(v)
			}
		})

		return &listSource{groupKvs}, keepOrder, nil
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
	return stepAction(func(src dataSource, keepOrder bool) (dst dataSource, keep bool, e error) {
		keep = keepOrder
		innerKVtask := promise.Start(func() []interface{} {
			innerKvs := From(inner).hGroupBy(innerKeySelector).get().(*listSource).data
			return []interface{}{innerKvs, true}
		})

		mapChunk := func(c *chunk) (r *chunk) {
			defer func() {
				if e := recover(); e != nil {
					fmt.Println(e)
				}
			}()
			outerKvs := getKeyValues(c, outerKeySelector, nil)
			results := make([]interface{}, 0, 10)

			if r, ok := innerKVtask.Get(); ok != promise.RESULT_SUCCESS {
				//todo:
				fmt.Println("error", ok, r)
			} else {
				innerKvs := r[0].(map[interface{}]interface{})

				for _, o := range outerKvs {
					outerkv := o.(*HKeyValue)
					if innerList, ok := innerKvs[outerkv.keyHash]; ok {
						//fmt.Println("outer", *outerkv, "inner", innerList)
						matchSelector(outerkv, innerList.([]interface{}), &results)
						//innerList1 := innerList.([]interface{})
						//for _, iv := range innerList1 {
						//	results = appendSlice(results, resultSelector(outerkv.value, iv))
						//}
					} else if isLeftJoin {
						//fmt.Println("outer", *outerkv)
						unmatchSelector(outerkv, &results)
						//results = appendSlice(results, resultSelector(outerkv.value, nil))
					}
				}
			}

			return &chunk{results, c.order}
		}

		switch s := src.(type) {
		case *listSource:
			outerKeySelectorFuture := parallelMapList(s, mapChunk, degree)
			dst, e = getFutureResult(outerKeySelectorFuture, func(results []interface{}) dataSource {
				result := expandChunks(results, false)
				return &listSource{result}
			})
			return
		case *chanSource:
			out := make(chan *chunk)
			_ = parallelMapChan(s, nil, mapChunk, degree)
			dst, e = &chanSource{out}, nil
			return
		}

		return nil, keep, nil
	})
}

func getUnion(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {
		reduceSrc := make(chan *chunk)
		//m := tMap{new(sync.Mutex), make(map[uint64]interface{})}
		mapChunk := func(c *chunk) (r *chunk) {
			reduceSrc <- &chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
			//for _, v := range c.data {
			//	m.add(tHash(v), v)
			//}
			return
		}

		//get all values and keys
		var f *promise.Future
		switch s := src.(type) {
		case *listSource:
			f = parallelMapList(s, mapChunk, degree)
		case *chanSource:
			f = parallelMapChan(s, nil, mapChunk, degree)
		}

		dataSource2 := From(source2).data
		var f1 *promise.Future
		switch s := dataSource2.(type) {
		case *listSource:
			f1 = parallelMapList(s, mapChunk, degree)
		case *chanSource:
			f1 = parallelMapChan(s, nil, mapChunk, degree)
		}

		f2 := promise.WhenAll(f, f1)

		//get distinct values
		distKvs := make(map[uint64]bool)
		chunks := make([]interface{}, 0, 2*degree)
		//result := make([]interface{}, 100000, 100000)
		//i := 0
		reduceChan(f2.GetChan(), reduceSrc, func(c *chunk) {
			chunks = appendSlice(chunks, c)
			result := make([]interface{}, len(c.data), len(c.data))
			i := 0
			for _, v := range c.data {
				kv := v.(*HKeyValue)
				if _, ok := distKvs[kv.keyHash]; !ok {
					distKvs[kv.keyHash] = true
					result[i] = kv.value
					i++
				}
			}
			//fmt.Println("union", len(result))
			c.data = result[0:i]
		})

		//get distinct values
		result := expandChunks(chunks, false)

		return &listSource{result}, keepOrder, nil

		//set := make(map[uint64]interface{})
		//for _, v := range src.ToSlice(false) {
		//	hashValue := tHash(v)
		//	if _, ok := set[hashValue]; !ok {
		//		set[hashValue] = v
		//	}
		//}
		//for _, v := range From(source2).Results() {
		//	hashValue := tHash(v)
		//	if _, ok := set[hashValue]; !ok {
		//		set[hashValue] = v
		//	}
		//}

		////set := make(map[interface{}]interface{})
		////for _, v := range src.ToSlice(false) {
		////	if _, ok := set[v]; !ok {
		////		set[v] = v
		////	}
		////}
		////for _, v := range From(source2).Results() {
		////	if _, ok := set[v]; !ok {
		////		set[v] = v
		////	}
		////}
		//result := make([]interface{}, len(set))
		//i := 0
		//for _, v := range set {
		//	result[i] = v
		//	i++
		//}
		//return &listSource{result}, keepOrder, nil
	})
}

func getConcat(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {
		slice1, slice2 := src.ToSlice(keepOrder), From(source2).KeepOrder(keepOrder).Results()

		result := make([]interface{}, len(slice1)+len(slice2))
		_ = copy(result[0:len(slice1)], slice1)
		_ = copy(result[len(slice1):len(slice1)+len(slice2)], slice2)
		return &listSource{result}, keepOrder, nil
	})
}

func getIntersect(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {

		distKvs := make(map[uint64]bool)

		f1 := promise.Start(func() []interface{} {
			reduceSrc := make(chan *chunk)
			mapChunk := func(c *chunk) (r *chunk) {
				reduceSrc <- &chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
				return
			}
			//get all values and keys
			var f *promise.Future
			switch s := src.(type) {
			case *listSource:
				f = parallelMapList(s, mapChunk, degree)
			case *chanSource:
				f = parallelMapChan(s, nil, mapChunk, degree)
			}
			//get distinct values of src1
			reduceChan(f.GetChan(), reduceSrc, func(c *chunk) {
				for _, v := range c.data {
					kv := v.(*HKeyValue)
					if _, ok := distKvs[kv.keyHash]; !ok {
						distKvs[kv.keyHash] = true
					}
				}

			})
			return nil
		})

		dataSource2 := From(source2).Select(func(v interface{}) interface{} {
			return &KeyValue{tHash(v), v}
		}).Results()

		_, _ = f1.Get()

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

		return &listSource{result[0:i]}, keepOrder, nil

	})
}

//util funcs------------------------------------------
func parallelMapChan(src *chanSource, out chan *chunk, task func(*chunk) *chunk, degree int) *promise.Future {
	itr := src.Itr()
	fs := make([]*promise.Future, degree, degree)
	for i := 0; i < degree; i++ {
		f := promise.Start(func() []interface{} {
			for {
				if c, ok := itr(); ok {
					if reflect.ValueOf(c).IsNil() {
						src.Close()
						break
					}
					d := task(c)
					if out != nil {
						out <- d
					}
				} else {
					break
				}
			}
			return nil
			//fmt.Println("r=", r)
		})
		fs[i] = f
	}
	f := promise.WhenAll(fs...)

	return f
}

func parallelMapList(src dataSource, task func(*chunk) *chunk, degree int) *promise.Future {
	fs := make([]*promise.Future, degree, degree)
	data := src.ToSlice(false)
	len := len(data)
	size := ceilSplitSize(len, degree)
	j := 0
	for i := 0; i < degree && i*size < len; i++ {
		end := (i + 1) * size
		if end >= len {
			end = len
		}
		c := &chunk{data[i*size : end], i * size} //, end}

		f := promise.Start(func() []interface{} {
			r := task(c)
			return []interface{}{r, true}
		})
		fs[i] = f
		j++
	}
	f := promise.WhenAll(fs[0:j]...)

	return f
}

func reduceChan(chEndFlag chan *promise.PromiseResult, src chan *chunk, reduce func(*chunk)) {
	for {
		select {
		case <-chEndFlag:
			return
		case v, _ := <-src:
			reduce(v)
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
		list := (make([]interface{}, len(c.data), len(c.data)))
		KeyValues = &list
	}
	mapSlice(c.data, func(v interface{}) interface{} {
		k := keyFunc(v)
		return &HKeyValue{tHash(k), k, v}
	}, KeyValues)
	return *KeyValues
}
