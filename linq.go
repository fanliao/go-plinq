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
	data chan *chunk
}

func (this chanSource) Typ() int {
	return SOURCE_CHUNK
}

func (this chanSource) Itr() func() (*chunk, bool) {
	ch := this.data
	return func() (*chunk, bool) {
		c, ok := <-ch
		//fmt.Println("chanSource receive", c)
		return c, ok
	}
}

func (this chanSource) Close() {
	close(this.data)
}

//receive all chunks from chan and return a slice includes all items
func (this chanSource) ToSlice(keepOrder bool) []interface{} {
	chunks := make([]interface{}, 0, 2)
	avl := newChunkAvlTree()

	//fmt.Println("ToSlice start receive chunk")
	for c := range this.data {
		//fmt.Println("ToSlice receive a chunk", *c)
		//if use the buffer channel, then must receive a nil as end flag
		if reflect.ValueOf(c).IsNil() {
			//fmt.Println("ToSlice receive a nil")
			this.Close()
			//fmt.Println("close chan")
			break
		}

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

func (this Queryable) get() dataSource {
	data := this.data
	for _, step := range this.steps {
		data, this.keepOrder, _ = step.stepAction()(data, this.keepOrder)
	}
	return data
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
	return stepAction(func(src dataSource, keepOrder bool) (dst dataSource, keep bool, e error) {
		var f *promise.Future
		keep = keepOrder

		switch s := src.(type) {
		case *listSource:
			l := len(s.ToSlice(false))
			results := make([]interface{}, l, l)
			f = parallelMapListToList(s, func(c *chunk) *chunk {
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
			//out := make(chan *chunk)

			_, out := parallelMapChanToChan(s, nil, func(c *chunk) *chunk {
				defer func() {
					if e := recover(); e != nil {
						fmt.Println(e)
					}
				}()
				result := make([]interface{}, len(c.data)) //c.end-c.start+2)
				mapSlice(c.data, selectFunc, &result)
				return &chunk{result, c.order}
			}, degree)

			//_ = f
			//todo: how to handle error in promise?
			//fmt.Println("select return out chan")
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
			f, _ := parallelMapChanToChan(s, nil, func(c *chunk) *chunk {
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

func getWhere(sure func(interface{}) bool, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dst dataSource, keep bool, e error) {
		mapChunk := func(c *chunk) (r *chunk) {
			r = filterChunk(c, sure)
			return
		}

		//reduceSrc := make(chan *chunk, 1)
		//var f *promise.Future
		//switch s := src.(type) {
		//case *listSource:
		//	f = parallelMapListToChan(s, reduceSrc, mapChunk, degree)
		//case *chanSource:
		//	f = parallelMapChan(s, reduceSrc, mapChunk, degree)
		//}
		_, reduceSrc := parallelMapToChan(src, nil, mapChunk, degree)
		//f.Done(func(...interface{}) { reduceSrc <- nil }) //fmt.Println("where send a nil------------") })

		return &chanSource{reduceSrc}, keepOrder, nil
	})
}

func getDistinct(distinctFunc func(interface{}) interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {
		mapChunk := func(c *chunk) (r *chunk) {
			r = &chunk{getKeyValues(c, distinctFunc, nil), c.order}
			return
		}

		//reduceSrc := make(chan *chunk)
		////get all values and keys
		//var f *promise.Future
		//switch s := src.(type) {
		//case *listSource:
		//	f = parallelMapListToChan(s, reduceSrc, mapChunk, degree)
		//case *chanSource:
		//	f = parallelMapChan(s, reduceSrc, mapChunk, degree)
		//}
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, degree)

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
		mapChunk := func(c *chunk) (r *chunk) {
			r = &chunk{getKeyValues(c, groupFunc, nil), c.order}
			return
		}

		//reduceSrc := make(chan *chunk)
		////get all values and keys
		//var f *promise.Future
		//switch s := src.(type) {
		//case *listSource:
		//	f = parallelMapListToChan(s, reduceSrc, mapChunk, degree)
		//case *chanSource:
		//	f = parallelMapChan(s, reduceSrc, mapChunk, degree)
		//}
		f, reduceSrc := parallelMapToChan(src, nil, mapChunk, degree)

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
			outerKeySelectorFuture := parallelMapListToList(s, mapChunk, degree)
			dst, e = getFutureResult(outerKeySelectorFuture, func(results []interface{}) dataSource {
				result := expandChunks(results, false)
				return &listSource{result}
			})
			return
		case *chanSource:
			//out := make(chan *chunk)
			_, out := parallelMapChanToChan(s, nil, mapChunk, degree)
			dst, e = &chanSource{out}, nil
			return
		}

		return nil, keep, nil
	})
}

func getUnion(source2 interface{}, degree int) stepAction {
	return stepAction(func(src dataSource, keepOrder bool) (dataSource, bool, error) {
		reduceSrc := make(chan *chunk)
		mapChunk := func(c *chunk) (r *chunk) {
			r = &chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
			return
		}

		////get all values and keys
		//var f *promise.Future
		//switch s := src.(type) {
		//case *listSource:
		//	f = parallelMapListToChan(s, reduceSrc, mapChunk, degree)
		//case *chanSource:
		//	f = parallelMapChan(s, reduceSrc, mapChunk, degree)
		//}
		f1, reduceSrc := parallelMapToChan(src, reduceSrc, mapChunk, degree)

		//dataSource2 := From(source2).data
		//var f1 *promise.Future
		//switch s := dataSource2.(type) {
		//case *listSource:
		//	f1 = parallelMapListToChan(s, reduceSrc, mapChunk, degree)
		//case *chanSource:
		//	f1 = parallelMapChan(s, reduceSrc, mapChunk, degree)
		//}
		f2, reduceSrc := parallelMapToChan(From(source2).data, reduceSrc, mapChunk, degree)

		reduceFuture := promise.WhenAll(f1, f2)

		//get distinct values
		distKvs := make(map[uint64]int)
		chunks := make([]interface{}, 0, 2*degree)
		reduceChan(reduceFuture.GetChan(), reduceSrc, func(c *chunk) {
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
			mapChunk := func(c *chunk) (r *chunk) {
				r = &chunk{getKeyValues(c, func(v interface{}) interface{} { return v }, nil), c.order}
				return
			}
			//reduceSrc := make(chan *chunk)
			////get all values and keys
			//var f *promise.Future
			//switch s := src.(type) {
			//case *listSource:
			//	f = parallelMapListToChan(s, reduceSrc, mapChunk, degree)
			//case *chanSource:
			//	f = parallelMapChan(s, reduceSrc, mapChunk, degree)
			//}
			f, reduceSrc := parallelMapToChan(src, nil, mapChunk, degree)
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
func parallelMapToChan(src dataSource, reduceSrcChan chan *chunk, mapChunk func(c *chunk) (r *chunk), degree int) (f *promise.Future, ch chan *chunk) {
	//get all values and keys
	//var f *promise.Future
	switch s := src.(type) {
	case *listSource:
		return parallelMapListToChan(s, reduceSrcChan, mapChunk, degree)
	case *chanSource:
		return parallelMapChanToChan(s, reduceSrcChan, mapChunk, degree)
	default:
		panic(ErrUnsupportSource)
	}
	//return f, reduceSrcChan

}

func parallelMapChanToChan(src *chanSource, out chan *chunk, task func(*chunk) *chunk, degree int) (*promise.Future, chan *chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *chunk)
		createOutChan = true
	}

	itr := src.Itr()
	fs := make([]*promise.Future, degree, degree)
	for i := 0; i < degree; i++ {
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

func parallelMapListToChan(src dataSource, out chan *chunk, task func(*chunk) *chunk, degree int) (*promise.Future, chan *chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *chunk)
		createOutChan = true
	}

	f := parallelMapList(src, func(c *chunk) func() []interface{} {
		return func() []interface{} {
			r := task(c)
			if out != nil {
				out <- r
			}
			return nil
		}
	}, degree)
	if createOutChan {
		addCloseChanCallback(f, out)
	}
	return f, out
}

func addCloseChanCallback(f *promise.Future, out chan *chunk) {
	f.Always(func(results ...interface{}) {
		if out != nil {
			if cap(out) == 0 {
				close(out)
			} else {
				out <- nil
			}
		}
	})
}

func parallelMapListToList(src dataSource, task func(*chunk) *chunk, degree int) *promise.Future {
	return parallelMapList(src, func(c *chunk) func() []interface{} {
		return func() []interface{} {
			r := task(c)
			return []interface{}{r, true}
		}
	}, degree)
}

func parallelMapList(src dataSource, getAction func(*chunk) func() []interface{}, degree int) *promise.Future {
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

		f := promise.Start(getAction(c))
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
		case v, ok := <-src:
			if ok {
				reduce(v)
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
