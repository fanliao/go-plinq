package plinq

import (
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
)

var _ = fmt.Println //for debugger

//the struct and functions of each operation-------------------------------------------------------------------------\

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
type stepAction func(DataSource, *ParallelOption, bool) (DataSource, bool, error) //, *promise.Future, bool, error)

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
	if this.typ == ACT_REVERSE || this.Typ() == ACT_UNION || this.Typ() == ACT_INTERSECT || this.Typ() == ACT_EXCEPT {
		option.ChunkSize = DefaultLargeChunkSize
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
func getSelect(selector OneArgsFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		keep = option.KeepOrder
		mapChunk := getChunkOprFunc(mapSliceToSelf, selector)
		if first {
			mapChunk = getChunkOprFunc(mapSlice, selector)
			//mapChunk = getMapChunkFunc(selectFunc)
		}

		dst, e = parallelMap(src, nil, mapChunk, option)
		//dst = &chanSource{chunkChan: out, future: fu}

		return
	})

}

// Get the action function for select operation
func getSelectMany(manySelector func(interface{}) []interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		keep = option.KeepOrder
		mapChunk := func(c *Chunk) *Chunk {
			results := mapSliceToMany(c.Data, manySelector)
			return &Chunk{NewSlicer(results), c.Order, c.StartIndex}
		}

		////try to use sequentail if the size of the data is less than size of chunk
		//if list, err, handled := trySequentialMap(src, option, mapChunk); handled {
		//	return list, option.KeepOrder, err
		//}

		dst, e = parallelMap(src, nil, mapChunk, option)

		return
	})

}

// Get the action function for where operation
func getWhere(predicate PredicateFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		mapChunk := getChunkOprFunc(filterSlice, predicate)

		//always use channel mode in Where operation
		dst, e = parallelMap(src, nil, mapChunk, option)
		keep = option.KeepOrder

		return
	})
}

// Get the action function for OrderBy operation
func getOrder(comparator CompareFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
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
				return comparator(this, that) == -1
			})
			return newDataSource(sorteds), true, nil
		case *chanSource:
			//AVL tree sort
			avl := newAvlTree(comparator)
			cs := parallelMapChanToChan(s, nil,
				getChunkOprFunc(forEachSlicer, func(i int, v interface{}) {
					avl.Insert(v)
				}), option)

			dst, e = getFutureResult(cs.Future(), func(r []interface{}) DataSource {
				return newDataSource(avl.ToSlice())
			})
			keep = true
			return
		}
		panic(ErrUnsupportSource)
	})
}

// Get the action function for DistinctBy operation
func getDistinct(selector OneArgsFunc) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		var useDefHash uint32 = 0
		mapChunk := getMapChunkToKVChunkFunc(&useDefHash, selector)

		//map the element to a keyValue that key is hash value and value is element
		cs := parallelMapToChan(src, nil, mapChunk, option)

		dst = reduceDistValues(cs, option)
		keep = option.KeepOrder
		return
	})
}

// Get the action function for GroupBy operation
// note the groupby cannot keep order because the map cannot keep order
func getGroupBy(selector OneArgsFunc, hashAsKey bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {

		var useDefHash uint32 = 0
		mapChunk := getMapChunkToKVChunkFunc(&useDefHash, selector)

		//map the element to a keyValue that key is group key and value is element
		cs := parallelMapToChan(src, nil, mapChunk, option)

		groups := make(map[interface{}]interface{})
		group := func(v interface{}) {
			kv := v.(*hKeyValue)
			k := kv.keyHash

			if v, ok := groups[k]; !ok {
				groups[k] = []interface{}{kv.value}
			} else {
				list := v.([]interface{})
				groups[k] = appendToSlice(list, kv.value)
			}
			//fmt.Println("groupKVs, ", k, v, groupKVs[k])
		}

		//reduce the keyValue map to get grouped slice
		//get key with group values values
		errs := reduceChan(cs, getChunkOprFunc(forEachSlicer, func(i int, v interface{}) {
			group(v)
		}))

		//fmt.Println("groupKVs, return ===", groupKVs)
		if errs == nil {
			return newDataSource(groups), option.KeepOrder, nil
		} else {
			return nil, option.KeepOrder, NewAggregateError("Group error", errs)
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
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		keep = option.KeepOrder
		innerKVtask := promise.Start(func() (interface{}, error) {
			if innerKVsDs, err := From(inner).hGroupBy(innerKeySelector).execute(); err == nil {
				return innerKVsDs.(*listSource).data.(*mapSlicer).data, nil
			} else {
				return nil, err
			}
		})

		var useDefHash uint32 = 0
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

		dst = parallelMapToChan(src, nil, mapChunk, option)
		return
	})
}

func canSequentialSet(src DataSource, src2 DataSource) bool {
	if src.Typ() == SOURCE_LIST && src2.Typ() == SOURCE_LIST {
		if src.ToSlice(false).Len() <= DefaultLargeChunkSize && src2.ToSlice(false).Len() <= DefaultLargeChunkSize {
			return true
		}
	}
	return false

}

// Get the action function for Union operation
// note the union cannot keep order because the map cannot keep order
func getUnion(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		src2 := From(source2).data
		if canSequentialSet(src, src2) {
			return sequentialUnion(src, src2, option, first)
		}
		reduceSrcChan := make(chan *Chunk, 2)
		var (
			useDefHash uint32
		)

		mapChunk := getMapChunkToKVChunkFunc(&useDefHash, nil)

		//map the elements of source and source2 to the a KeyValue slice
		//includes the hash value and the original element
		cs1 := parallelMapToChan(src, reduceSrcChan, mapChunk, option)
		cs2 := parallelMapToChan(src2, reduceSrcChan, mapChunk, option)

		mapFuture := promise.WhenAll(cs1.Future(), cs2.Future())

		outCs := &chanSource{chunkChan: reduceSrcChan, future: mapFuture}
		outCs.addCallbackToCloseChan()

		reduceCs := reduceDistValues(outCs, option)
		return reduceCs, option.KeepOrder, nil
	})
}

// Get the action function for Union operation
// note the union cannot keep order because the map cannot keep order
func sequentialUnion(src DataSource, src2 DataSource, option *ParallelOption, first bool) (ds DataSource, keep bool, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = newErrorWithStacks(err)
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
	distChunkValues(c1, distKVs, &result)
	distChunkValues(c2, distKVs, &result)
	//fmt.Println("\n-----result=", result)

	return &listSource{NewSlicer(result)}, option.KeepOrder, nil
}

// Get the action function for Concat operation
func getConcat(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		//TODO: if the source is channel source, should use channel mode
		slice1 := src.ToSlice(option.KeepOrder).ToInterfaces()
		if slice2, err2 := From(source2).SetKeepOrder(option.KeepOrder).Results(); err2 == nil {
			result := make([]interface{}, len(slice1)+len(slice2))
			_ = copy(result[0:len(slice1)], slice1)
			_ = copy(result[len(slice1):len(slice1)+len(slice2)], slice2)
			return newDataSource(result), option.KeepOrder, nil
		} else {
			return nil, option.KeepOrder, err2
		}

	})
}

// Get the action function for intersect operation
// note the intersect cannot keep order because the map cannot keep order
func getIntersect(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		dst, e = filterSet(src, source2, false, option)
		keep = option.KeepOrder
		return

	})
}

// Get the action function for Except operation
// note the except cannot keep order because the map cannot keep order
func getExcept(source2 interface{}) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		//result, f, err = filterSetA(src, source2, true, option)
		dst, e = filterSet(src, source2, true, option)
		keep = option.KeepOrder
		return
	})
}

// Get the action function for Reverse operation
func getReverse() stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
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
				wholeSlice[size-1-j], wholeSlice[j] = c.Data.Index(i), slicer.Index(size-1-j)
			})
			return c
		}

		reverseSrc := &listSource{NewSlicer(srcSlice)} //newDataSource(srcSlice)
		if size <= 1000 {
			forEachSlicer(srcSlice, func(i int, v interface{}) {
				j := i
				wholeSlice[size-1-j], wholeSlice[j] = srcSlice.Index(i), slicer.Index(size-1-j)
			})
			dst = newDataSource(wholeSlice)
			return
		}

		f := parallelMapListToList(reverseSrc, func(c *Chunk) *Chunk {
			return mapChunk(c)
		}, option)
		_, e = f.Get()
		dst = newDataSource(wholeSlice)
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
func getElementAt(src DataSource, i int, option *ParallelOption) (element interface{}, found bool, e error) {
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
func getFirstBy(src DataSource, predicate PredicateFunc, option *ParallelOption) (element interface{}, found bool, e error) {
	return getFirstElement(src, foundMatchFunc(predicate, true), false, option)
}

// Get the action function for LastBy operation
// 根据条件查找最后一个符合的元素
func getLastBy(src DataSource, predicate PredicateFunc, option *ParallelOption) (element interface{}, found bool, e error) {
	return getLastElement(src, foundMatchFunc(predicate, false), option)
}

// Get the action function for Aggregate operation
func getAggregate(src DataSource, aggregateFuncs []*AggregateOperation, option *ParallelOption) (result []interface{}, e error) {
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
	cs := parallelMapToChan(src, nil, mapChunk, option)

	//reduce the keyValue map to get grouped slice
	//get key with group values values
	first := true
	reduce := func(c *Chunk) {
		if first {
			for i := 0; i < len(rs); i++ {
				rs[i] = aggregateFuncs[i].Seed
			}
		}
		first = false

		for i := 0; i < len(rs); i++ {
			rs[i] = aggregateFuncs[i].ReduceAction(c.Data.Index(i), rs[i])
		}
	}

	avl := newChunkAvlTree()
	if errs := reduceChan(cs, func(c *Chunk) (r *Chunk) {
		if !keep {
			reduce(c)
		} else {
			avl.Insert(c)
		}
		return
	}); errs != nil {
		e = getError(errs)
	}

	if keep {
		cs := avl.ToSlice()
		for _, v := range cs {
			c := v.(*Chunk)
			reduce(c)
		}
	}

	if first {
		return rs, newErrorWithStacks(errors.New("cannot aggregate an empty slice"))
	} else {
		return rs, e
	}

}

func getSkipTake(findMatch func(*Chunk, promise.Canceller) (int, bool), isTake bool, useIndex bool) stepAction {
	return stepAction(func(src DataSource, option *ParallelOption, first bool) (dst DataSource, keep bool, e error) {
		switch s := src.(type) {
		case *listSource:
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
				return newDataSource(s.data.Slice(0, i)), option.KeepOrder, e
			} else {
				return newDataSource(s.data.Slice(i, s.data.Len())), option.KeepOrder, e
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
				matchedList := newChunkMatchResultList(beforeMatchAct, afterMatchAct, beMatchAct, useIndex)
				return forEachChanByOrder(s, srcChan, func(c *Chunk, foundFirstMatch *bool) bool {
					if !*foundFirstMatch {
						//检查块是否存在匹配的数据，按Index计算的总是返回false，因为必须要等前面所有的块已经排好序后才能得到正确的索引
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

			outCs := &chanSource{chunkChan: out, future: f}
			outCs.addCallbackToCloseChan()

			return outCs, option.KeepOrder, nil
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
					chunkResult := getChunkMatchResult(c, findMatch, useIndex)

					*foundFirstMatch = matchedList.handleChunk(chunkResult)
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

// Get the action function for Any operation
func getAny(src DataSource, predicate PredicateFunc, option *ParallelOption) (result interface{}, allMatched bool, err error) {
	getMapChunk := func(c *Chunk) func(promise.Canceller) (interface{}, error) {
		return func(canceller promise.Canceller) (foundNoMatched interface{}, e error) {
			size := c.Data.Len()
			for i := 0; i < size; i++ {
				if canceller.IsCancellationRequested() {
					break
				}
				if predicate(c.Data.Index(i)) {
					return true, nil
				}
			}
			//fmt.Println("getAny return", false)
			return false, nil
		}
	}

	predicateResult := func(v interface{}) bool {
		if r, ok := v.(bool); r && ok {
			return true
		} else {
			return false
		}
	}
	var f *promise.Future
	switch s := src.(type) {
	case *listSource:
		f = parallelMapListForAnyTrue(s, getMapChunk, predicateResult, option)
	case *chanSource:
		f = parallelMapChanForAnyTrue(s, getMapChunk, predicateResult, option)
	}
	r, e := f.Get()
	if val, ok := r.(bool); ok {
		return val, val, e
	} else {
		return false, false, e
	}

	return
}

func getChunkMatchResult(c *Chunk, findMatch func(c *Chunk, canceller promise.Canceller) (r int, found bool), useIndex bool) (r *chunkMatchResult) {
	r = &chunkMatchResult{chunk: c}
	if !useIndex {
		r.matchIndex, r.matched = findMatch(c, nil)
	}
	return
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
		return
	}

	if isNil(err) {
		err = nil
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

func filterSet(src DataSource, source2 interface{}, isExcept bool, option *ParallelOption) (DataSource, error) {
	src2 := newDataSource(source2)

	if canSequentialSet(src, src2) {
		return filterSetWithSeq(src, src2, isExcept, option)
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

func filterSetByChan(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, error) {
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
	cs1 := parallelMapToChan(src, nil, mapChunk, option)
	cs2 := parallelMapToChan(src2, nil, mapChunk2, option)

	distKVs1 := make(map[interface{}]bool, 100)
	distKVs2 := make(map[interface{}]bool, 100)
	resultKVs := make(map[interface{}]interface{}, 100)

	reduceSrcChan1, reduceSrcChan2 := cs1.chunkChan, cs2.chunkChan
	f1, f2 := cs1.Future(), cs2.Future()
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
		return nil, err1
	} else if _, err2 := f2.Get(); err2 != nil {
		return nil, err2
	}

	//获取结果集
	i, results := 0, make([]interface{}, len(resultKVs))
	for _, v := range resultKVs {
		results[i] = v
		i++
	}
	return newListSource(results), nil

}

func filterSetWithSeq(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (DataSource, error) {
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
	mapDist := getChunkOprFunc(forEachSlicer, func(i int, v interface{}) {
		k, val := toKeyValue(v)

		if addDistVal(k, val, distKVs, resultKVs, isExcept) {
			results[count] = val
			count++
		}
	})
	_ = mapDist(c1)
	return &listSource{NewSlicer(results[0:count])}, nil
}

func filterSetByList(src DataSource, src2 DataSource, isExcept bool, option *ParallelOption) (cs DataSource, e error) {
	ds2 := src2
	mapChunk, mapChunk2 := getFilterSetMapFuncs()
	ds1, e := parallelMap(src, nil, mapChunk, option)
	f2 := parallelMapListToList(ds2, mapChunk2, option)

	var distKVs map[interface{}]bool
	resultKVs := make(map[interface{}]interface{}, 100)
	mapDist := func(c *Chunk) *Chunk {
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
	cs = parallelMapToChan(ds1, nil, mapDist, option)
	return
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
			if i == end {
				break
			}
			v := c.Data.Index(i)
			if canceller != nil && canceller.IsCancellationRequested() {
				canceller.SetCancelled()
				break
			}
			if predicate(v) {
				r, found = i, true
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

//连续分割，将slice分割为几个连续的块，块数=option.Degree
func splitContinuous(src DataSource, action func(*Chunk), option *ParallelOption) {
	data := src.ToSlice(false)
	size := data.Len()
	lenOfData, size := size, ceilChunkSize(size, option.Degree)

	if size < option.ChunkSize {
		size = option.ChunkSize
	}

	for i := 0; i < option.Degree && i*size < lenOfData; i++ {
		end := (i + 1) * size
		if end >= lenOfData {
			end = lenOfData
		}
		c := &Chunk{data.Slice(i*size, end), i, i * size} //, end}
		action(c)
	}

	return
}

//条带式分割，将slice分割为固定大小的Chunk，然后发送到Channel并返回
//如果数据量==0，则返回nil
func splitToChunkChan(src DataSource, option *ParallelOption) (ch chan *Chunk) {
	data := src.ToSlice(false)
	lenOfData := data.Len()

	size := option.ChunkSize
	if size < lenOfData/(numCPU*5) {
		size = lenOfData / (numCPU * 5)
	}
	ch = make(chan *Chunk, option.Degree)
	if lenOfData == 0 {
		ch = nil
	}

	go func() {
		for i := 0; i*size < lenOfData; i++ {
			end := (i + 1) * size
			if end >= lenOfData {
				end = lenOfData
			}
			c := &Chunk{data.Slice(i*size, end), i, i * size} //, end}
			//fmt.Println("\n parallelMapListToChan, send", i, size, data.Slice(i*size, end).ToInterfaces())
			sendChunk(ch, c)
		}
		func() {
			defer func() { _ = recover() }()
			close(ch)
		}()
	}()
	return
}

//对data source进行并行的Map处理，小数据量下返回listSource，否则都返回chanSource
func parallelMap(src DataSource, reduceSrcChan chan *Chunk, mapChunk func(c *Chunk) (r *Chunk), option *ParallelOption) (dst DataSource, err error) { //f *promise.Future, ch chan *Chunk) {
	//get all values and keys
	switch s := src.(type) {
	case *listSource:
		if s.data.Len() <= option.ChunkSize*option.Degree {
			f := parallelMapListToList(s, mapChunk, option)
			dst, err = getFutureResult(f, func(r []interface{}) DataSource {
				return newDataSource(expandChunks(r, option.KeepOrder))
			})
			return
		} else {
			return parallelMapListToChan(s, reduceSrcChan, mapChunk, option), nil
		}
	case *chanSource:
		return parallelMapChanToChan(s, reduceSrcChan, mapChunk, option), nil //, startOrders...)
	default:
		panic(ErrUnsupportSource)
	}
}

//对data source进行并行的Map处理，返回chanSource
func parallelMapToChan(src DataSource, reduceSrcChan chan *Chunk, mapChunk func(c *Chunk) (r *Chunk), option *ParallelOption) (cs *chanSource) { //f *promise.Future, ch chan *Chunk) {
	//get all values and keys
	switch s := src.(type) {
	case *listSource:
		return parallelMapListToChan(s, reduceSrcChan, mapChunk, option)
	case *chanSource:
		return parallelMapChanToChan(s, reduceSrcChan, mapChunk, option) //, startOrders...)
	default:
		panic(ErrUnsupportSource)
	}
}

func forEachChan(src *chanSource, srcChan chan *Chunk, action func(*Chunk) (result interface{}, beEnded bool, err error)) (r interface{}, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = newErrorWithStacks(err)
			//fmt.Println("parallelMapChanToChan, get error===:", cc, e)
		}
	}()
	for c := range srcChan {
		//fmt.Println("foreach chan,", c)
		if !isNil(c) {
			r, beEnded, err := action(c)
			if err != nil {
				return nil, err
			} else if beEnded {
				return r, err
			}
		} else if cap(srcChan) > 0 {
			src.Close()
			break
		}
	}
	if src.Future() != nil {
		if _, err := src.Future().Get(); err != nil {
			return nil, err
		}
	}
	return

}

func parallelMapChanToChan(src *chanSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption) (cs *chanSource) { //(*promise.Future, chan *Chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.Degree)
		createOutChan = true
	}

	srcChan := src.ChunkChan(option.ChunkSize)

	fs := make([]*promise.Future, option.Degree)
	for i := 0; i < option.Degree; i++ {
		f := promise.Start(func() (r interface{}, e error) {
			r, e = forEachChan(src, srcChan, func(c *Chunk) (result interface{}, beEnded bool, err error) {
				d := task(c)
				if out != nil && d != nil {
					//fmt.Println("\nparallelMapChanToChan, from=", c.Order, c.Data.ToInterfaces(), "to=", d.Order, d.Data.ToInterfaces())
					sendChunk(out, d)
				}
				return nil, false, nil
			})
			return
		})
		fs[i] = f
	}
	f := promise.WhenAllFuture(fs...).Fail(func(err interface{}) { src.Close() })

	outCs := &chanSource{chunkChan: out, future: f}
	if createOutChan {
		outCs.addCallbackToCloseChan()
	}
	return outCs
}

func parallelMapListToChan(src DataSource, out chan *Chunk, task func(*Chunk) *Chunk, option *ParallelOption) (cs *chanSource) { //(*promise.Future, chan *Chunk) {
	var createOutChan bool
	if out == nil {
		out = make(chan *Chunk, option.Degree)
		createOutChan = true
	}

	var f *promise.Future

	chunkChan := splitToChunkChan(src, option)
	if chunkChan == nil {
		f = promise.Wrap([]interface{}{})
		cs = &chanSource{chunkChan: out, future: f}
	} else {
		src := &chanSource{chunkChan: chunkChan}
		cs = parallelMapChanToChan(src, out, task, option)
	}
	if createOutChan {
		cs.addCallbackToCloseChan()
	}
	return cs

}

func parallelMapListToList(src DataSource, task func(*Chunk) *Chunk, option *ParallelOption) (f *promise.Future) {

	i, fs := 0, make([]interface{}, option.Degree)
	splitContinuous(src, func(c *Chunk) {
		fs[i] = func() (interface{}, error) {
			r := task(c)
			return r, nil
		}
		i++
	}, option)
	f = promise.WaitAll(fs[0:i]...)
	return
}

//并行循环slice，如果任一任务的返回值满足要求，则返回该值，否则返回false，
//如果没有匹配的返回值并且有任务失败则reject
func parallelMapListForAnyTrue(src DataSource, getAction func(*Chunk) func(promise.Canceller) (interface{}, error), predicate PredicateFunc, option *ParallelOption) (f *promise.Future) {
	i, fs := 0, make([]*promise.Future, option.Degree)
	splitContinuous(src, func(c *Chunk) {
		fs[i] = promise.Start(getAction(c))
		i++
	}, option)
	f = promise.WhenAnyTrue(predicate, fs[0:i]...)
	return
}

//并行循环channel，如果任一任务的返回值满足要求，则返回该值，否则返回false，
//如果没有匹配的返回值并且有任务失败则reject
func parallelMapChanForAnyTrue(src *chanSource,
	getAction func(*Chunk) func(promise.Canceller) (interface{}, error),
	predicate PredicateFunc, option *ParallelOption) *promise.Future {

	srcChan := src.ChunkChan(option.ChunkSize)

	fs := make([]*promise.Future, option.Degree)
	for i := 0; i < option.Degree; i++ {
		//k := i
		f := promise.Start(func(canceller promise.Canceller) (r interface{}, e error) {
			r, e = forEachChan(src, srcChan, func(c *Chunk) (result interface{}, beEnded bool, err error) {
				if result, err = getAction(c)(canceller); err == nil {
					if predicate(result) {
						r = result
						beEnded = true
						return
					}
				} else {
					return
				}
				return nil, false, nil
			})
			return
		})
		fs[i] = f
	}
	f := promise.WhenAnyTrue(predicate, fs...).Fail(func(err interface{}) { src.Close() })

	return f
}

func parallelMatchListByDirection(src DataSource, getAction func(*Chunk, promise.Canceller) (int, bool), option *ParallelOption, isFarword bool) (f *promise.Future) {
	count, fs := 0, make([]*promise.Future, option.Degree)
	splitContinuous(src, func(c *Chunk) {
		fs[count] = promise.Start(func(canceller promise.Canceller) (interface{}, error) {
			r, found := getAction(c, canceller)
			if found && r != -1 {
				r = r + c.StartIndex
			}
			return r, nil
		})
		count++
	}, option)

	//根据查找的顺序来检查各任务查找的结果
	f = promise.Start(func() (interface{}, error) {
		var idx interface{}
		rs, errs := make([]interface{}, count), make([]error, count)
		allOk, hasOk := true, false
		start, end := 0, count

		if !isFarword {
			start, end = count-1, -1
		}

		forFutures := func(fs []*promise.Future, start int, action func(i int) bool) {
			i := start
			for {
				if action(i) {
					break
				}

				if isFarword {
					i++
				} else {
					i--
				}
				//所有Future都判断完毕
				if i == end {
					break
				}
			}
		}

		//for {
		forFutures(fs, start, func(i int) (beEnd bool) {
			f := fs[i]
			//根据查找顺序，如果有Future失败或者找到了数据，则取消后面的Future
			if !allOk || hasOk {
				forFutures(fs, i, func(i int) (beEnd bool) {
					if c := fs[i].Canceller(); c != nil {
						fs[i].RequestCancel()
					}
					return
				})
				return true
			}

			//判断每个Future的结果
			rs[i], errs[i] = f.Get()
			if errs[i] != nil {
				allOk = false
			} else if rs[i].(int) != -1 {
				hasOk = true
			}
			idx = rs[i]
			return
		})

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
		}
	}()
	if useSingle := singleDegree(src, option); useSingle || ifMustSequential(aggregateFuncs) {
		if len(aggregateFuncs) == 1 && aggregateFuncs[0] == countAggOpr {
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
func reduceChan(cs *chanSource, reduce func(*Chunk) *Chunk) interface{} {
	for v := range cs.chunkChan {
		if !isNil(v) {
			reduce(v)
		} else if cap(cs.chunkChan) > 0 {
			close(cs.chunkChan)
			break
		}
	}

	f := cs.Future()
	if f != nil {
		if _, err := f.Get(); err != nil {
			return err
		}
	}
	return nil
}

//the functions reduces the paralleliam map result----------------------------------------------------------
func reduceDistValues(src *chanSource, option *ParallelOption) (cs *chanSource) { //(f *promise.Future, out chan *Chunk) {
	//get distinct values
	distKVs := make(map[interface{}]int)
	option.Degree = 1
	//fmt.Println("reduceDistValues")
	return parallelMapToChan(src, nil, func(c *Chunk) *Chunk {

		r := distChunkValues(c, distKVs, nil)
		//fmt.Println("\n distChunkValues", c, r.Data)
		return r
	}, option)
}

//util functions-----------------------------------------------------------------

func getFutureResult(f *promise.Future, dataSourceFunc func([]interface{}) DataSource) (DataSource, error) {
	if results, err := f.Get(); err != nil {
		//todo
		return nil, err
	} else {
		rs := results.([]interface{})
		if rs != nil && len(rs) == 1 {
			if c, ok := rs[0].(*Chunk); ok {
				return &listSource{c.Data}, nil
			}
		}
		return dataSourceFunc(rs), nil
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
		k := keyFunc(v)
		if hashAsKey {
			return &hKeyValue{hash64(k), k, v}
		} else {
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
