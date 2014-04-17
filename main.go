package plinq

import (
	"fmt"
	"strconv"
	"time"
)

type power struct {
	i int
	p int
}

func getChanSrc(src []interface{}) chan interface{} {
	chanSrc := make(chan interface{})
	go func() {
		for _, v := range src {
			chanSrc <- v
		}
		close(chanSrc)
	}()
	return chanSrc
}

func getIntChanSrc(src []int) chan int {
	chanSrc := make(chan int)
	go func() {
		for _, v := range src {
			chanSrc <- v
		}
		close(chanSrc)
	}()
	return chanSrc
}

func TestLinq() {
	time.Now()
	count := 20

	arrInts := make([]int, 0, 20)
	src1 := make([]interface{}, 0, 20)
	src2 := make([]interface{}, 0, 20)
	powers := make([]interface{}, 0, 20)
	for i := 0; i < count; i++ {
		arrInts = append(arrInts, i)
		src1 = append(src1, i)
		src2 = append(src2, i+count/2)
	}
	for i := count / 4; i < count/2; i++ {
		powers = append(powers, power{i, i * i})
		powers = append(powers, power{i, i * 100})
	}

	var whereFunc = func(v interface{}) bool {
		//var ss []int
		//_ = ss[2]
		i := v.(int)
		return i%2 == 0
	}
	_ = whereFunc
	var selectFunc = func(v interface{}) interface{} {
		i := v.(int)
		return "item" + strconv.Itoa(i)
	}
	_ = selectFunc
	var groupKeyFunc = func(v interface{}) interface{} {
		return v.(int) / 10
	}
	_ = groupKeyFunc

	var joinResultSelector = func(o interface{}, i interface{}) interface{} {
		if i == nil {
			return strconv.Itoa(o.(int))
		} else {
			o1, i1 := o.(int), i.(power)
			return strconv.Itoa(o1) + ";" + strconv.Itoa(i1.p)
		}
	}
	_ = joinResultSelector

	var groupJoinResultSelector = func(o interface{}, is []interface{}) interface{} {
		return KeyValue{o, is}
	}
	_ = groupJoinResultSelector

	testLinqOpr("Where opretion", func() ([]interface{}, error) {
		return From(src1).Where(whereFunc).Results()
	})

	//test where and select
	testLinqWithAllSource("Where and Select opretions", src1, func(q *Queryable) *Queryable {
		return q.Where(whereFunc).Select(selectFunc)
	})

	//test where and select with int slice
	dst, _ := From(arrInts).Where(whereFunc).Select(selectFunc).Results()
	fmt.Println("Int slice where select return", dst, "\n")

	dst, _ = From(getIntChanSrc(arrInts)).Where(whereFunc).Select(selectFunc).Results()
	fmt.Println("Int chan where select return", dst, "\n")

	//test group
	testLinqWithAllSource("Group opretions", src1, func(q *Queryable) *Queryable {
		return q.GroupBy(groupKeyFunc)
	}, func(dst []interface{}) {
		fmt.Println()
		for _, o := range dst {
			kv := o.(*KeyValue)
			fmt.Println("group get k=", kv.key, ";v=", kv.value, " ")
		}
	})

	//test left join
	testLinqWithAllSource("LeftJoin opretions", src1, func(q *Queryable) *Queryable {
		return q.LeftJoin(powers,
			func(o interface{}) interface{} { return o },
			func(i interface{}) interface{} { return i.(power).i },
			joinResultSelector)
	})

	//test left group join
	testLinqWithAllSource("LeftGroupJoin opretions", src1, func(q *Queryable) *Queryable {
		return q.LeftGroupJoin(powers,
			func(o interface{}) interface{} { return o },
			func(i interface{}) interface{} { return i.(power).i },
			groupJoinResultSelector)
	})

	//test union
	testLinqWithAllSource("Union opretions", src1, func(q *Queryable) *Queryable {
		return q.Union(src2)
	})

	//test intersect
	testLinqWithAllSource("Intersect opretions", src1, func(q *Queryable) *Queryable {
		return q.Intersect(src2)
	})

	//test except
	testLinqWithAllSource("Except opretions", src1, func(q *Queryable) *Queryable {
		return q.Except(src2)
	})

	//test Concat
	testLinqWithAllSource("Concat opretions", src1, func(q *Queryable) *Queryable {
		return q.Concat(src2)
	})

	//test Reverse
	testLinqWithAllSource("Reverse opretions", src1, func(q *Queryable) *Queryable {
		return q.Reverse()
	})

	//test Aggregate
	testLinqAggWithAllSource("aggregate opretions", src1, func(q *Queryable) (interface{}, error) {
		return q.Sum()
	})

	//test Average
	testLinqAggWithAllSource("average opretions", src1, func(q *Queryable) (interface{}, error) {
		return q.Average()
	})

	//test Max
	testLinqAggWithAllSource("max opretions", src1, func(q *Queryable) (interface{}, error) {
		return q.Max()
	})

	//test Min
	testLinqAggWithAllSource("min opretions", src1, func(q *Queryable) (interface{}, error) {
		return q.Min()
	})

	//test aggregate multiple operation
	testLinqAggWithAllSource("aggregate multiple opretions", src1, func(q *Queryable) (interface{}, error) {
		return q.Aggregate(Sum, Count, Max, Min)
	})

	//TODO: don't support the mixed type in aggregate
	////test aggregate multiple operation
	//testLinqAggWithAllSource("aggregate multiple opretions with mixed type", src1, func(q *Queryable) (interface{}, error) {
	//	return From([]interface{}{1, int8(2), uint(3), float64(4.4)}).Aggregate(Sum, Count, Max, Min)
	//})

	//TODO: don't support the mixed type in aggregate
	//test aggregate multiple operation
	testLinqAggWithAllSource("aggregate multiple opretions with mixed type", src1, func(q *Queryable) (interface{}, error) {
		return From([]interface{}{0, 3, 6, 9}).Aggregate(Sum, Count, Max, Min)
	})
	myAgg := &AggregateOpr{"",
		func(v interface{}, t interface{}) interface{} {
			v1, t1 := v.(power), t.(string)
			return t1 + "|{" + strconv.Itoa(v1.i) + ":" + strconv.Itoa(v1.p) + "}"
		}, func(t1 interface{}, t2 interface{}) interface{} {
			return t1.(string) + t2.(string)
		}}
	//test customized aggregate operation
	testLinqAggWithAllSource("customized aggregate opretions", powers, func(q *Queryable) (interface{}, error) {
		return q.Aggregate(myAgg)
	})

	fmt.Print("distinctKvs return:")
	concats, _ := From(src1).Concat(src2).Results()
	kvs, e := distinctKVs(concats, &ParallelOption{numCPU, DEFAULTCHUNKSIZE, false})
	if e == nil {
		for _, v := range kvs {
			fmt.Print(v, " ")
		}
		fmt.Println(", len=", len(kvs), "\n")
	} else {
		fmt.Println(e.Error(), "\n")
	}

	size := count / 4
	chunkSrc := make(chan *Chunk)
	go func() {
		chunkSrc <- &Chunk{src1[0:size], 0}
		chunkSrc <- &Chunk{src1[size : 2*size], size}
		chunkSrc <- &Chunk{src1[2*size : 3*size], 2 * size}
		chunkSrc <- &Chunk{src1[3*size : 4*size], 3 * size}
		chunkSrc <- nil
		fmt.Println("close src------------------", chunkSrc)
	}()
	dst, err := From(chunkSrc).Where(whereFunc).Select(selectFunc).KeepOrder(true).Results()
	if err == nil {
		fmt.Println("chunkchansource where select return", dst)
		fmt.Println()
	} else {
		fmt.Println("chunkchansource where select get error:", err)
		fmt.Println()
	}

}

func testLinqOpr(title string, linqFunc func() ([]interface{}, error), rsHandlers ...func([]interface{})) {
	fmt.Print(title, " ")
	var rsHanlder func([]interface{})
	if rsHandlers != nil && len(rsHandlers) > 0 {
		rsHanlder = rsHandlers[0]
	} else {
		rsHanlder = func(dst []interface{}) { fmt.Print(dst) }
	}
	if dst, err := linqFunc(); err == nil {
		fmt.Print("return:")
		rsHanlder(dst)
		fmt.Println("\n")
	} else {
		fmt.Println("get error:\n", err, "\n")
	}
}

func testLinqAgg(title string, aggFunc func() (interface{}, error)) {
	fmt.Print(title, " ")
	if dst, err := aggFunc(); err == nil {
		fmt.Printf("return:%v", dst)
		fmt.Println("\n")
	} else {
		fmt.Println("get error:\n", err, "\n")
	}
}

func testLinqWithAllSource(title string, listSrc []interface{}, query func(*Queryable) *Queryable, rsHandlers ...func([]interface{})) {
	testLinqOpr(title, func() ([]interface{}, error) {
		return query(From(listSrc)).Results()
	}, rsHandlers...)
	testLinqOpr("Chan source use "+title, func() ([]interface{}, error) {
		return query(From(getChanSrc(listSrc))).Results()
	}, rsHandlers...)
}

func testLinqAggWithAllSource(title string, listSrc []interface{}, agg func(*Queryable) (interface{}, error)) {
	testLinqAgg(title, func() (interface{}, error) {
		return agg(From(listSrc))
	})
	testLinqAgg("Chan source use "+title, func() (interface{}, error) {
		return agg(From(getChanSrc(listSrc)))
	})
}

func testAVL() {
	a := []interface{}{3, 2, 1, 4, 5, 6, 7, 10, 9, 8, 7, 6}
	avl := NewAvlTree(func(a interface{}, b interface{}) int {
		a1, b1 := a.(int), b.(int)
		if a1 < b1 {
			return -1
		} else if a1 == b1 {
			return 0
		} else {
			return 1
		}
	})
	for i := 0; i < len(a); i++ {
		avl.Insert(a[i])
	}
	//_ = taller
	//result := make([]interface{}, 0, 10)
	//avlToSlice(tree, &result)
	result := avl.ToSlice()
	fmt.Println("avl result=", result, "count=", avl.count)

}

func testHash() {
	printHash("user1" + strconv.Itoa(10))
	printHash("user" + strconv.Itoa(110))
	printHash("user" + strconv.Itoa(0))
	printHash("user" + strconv.Itoa(0))
	printHash(nil)
	printHash(nil)
	printHash(111.11)
	printHash(111.11)
	printHash([]int{1, 2, 0})
	printHash([]int{1, 2, 0})
	printHash(0)
	printHash(0)
	printHash([]interface{}{1, "user" + strconv.Itoa(2), 0})
	printHash([]interface{}{1, "user" + strconv.Itoa(2), 0})
	slice := []interface{}{5, "user" + strconv.Itoa(5)}
	printHash(slice)
	printHash(slice)
	printHash(power{1, 1})
	printHash(power{1, 1})
}

func printHash(data interface{}) {
	fmt.Println("hash", data, hash64(data))
}
