package main

import (
	"fmt"
	"strconv"
	"time"
)

//queryable-------------------------------------------------------------------------------------

//type queryable struct {
//	source  chan interface{}
//	actions []func(chan interface{}) chan interface{}
//	datas   []interface{}
//}

//func (this queryable) Where(sure func(interface{}) bool) queryable {
//	action := func(src chan interface{}) chan interface{} {
//		return forChan(src, func(v interface{}, dst chan interface{}) {
//			if sure(v) {
//				dst <- v
//			}
//		})
//	}
//	this.actions = append(this.actions, action)
//	return this
//}

//func (this queryable) Select(f func(interface{}) interface{}) queryable {
//	action := func(src chan interface{}) chan interface{} {
//		return forChan(src, func(v interface{}, dst chan interface{}) {
//			dst <- f(v)
//		})

//	}
//	this.actions = append(this.actions, action)
//	return this
//}

//func (this queryable) Get() chan interface{} {
//	src := this.source
//	datas := make([]interface{}, 0, 10000)
//	startChan := make(chan chunk, 10000)
//	endChan := make(chan int)
//	go func() {
//		count, start, end := 0, 0, 0
//		for v := range src {
//			datas = append(datas, v)
//			count++
//			if count == BATCH_SIZE {
//				end = len(datas) - 1
//				startChan <- chunk{start, end}
//				fmt.Println("send", start, end, datas[start:end+1])
//				count, start = 0, end+1
//			}
//		}
//		endChan <- 1
//		close(startChan)
//	}()
//	<-endChan
//	for _, action := range this.actions {
//		src = action(src)
//	}
//	return src
//}

//func forChan(src chan interface{}, f func(interface{}, chan interface{})) chan interface{} {
//	dst := make(chan interface{}, 1)
//	go func() {
//		for v := range src {
//			f(v, dst)
//		}
//		close(dst)
//	}()
//	return dst
//}

type queryableS struct {
	source  []interface{}
	actions []func([]interface{}) []interface{}
}

func (this queryableS) Where(sure func(interface{}) bool) queryableS {
	action := func(src []interface{}) []interface{} {
		//dst := make([]interface{}, 0, len(this.source))
		//mapSlice(src, func(v interface{}, out *[]interface{}) {
		//	if sure(v) {
		//		*out = append(*out, v)
		//	}
		//}, &dst)
		dst := filterSlice(src, sure)
		return dst
	}
	this.actions = append(this.actions, action)
	return this
}

func (this queryableS) Select(f func(interface{}) interface{}) queryableS {
	action := func(src []interface{}) []interface{} {
		//dst := make([]interface{}, 0, len(this.source))
		//mapSlice(src, func(v interface{}, out *[]interface{}) {
		//	*out = append(*out, f(v))
		//}, &dst)
		dst := mapSlice(src, f, nil)
		return dst
	}
	this.actions = append(this.actions, action)
	return this
}

func (this queryableS) Get() []interface{} {
	data := this.source
	for _, action := range this.actions {
		data = action(data)
	}
	return data
}

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

func main() {
	time.Now()
	count := 20

	arrInts := make([]int, 0, 20)
	src1 := make([]interface{}, 0, 20)
	src2 := make([]interface{}, 0, 20)
	pow1 := make([]interface{}, 0, 20)
	for i := 0; i < count; i++ {
		arrInts = append(arrInts, i)
		src1 = append(src1, i)
		src2 = append(src2, i+count/2)
	}
	for i := count / 4; i < count/2; i++ {
		pow1 = append(pow1, power{i, i * i})
		pow1 = append(pow1, power{i, i * 100})
	}

	var whereFunc = func(v interface{}) bool {
		//var ss []int
		//_ = ss[2]
		i := v.(int)
		return i%2 == 0
	}
	var selectFunc = func(v interface{}) interface{} {
		i := v.(int)
		return "item" + strconv.Itoa(i)
	}
	var groupKeyFunc = func(v interface{}) interface{} {
		return v.(int) / 10
	}

	var joinResultSelector = func(o interface{}, i interface{}) interface{} {
		if i == nil {
			return strconv.Itoa(o.(int))
		} else {
			o1, i1 := o.(int), i.(power)
			return strconv.Itoa(o1) + ";" + strconv.Itoa(i1.p)
		}
	}

	var groupJoinResultSelector = func(o interface{}, is []interface{}) interface{} {
		return KeyValue{o, is}
	}

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
		return q.LeftJoin(pow1,
			func(o interface{}) interface{} { return o },
			func(i interface{}) interface{} { return i.(power).i },
			joinResultSelector)
	})

	//test left group join
	testLinqWithAllSource("LeftGroupJoin opretions", src1, func(q *Queryable) *Queryable {
		return q.LeftGroupJoin(pow1,
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

	size := count / 4
	chunkSrc := make(chan *chunk)
	go func() {
		chunkSrc <- &chunk{src1[0:size], 0}
		chunkSrc <- &chunk{src1[size : 2*size], size}
		chunkSrc <- &chunk{src1[2*size : 3*size], 2 * size}
		chunkSrc <- &chunk{src1[3*size : 4*size], 3 * size}
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

func testLinqWithAllSource(title string, listSrc []interface{}, query func(*Queryable) *Queryable, rsHandlers ...func([]interface{})) {
	testLinqOpr(title, func() ([]interface{}, error) {
		return query(From(listSrc)).Results()
	}, rsHandlers...)
	testLinqOpr("Chan source use "+title, func() ([]interface{}, error) {
		return query(From(getChanSrc(listSrc))).Results()
	}, rsHandlers...)
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
	fmt.Println("hash", data, tHash(data))
}
