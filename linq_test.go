package plinq

import (
	"github.com/ahmetalpbalkan/go-linq"
	c "github.com/smartystreets/goconvey/convey"
	//"math"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"testing"
)

const (
	count        int = 22
	rptCount     int = 24
	countForB    int = 10000
	rptCountForB int = 11000
	MAXPROCS     int = 4
)

var (
	arrUserForT    []interface{}       = make([]interface{}, count, count)
	arrRptUserForT []interface{}       = make([]interface{}, rptCount, rptCount)
	arrUser2ForT   []interface{}       = make([]interface{}, count, count)
	arrIntForT     []int               = make([]int, count, count)
	mapForT        map[int]interface{} = make(map[int]interface{}, count)

	arr        []interface{} = make([]interface{}, countForB, countForB)
	arrUser    []interface{} = make([]interface{}, countForB, countForB)
	arrRptUser []interface{} = make([]interface{}, rptCountForB, rptCountForB)
	arrUser2   []interface{} = make([]interface{}, countForB, countForB)

	arrRole []interface{} = make([]interface{}, countForB, countForB)
)

func init() {
	runtime.GOMAXPROCS(MAXPROCS)
	for i := 0; i < count; i++ {
		arrIntForT[i] = i
		arrUserForT[i] = user{i, "user" + strconv.Itoa(i)}
		arrRptUserForT[i] = user{i, "user" + strconv.Itoa(i)}
		arrUser2ForT[i] = user{i + countForB/2, "user" + strconv.Itoa(i+count/2)}
		mapForT[i] = user{i, "user" + strconv.Itoa(i)}
	}
	for i := 0; i < rptCount-count; i++ {
		arrRptUserForT[count+i] = user{i, "user" + strconv.Itoa(count+i)}
	}

	for i := 0; i < countForB; i++ {
		arr[i] = i
		arrUser[i] = user{i, "user" + strconv.Itoa(i)}
		arrRptUser[i] = user{i, "user" + strconv.Itoa(i)}
		arrUser2[i] = user{i + countForB/2, "user" + strconv.Itoa(i+countForB/2)}
	}

	for i := 0; i < rptCountForB-countForB; i++ {
		arrRptUser[countForB+i] = user{i, "user" + strconv.Itoa(countForB+i)}
	}

	for i := 0; i < countForB/2; i++ {
		arrRole[i*2] = role{i, "role" + strconv.Itoa(i)}
		arrRole[i*2+1] = role{i, "role" + strconv.Itoa(i+1)}
	}
}

type user struct {
	id   int
	name string
}
type role struct {
	uid  int
	role string
}

func wherePanic(v interface{}) bool {
	var s []interface{}
	_ = s[2]
	return true
}

func whereInt(v interface{}) bool {
	i := v.(int)
	//time.Sleep(10 * time.Nanosecond)
	return i%2 == 0
}

func whereUser(v interface{}) bool {
	u := v.(user)
	//time.Sleep(10 * time.Nanosecond)
	return u.id%2 == 0
}

func whereMap(v interface{}) bool {
	u := v.(*KeyValue)
	//time.Sleep(10 * time.Nanosecond)
	return u.key.(int)%2 == 0
}

func selectPanic(v interface{}) interface{} {
	var s []interface{}
	_ = s[2]
	u := v.(user)
	return strconv.Itoa(u.id) + "/" + u.name
}

func selectUser(v interface{}) interface{} {
	u := v.(user)
	return strconv.Itoa(u.id) + "/" + u.name
}

func selectInt(v interface{}) interface{} {
	return v.(int) * 10
}

func TestWhere(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(arrIntForT).SetSizeOfChunk(size).Where(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in where function", func() {
			_, err := From(arrIntForT).SetSizeOfChunk(size).Where(wherePanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Filter an empty slice", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Where(wherePanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Filter an int slice", func() {
			rs, err := From(arrIntForT).SetSizeOfChunk(size).Where(whereInt).Results()
			c.So(rs, shouldSlicesResemble, []interface{}{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20})
			c.So(err, c.ShouldBeNil)
		})

		filteredUsers := make([]interface{}, count/2)
		for i := 0; i < count/2; i++ {
			filteredUsers[i] = user{i * 2, "user" + strconv.Itoa(i*2)}
		}
		c.Convey("Filter an interface{} slice", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Where(whereUser).Results()
			c.So(rs, shouldSlicesResemble, filteredUsers)
			c.So(err, c.ShouldBeNil)
		})

		//filteredKVs := make([]interface{}, count/2)
		//for i := 0; i < count/2; i++ {
		//	filteredKVs[i] = &KeyValue{i * 2, user{i * 2, "user" + strconv.Itoa(i*2)}}
		//}
		//c.Convey("Filter a map", t, func() {
		//	rs, err := From(mapForT).Where(whereMap).Results()
		//	c.So(rs, shouldSlicesResemble, filteredKVs)
		//	c.So(err, c.ShouldBeNil)
		//})

		c.Convey("Filter a interface{} channel", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Where(whereUser).Results()
			c.So(rs, shouldSlicesResemble, filteredUsers)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Filter a int channel", func() {
			rs, err := From(getIntChan(arrIntForT)).SetSizeOfChunk(size).Where(whereInt).Results()
			c.So(rs, shouldSlicesResemble, []interface{}{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20})
			c.So(err, c.ShouldBeNil)
		})

	}

	c.Convey("Test Where Sequential", t, func() { test(30) })
	c.Convey("Test Where parallel", t, func() { test(7) })
}

func TestSelect(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(arrIntForT).SetSizeOfChunk(size).Select(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in select function", func() {
			_, err := From(arrIntForT).SetSizeOfChunk(size).Select(selectPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Select an empty slice", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Select(selectPanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		newInts := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newInts[i] = i * 10
		}
		c.Convey("select an int slice", func() {
			rs, err := From(arrIntForT).SetSizeOfChunk(size).Select(selectInt).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		newUsers := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		c.Convey("Select an interface{} slice", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Select(selectUser).Results()
			c.So(rs, shouldSlicesResemble, newUsers)
			c.So(err, c.ShouldBeNil)
		})

		//filteredKVs := make([]interface{}, count/2)
		//for i := 0; i < count/2; i++ {
		//	filteredKVs[i] = &KeyValue{i * 2, user{i * 2, "user" + strconv.Itoa(i*2)}}
		//}
		//c.Convey("Filter a map", t, func() {
		//	rs, err := From(mapForT).Where(whereMap).Results()
		//	c.So(rs, shouldSlicesResemble, filteredKVs)
		//	c.So(err, c.ShouldBeNil)
		//})

		c.Convey("Select a interface{} channel", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Select(selectUser).Results()
			c.So(rs, shouldSlicesResemble, newUsers)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Select a int channel", func() {
			rs, err := From(getIntChan(arrIntForT)).SetSizeOfChunk(size).Select(selectInt).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

	}

	c.Convey("Test Select Sequential", t, func() { test(30) })
	c.Convey("Test Select parallel", t, func() { test(7) })
}

func BenchmarkBlockSourceWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Where(whereUser).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Where(func(i linq.T) (bool, error) {
			v := i.(user)
			return v.id%2 == 0, nil
		}).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

//func BenchmarkGoLinqParallelWhere(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := linq.From(arrUser).AsParallel().Where(func(i linq.T) (bool, error) {
//			v := i.(user)
//			return v.id%2 == 0, nil
//		}).Results()
//		if len(dst) != countForB/2 {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

func BenchmarkBlockSourceSelectWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Where(whereUser).Select(selectUser).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqSelectWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Where(func(i linq.T) (bool, error) {
			v := i.(user)
			return v.id%2 == 0, nil
		}).Select(func(v linq.T) (linq.T, error) {
			u := v.(user)
			return strconv.Itoa(u.id) + "/" + u.name, nil
		}).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

//func BenchmarkGoLinqParallelSelectWhere(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := linq.From(arrUser).AsParallel().Where(func(i linq.T) (bool, error) {
//			v := i.(user)
//			return v.id%2 == 0, nil
//		}).Select(func(v linq.T) (linq.T, error) {
//			u := v.(user)
//			return strconv.Itoa(u.id) + "/" + u.name, nil
//		}).Results()
//		if len(dst) != countForB/2 {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

func BenchmarkBlockSourceGroupBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).GroupBy(func(v interface{}) interface{} {
			return v.(user).id / 10
		}).Results()
		if len(dst) != countForB/10 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

//test distinct-----------------------------------------------------------------------------
func distinctUser(v interface{}) interface{} {
	u := v.(user)
	return u.id
}
func BenchmarkBlockSourceDistinct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrRptUser).DistinctBy(distinctUser).Results()
		if len(dst) != countForB {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqDistinct(b *testing.B) {
	if countForB > 10000 {
		b.Fatal()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).DistinctBy(func(a linq.T, b linq.T) (bool, error) {
			v1, v2 := a.(user), b.(user)
			return v1.id == v2.id, nil
		}).Results()
		if len(dst) != countForB {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

//func BenchmarkGoLinqParallelDistinct(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := linq.From(arrUser).DistinctBy(func(a linq.T, b linq.T) (bool, error) {
//			v1, v2 := a.(user), b.(user)
//			return v1.id == v2.id, nil
//		}).AsParallel().Results()
//		if len(dst) != countForB {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

//test order-----------------------------------------------------------------------------
func orderUser(v1 interface{}, v2 interface{}) int {
	u1, u2 := v1.(user), v2.(user)
	if u1.id < u2.id {
		return -1
	} else if u1.id == u2.id {
		return 0
	} else {
		return 1
	}
}

func randomList(list []interface{}) {
	rand.Seed(10)
	for i := 0; i < len(list); i++ {
		swapIndex := rand.Intn(len(list))
		t := list[swapIndex]
		list[swapIndex] = list[i]
		list[i] = t
	}
}

func BenchmarkBlockSourceOrder(b *testing.B) {
	b.StopTimer()
	randoms := make([]interface{}, 0, len(arrRptUser))
	_ = copy(randoms, arrRptUser)
	randomList(randoms)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dst, _ := From(arrRptUser).OrderBy(orderUser).Results()
		if len(dst) != len(arrRptUser) || dst[0].(user).id != 0 || dst[10].(user).id != 5 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

//test join-----------------------------------------------------------------
func userSelector(v interface{}) interface{} {
	return v.(user).id
}
func roleSelector(v interface{}) interface{} {
	r := v.(role)
	return r.uid
}
func resultSelector(u interface{}, v interface{}) interface{} {
	return strconv.Itoa(u.(user).id) + "-" + v.(role).role
}

func BenchmarkBlockSourceJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Join(arrRole, userSelector, roleSelector, resultSelector).Results()
		if len(dst) != countForB {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqJoin(b *testing.B) {
	if countForB > 10000 {
		b.Fatal()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Join(arrRole, func(v linq.T) linq.T {
			return v.(user).id
		}, func(v linq.T) linq.T {
			r := v.(role)
			return r.uid
		}, func(u linq.T, v linq.T) linq.T {
			return strconv.Itoa(u.(user).id) + "-" + v.(role).role
		}).Results()
		if len(dst) != countForB {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test union--------------------------------------------------------------------
func BenchmarkBlockSourceUnion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Union(arrUser2).Results()
		if len(dst) != countForB+countForB/2 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqUnion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Union(arrUser2).Results()
		if len(dst) != countForB+countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test concat--------------------------------------------------------------------
func BenchmarkBlockSourceConcat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Concat(arrUser2).Results()
		if len(dst) != countForB+countForB {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

////test intersect--------------------------------------------------------------------
func BenchmarkBlockSourceIntersect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Intersect(arrUser2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqIntersect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Intersect(arrUser2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test except--------------------------------------------------------------------
func BenchmarkBlockSourceExcept(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Except(arrUser2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqExcept(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Except(arrUser2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test reverse--------------------------------------------------------------------
func BenchmarkBlockSourceReverse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Reverse().Results()
		if len(dst) != countForB {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqReverse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).Reverse().Results()
		if len(dst) != countForB {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

func sum(v interface{}, summary interface{}) interface{} {
	return v.(int) + summary.(int)
}

////test reverse--------------------------------------------------------------------
func BenchmarkBlockSourceAggregate(b *testing.B) {
	var (
		r   interface{}
		err error
	)
	for i := 0; i < b.N; i++ {
		if r, err = From(arr).Aggregate(Sum); err != nil {
			b.Fail()
			b.Error(err)
		}
		//if len(dst) != countForB {
		//	b.Fail()
		//	//b.Log("arr=", arr)
		//	b.Error("size is ", len(dst))
		//	b.Log("dst=", dst)
		//}
	}
	b.Log(r)
}

func BenchmarkGoLinqAggregate(b *testing.B) {
	var (
		r   interface{}
		err error
	)
	for i := 0; i < b.N; i++ {
		if r, err = linq.From(arr).Sum(); err != nil {
			b.Fail()
			b.Error(err)
		}
		//if len(dst) != countForB {
		//	b.Fail()
		//	b.Error("size is ", len(dst))
		//}
	}
	b.Log(r)
}

//func BencmarkQuickSort(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		sortQ := sortable{}
//		sortQ.less = func(this, that interface{}) bool {
//			return this.(user).id < that.(user).id
//		}
//		sortQ.values = make([]interface{}, len(arrUser))
//		_ = copy(sortQ.values, arrUser)
//		sort.Sort(sortQ)
//	}
//}

//func mergeChunk(results []interface{}, i int, tr []interface{}, start int) int {
//	c1 := results[i].(*chunk)
//	if i+1 == len(results) {
//		//only a chunk
//		copy(tr[start:start+len(c1.Data)], c1.Data)
//		return len(tr)
//	}

//	c2 := results[i+1].(*chunk)
//	j, k, m := 0, 0, start
//	for ; j < len(c1.Data) && k < len(c2.Data); m++ {
//		if c1.Data[j].(user).id < c2.Data[k].(user).id {
//			tr[m] = c1.Data[j]
//			j++
//		} else {
//			tr[m] = c2.Data[k]
//			k++
//		}
//	}

//	if j < len(c1.Data) {
//		for i := 0; i < len(c1.Data)-j; i++ {
//			tr[m+i] = c1.Data[j+i]
//		}
//	}
//	if k < len(c2.Data) {
//		for i := 0; i < len(c2.Data)-k; i++ {
//			tr[m+i] = c2.Data[k+i]
//		}
//	}
//	return start + len(c1.Data) + len(c2.Data)
//}

//func BencmarkParallelSort(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		src := &blockSource{arrUser}
//		tout := make([]interface{}, len(arrUser), len(arrUser))
//		f := makeBlockTasks(src, func(c *chunk) []interface{} {
//			sortQ := sortableQuery{}
//			sortQ.less = func(this, that interface{}) bool {
//				return this.(user).id < that.(user).id
//			}
//			sortQ.values = tout[c.Order : c.Order+len(c.Data)]
//			_ = copy(sortQ.values, c.Data)
//			return []interface{}{&chunk{sortQ.values, c.Order}, true}
//		}, MAXPROCS)

//		sourceFromFuture(f, func(results []interface{}) source {
//			//tr := make([]interface{}, len(arrUser), len(arrUser))

//			//l := 0
//			//for len(results) > 1 {
//			//	k, start, j := ceilSplitSize(len(results), 2), 0, 0

//			//	tresult := make([]interface{}, k, k)
//			//	var t []interface{}
//			//	if l%2 == 0 {
//			//		t = tr
//			//	} else {
//			//		t = tout
//			//	}
//			//	for i := 0; i < len(results); i = i + 2 {
//			//		//merge two chunk
//			//		nextStart := mergeChunk(results, i, t, start)
//			//		tresult[j] = &chunk{results[start:nextStart], start}
//			//		j++
//			//	}
//			//	results = tresult
//			//}
//			//result := expandSlice(results)
//			//return &blockSource{result}
//			return nil
//		})
//	}
//}

func TestAvl(t *testing.T) {
	compareUser := func(a interface{}, b interface{}) int {
		if a == nil && b == nil {
			return 0
		} else if a == nil {
			return -1
		} else if b == nil {
			return 1
		}

		a1, b1 := a.(*user), b.(*user)
		if a1.id < b1.id {
			return -1
		} else if a1.id == b1.id {
			return 0
		} else {
			return 1
		}
	}

	avlValid := func(src []interface{}, validFunc func(sorted []interface{})) {
		avlRef := NewAvlTree(compareUser)
		for _, v := range src {
			avlRef.Insert(v)
		}

		sorted := avlRef.ToSlice()
		validFunc(sorted)
	}

	avlValid([]interface{}{nil, nil, &user{1, "user1"}, nil,
		&user{4, "user4"}, &user{1, "user1"}, &user{6, "user6"},
		&user{5, "user5"}, &user{2, "user2"}},
		func(sorted []interface{}) {
			if len(sorted) != 6 || sorted[0] != nil || sorted[5].(*user).id != 6 {
				t.Log("failed, sort result=", sorted)
			}
		})
	avlValid([]interface{}{},
		func(sorted []interface{}) {
			if len(sorted) != 0 {
				t.Log("failed, sort result=", sorted)
			}
		})

}

func getChan(src []interface{}) chan interface{} {
	chanSrc := make(chan interface{})
	go func() {
		for _, v := range src {
			chanSrc <- v
		}
		close(chanSrc)
	}()
	return chanSrc
}

func getIntChan(src []int) chan int {
	chanSrc := make(chan int)
	go func() {
		for _, v := range src {
			chanSrc <- v
		}
		close(chanSrc)
	}()
	return chanSrc
}

func shouldSlicesResemble(actual interface{}, expected ...interface{}) string {
	actualSlice, expectedSlice := reflect.ValueOf(actual), reflect.ValueOf(expected[0])
	if actualSlice.Kind() != expectedSlice.Kind() {
		return fmt.Sprintf("Expected1: '%v'\nActual:   '%v'\n", expected[0], actual)
	}

	if actualSlice.Kind() != reflect.Slice {
		return fmt.Sprintf("Expected2: '%v'\nActual:   '%v'\n", expected[0], actual)
	}

	if actualSlice.Len() != expectedSlice.Len() {
		return fmt.Sprintf("Expected3: '%v'\nActual:   '%v'\n", expected[0], actual)
	}

	for i := 0; i < actualSlice.Len(); i++ {
		if !reflect.DeepEqual(actualSlice.Index(i).Interface(), expectedSlice.Index(i).Interface()) {
			return fmt.Sprintf("Expected4: '%v'\nActual:   '%v'\n", expected[0], actual)
			//return fmt.Sprintf("Expected4: '%v'\nActual:   '%v'\n", actualSlice.Index(i).Interface(), expectedSlice.Index(i).Interface())
		}
	}
	return ""
}
