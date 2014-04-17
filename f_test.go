package main

import (
	"github.com/ahmetalpbalkan/go-linq"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
)

const (
	count         int = 1000
	distinctcount int = 1100
	MAXPROCS      int = 4
)

var (
	arr             []interface{} = make([]interface{}, count, count)
	arrUser         []interface{} = make([]interface{}, count, count)
	arrRepeatedUser []interface{} = make([]interface{}, distinctcount, distinctcount)
	arrUser2        []interface{} = make([]interface{}, count, count)

	arrRole []interface{} = make([]interface{}, count, count)
)

func init() {
	runtime.GOMAXPROCS(MAXPROCS)
	for i := 0; i < count; i++ {
		arr[i] = i
		arrUser[i] = user{i, "user" + strconv.Itoa(i)}
		arrRepeatedUser[i] = user{i, "user" + strconv.Itoa(i)}
		arrUser2[i] = user{i + count/2, "user" + strconv.Itoa(i+count/2)}
	}

	for i := 0; i < distinctcount-count; i++ {
		arrRepeatedUser[count+i] = user{i, "user" + strconv.Itoa(count+i)}
	}

	for i := 0; i < count/2; i++ {
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

func where1(v interface{}) bool {
	i := v.(int)
	//time.Sleep(10 * time.Nanosecond)
	return i%2 == 0
}

func whereUser(v interface{}) bool {
	u := v.(user)
	//time.Sleep(10 * time.Nanosecond)
	return u.id%2 == 0
}

func selectUser(v interface{}) interface{} {
	u := v.(user)
	return strconv.Itoa(u.id) + "/" + u.name
}

func select1(v interface{}) interface{} {
	return v.(int) + 9999
}

func select2(v interface{}) interface{} {
	return math.Sin(math.Cos(math.Pow(float64(v.(int)), 2)))
}

func BenchmarkSyncWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := queryableS{arrUser, make([]func([]interface{}) []interface{}, 0, 1)}
		dst := q.Where(whereUser).Get()

		if len(dst) != count/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

func BenchmarkBlockSourceWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Where(whereUser).Results()
		if len(dst) != count/2 {
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
		if len(dst) != count/2 {
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
//		if len(dst) != count/2 {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

func BenchmarkBlockSourceSelectWhere(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Where(whereUser).Select(selectUser).Results()
		if len(dst) != count/2 {
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
		if len(dst) != count/2 {
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
//		if len(dst) != count/2 {
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
		if len(dst) != count/10 {
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
		dst, _ := From(arrRepeatedUser).DistinctBy(distinctUser).Results()
		if len(dst) != count {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqDistinct(b *testing.B) {
	if count > 10000 {
		b.Fatal()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(arrUser).DistinctBy(func(a linq.T, b linq.T) (bool, error) {
			v1, v2 := a.(user), b.(user)
			return v1.id == v2.id, nil
		}).Results()
		if len(dst) != count {
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
//		if len(dst) != count {
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
	randoms := make([]interface{}, 0, len(arrRepeatedUser))
	_ = copy(randoms, arrRepeatedUser)
	randomList(randoms)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dst, _ := From(arrRepeatedUser).OrderBy(orderUser).Results()
		if len(dst) != len(arrRepeatedUser) || dst[0].(user).id != 0 || dst[10].(user).id != 5 {
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
		if len(dst) != count {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinqJoin(b *testing.B) {
	if count > 10000 {
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
		if len(dst) != count {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test union--------------------------------------------------------------------
func BenchmarkBlockSourceUnion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Union(arrUser2).Results()
		if len(dst) != count+count/2 {
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
		if len(dst) != count+count/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test concat--------------------------------------------------------------------
func BenchmarkBlockSourceConcat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Concat(arrUser2).Results()
		if len(dst) != count+count {
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
		if len(dst) != count/2 {
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
		if len(dst) != count/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test except--------------------------------------------------------------------
func BenchmarkBlockSourceExcept(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Except(arrUser2).Results()
		if len(dst) != count/2 {
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
		if len(dst) != count/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test reverse--------------------------------------------------------------------
func BenchmarkBlockSourceReverse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(arrUser).Reverse().Results()
		if len(dst) != count {
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
		if len(dst) != count {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
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
//		copy(tr[start:start+len(c1.data)], c1.data)
//		return len(tr)
//	}

//	c2 := results[i+1].(*chunk)
//	j, k, m := 0, 0, start
//	for ; j < len(c1.data) && k < len(c2.data); m++ {
//		if c1.data[j].(user).id < c2.data[k].(user).id {
//			tr[m] = c1.data[j]
//			j++
//		} else {
//			tr[m] = c2.data[k]
//			k++
//		}
//	}

//	if j < len(c1.data) {
//		for i := 0; i < len(c1.data)-j; i++ {
//			tr[m+i] = c1.data[j+i]
//		}
//	}
//	if k < len(c2.data) {
//		for i := 0; i < len(c2.data)-k; i++ {
//			tr[m+i] = c2.data[k+i]
//		}
//	}
//	return start + len(c1.data) + len(c2.data)
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
//			sortQ.values = tout[c.order : c.order+len(c.data)]
//			_ = copy(sortQ.values, c.data)
//			return []interface{}{&chunk{sortQ.values, c.order}, true}
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
