package plinq

import (
	"fmt"
	"github.com/ahmetalpbalkan/go-linq"
	"reflect"
	"runtime"
	"strconv"
	"testing"
)

const (
	countForB    int  = 1000000
	rptCountForB int  = 1100000
	testGoLinq   bool = true
)

var (
	bInts     []int         = make([]int, countForB, countForB)
	bUsers    []interface{} = make([]interface{}, countForB, countForB)
	bRptUsers []user        = make([]user, rptCountForB, rptCountForB)
	bUsers2   []user        = make([]user, countForB, countForB)
	bRoles    []role        = make([]role, countForB, countForB)
)

func init() {
	fmt.Println("DEFAULTCHUNKSIZE=", DEFAULTCHUNKSIZE)
	fmt.Println("countForB=", countForB)
	MAXPROCS = numCPU
	runtime.GOMAXPROCS(MAXPROCS)
	for i := 0; i < countForB; i++ {
		bInts[i] = i
		bUsers[i] = user{i, "user" + strconv.Itoa(i)}
		bRptUsers[i] = user{i, "user" + strconv.Itoa(i)}
		bUsers2[i] = user{i + countForB/2, "user" + strconv.Itoa(i+countForB/2)}
	}

	for i := 0; i < rptCountForB-countForB; i++ {
		bRptUsers[countForB+i] = user{i, "user" + strconv.Itoa(countForB+i)}
	}

	for i := 0; i < countForB/2; i++ {
		bRoles[i*2] = role{i, "role" + strconv.Itoa(i)}
		bRoles[i*2+1] = role{i, "role" + strconv.Itoa(i+1)}
	}
}

func BenchmarkCopy1(b *testing.B) {
	var rs []interface{}
	for i := 0; i < b.N; i++ {
		v := reflect.ValueOf(bUsers2)
		size := len(bUsers2)
		rs = make([]interface{}, size)
		for i := 0; i < size; i++ {
			rs[i] = v.Index(i).Interface()
		}

		if len(rs) != len(bUsers2) {
			b.Fail()
			b.Error("size is ", len(bUsers2))
		}
	}
	b.StopTimer()
	b.Log(rs[0])
}

func BenchmarkCopy2(b *testing.B) {
	var rs []interface{}
	for i := 0; i < b.N; i++ {
		var i interface{}
		i = bUsers2
		v := reflect.ValueOf(i)
		size := v.Len()
		rs = make([]interface{}, size)
		for i := 0; i < size; i++ {
			rs[i] = v.Index(i).Interface()
		}

		if len(rs) != len(bUsers2) {
			b.Fail()
			b.Error("size is ", len(bUsers2))
		}
	}
	b.StopTimer()
	b.Log(rs[0])
}

func BenchmarkCopy3(b *testing.B) {
	var rs reflect.Value
	for i := 0; i < b.N; i++ {
		var i interface{}
		i = bUsers2
		v := reflect.ValueOf(i)
		size := v.Len()
		rs = reflect.MakeSlice(reflect.SliceOf(v.Type().Elem()), size, size)
		reflect.Copy(rs, v)
		if rs.Len() != len(bUsers2) {
			b.Fail()
			b.Error("size is ", len(bUsers2))
		}
	}
	b.StopTimer()
	b.Log(rs.Index(0).Interface())
}

func BenchmarkFor1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, v := range bUsers {
			_ = v
		}
	}
}

func BenchmarkFor2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var i interface{}
		i = bUsers
		v := reflect.ValueOf(i)
		size := v.Len()
		for i := 0; i < size; i++ {
			_ = v.Index(i).Interface()
		}
	}
}

func BenchmarkGoPLinq_Where(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(bUsers).Where(filterUser, 1000000).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinq_Where(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(bUsers).Where(func(i linq.T) (bool, error) {
			v := i.(user)
			return strconv.Itoa(v.id%2) == "0", nil
		}).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

func BenchmarkGoPLinq_Select(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(bUsers).Select(selectUser).Results()
		if len(dst) != countForB {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinq_Select(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(bUsers).Select(func(v linq.T) (linq.T, error) {
			u := v.(user)
			return strconv.Itoa(u.id) + "/" + u.name, nil
		}).Results()
		if len(dst) != countForB {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

//func BenchmarkGoPLinq_GroupBy(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := From(bUsers).GroupBy(func(v interface{}) interface{} {
//			return v.(user).id / 10
//		}).Results()
//		if len(dst) != countForB/10 {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

////test distinct-----------------------------------------------------------------------------
//func BenchmarkGoPLinq_Distinct(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := From(bRptUsers).DistinctBy(distinctUser).Results()
//		if len(dst) != countForB {
//			b.Fail()
//			//b.Log("arr=", arr)
//			b.Error("size is ", len(dst))
//			b.Log("dst=", dst)
//		}
//	}
//}

//func BenchmarkGoLinq_Distinct(b *testing.B) {
//	if !testGoLinq {
//		b.SkipNow()
//		return
//	}
//	if countForB > 1000 {
//		b.Fatal()
//		return
//	}
//	for i := 0; i < b.N; i++ {
//		dst, _ := linq.From(bUsers).DistinctBy(func(a linq.T, b linq.T) (bool, error) {
//			v1, v2 := a.(user), b.(user)
//			return v1.id == v2.id, nil
//		}).Results()
//		if len(dst) != countForB {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

//test order-----------------------------------------------------------------------------

//func randomList(list []interface{}) {
//	rand.Seed(10)
//	for i := 0; i < len(list); i++ {
//		swapIndex := rand.Intn(len(list))
//		t := list[swapIndex]
//		list[swapIndex] = list[i]
//		list[i] = t
//	}
//}

//func BenchmarkGoPLinqOrder(b *testing.B) {
//	b.StopTimer()
//	randoms := make([]interface{}, 0, len(bRptUsers))
//	_ = copy(randoms, bRptUsers)
//	randomList(randoms)
//	b.StartTimer()

//	for i := 0; i < b.N; i++ {
//		dst, _ := From(bRptUsers).OrderBy(orderUserById).Results()
//		if len(dst) != len(bRptUsers) || dst[0].(user).id != 0 || dst[10].(user).id != 5 {
//			b.Fail()
//			//b.Log("arr=", arr)
//			b.Error("size is ", len(dst))
//			b.Log("dst=", dst)
//		}
//	}
//}

////test join-----------------------------------------------------------------
//func BenchmarkGoPLinq_Join(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := From(bUsers).Join(bRoles, userSelector, roleSelector, resultSelector).Results()
//		if len(dst) != countForB {
//			b.Fail()
//			//b.Log("arr=", arr)
//			b.Error("size is ", len(dst))
//			b.Log("dst=", dst)
//		}
//	}
//}

//func BenchmarkGoLinq_Join(b *testing.B) {
//	if !testGoLinq {
//		b.SkipNow()
//		return
//	}
//	if countForB > 1000 {
//		b.Fatal()
//		return
//	}
//	for i := 0; i < b.N; i++ {
//		dst, _ := linq.From(bUsers).Join(bRoles, func(v linq.T) linq.T {
//			return v.(user).id
//		}, func(v linq.T) linq.T {
//			r := v.(role)
//			return r.uid
//		}, func(u linq.T, v linq.T) linq.T {
//			return strconv.Itoa(u.(user).id) + "-" + v.(role).role
//		}).Results()
//		if len(dst) != countForB {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

////test union--------------------------------------------------------------------
func BenchmarkGoPLinq_Union(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(bUsers).Union(bUsers2).Results()
		if len(dst) != countForB+countForB/2 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinq_Union(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(bUsers).Union(bUsers2).Results()
		if len(dst) != countForB+countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test concat--------------------------------------------------------------------
//func BenchmarkGoPLinqConcat(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := From(bUsers).Concat(bUsers2).Results()
//		if len(dst) != countForB+countForB {
//			b.Fail()
//			//b.Log("arr=", arr)
//			b.Error("size is ", len(dst))
//			b.Log("dst=", dst)
//		}
//	}
//}

////test intersect--------------------------------------------------------------------
//func BenchmarkGoPLinqIntersect(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		dst, _ := From(bUsers).Intersect(bUsers2).Results()
//		if len(dst) != countForB/2 {
//			b.Fail()
//			//b.Log("arr=", arr)
//			b.Error("size is ", len(dst))
//			b.Log("dst=", dst)
//		}
//	}
//}

//func BenchmarkGoLinqIntersect(b *testing.B) {
//	if !testGoLinq {
//		b.SkipNow()
//		return
//	}
//	for i := 0; i < b.N; i++ {
//		dst, _ := linq.From(bUsers).Intersect(bUsers2).Results()
//		if len(dst) != countForB/2 {
//			b.Fail()
//			b.Error("size is ", len(dst))
//		}
//	}
//}

////test except--------------------------------------------------------------------
func BenchmarkGoPLinq_Except(b *testing.B) {
	filteri = 1
	for i := 0; i < b.N; i++ {
		dst, _ := From(bUsers).Except(bUsers2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinq_Except(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(bUsers).Except(bUsers2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test except--------------------------------------------------------------------
func BenchmarkGoPLinq_Intersect(b *testing.B) {
	filteri = 1
	for i := 0; i < b.N; i++ {
		dst, _ := From(bUsers).Intersect(bUsers2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinq_Intersect(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(bUsers).Intersect(bUsers2).Results()
		if len(dst) != countForB/2 {
			b.Fail()
			b.Error("size is ", len(dst))
		}
	}
}

////test reverse--------------------------------------------------------------------
func BenchmarkGoPLinq_Reverse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dst, _ := From(bUsers).Reverse().Results()
		if len(dst) != countForB {
			b.Fail()
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

func BenchmarkGoLinq_Reverse(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		dst, _ := linq.From(bUsers).Reverse().Results()
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
func BenchmarkGoPLinq_Aggregate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := From(bInts).Aggregate(Sum); err != nil {
			b.Fail()
			b.Error(err)
		}
	}
}

func BenchmarkGoLinq_Aggregate(b *testing.B) {
	if !testGoLinq {
		b.SkipNow()
		return
	}
	for i := 0; i < b.N; i++ {
		if _, err := linq.From(bInts).Sum(); err != nil {
			b.Fail()
			b.Error(err)
		}
	}
}

//test SkipWhile/TakeWhile ---------------------------------------
func testPlinqSkipWhile(b *testing.B, i int) {
	//fmt.Println("find", i)
	if r, err := From(bInts).SkipWhile(func(v interface{}) bool {
		return v.(int) < i
	}, 5000).Results(); err != nil {
		b.Fail()
		b.Error(err)
	} else if len(r) != countForB-i {
		b.Fail()
		b.Error(len(r), i)
	}
}

func BenchmarkGoPLinq_SkipWhile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testPlinqSkipWhile(b, countForB/3)
		testPlinqSkipWhile(b, 2*countForB/3)
		testPlinqSkipWhile(b, 5*countForB/6)
	}
}

func testLinqSkipWhile(b *testing.B, i int) {
	if r, err := linq.From(bInts).SkipWhile(func(v linq.T) (bool, error) {
		return v.(int) < i, nil
	}).Results(); err != nil {
		b.Fail()
		b.Error(err)
	} else if len(r) != countForB-i {
		b.Fail()
		b.Error(len(r), i)
	}
}
func BenchmarkGoLinq_SkipWhile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testLinqSkipWhile(b, countForB/3)
		testLinqSkipWhile(b, 2*countForB/3)
		testLinqSkipWhile(b, 5*countForB/6)
	}
}

//test FirstBy ---------------------------------------
func testPlinqFirst(b *testing.B, i int) {
	//fmt.Println("find", i)
	if r, found, err := From(bInts).FirstBy(func(v interface{}) bool {
		return v.(int) == i
	}, 5000); err != nil {
		b.Fail()
		b.Error(err)
	} else if r != i || !found {
		b.Fail()
		b.Error(r, i, found)
	}
}

func BenchmarkGoPLinq_FirstBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testPlinqFirst(b, countForB/3)
		testPlinqFirst(b, 2*countForB/3)
		testPlinqFirst(b, 5*countForB/6)
	}
}

func testLinqFirst(b *testing.B, i int) {
	if j, found, err := linq.From(bInts).FirstBy(func(v linq.T) (bool, error) {
		return v.(int) == i, nil
	}); err != nil {
		b.Fail()
		b.Error(err)
	} else if j != i || !found {
		b.Fail()
		b.Error(j, i, found)
	}
}
func BenchmarkGoLinq_FirstBy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testLinqFirst(b, countForB/3)
		testLinqFirst(b, 2*countForB/3)
		testLinqFirst(b, 5*countForB/6)
	}
}

//func BencmarkQuickSort(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		sortQ := sortable{}
//		sortQ.less = func(this, that interface{}) bool {
//			return this.(user).id < that.(user).id
//		}
//		sortQ.values = make([]interface{}, len(bUsers))
//		_ = copy(sortQ.values, bUsers)
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
//		src := &blockSource{bUsers}
//		tout := make([]interface{}, len(bUsers), len(bUsers))
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
//			//tr := make([]interface{}, len(bUsers), len(bUsers))

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
