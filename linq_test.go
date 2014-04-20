package plinq

import (
	"errors"
	"fmt"
	"github.com/ahmetalpbalkan/go-linq"
	c "github.com/smartystreets/goconvey/convey"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const (
	count        int = 22
	rptCount     int = 24
	countForB    int = 100000
	rptCountForB int = 110000
)

var (
	MAXPROCS       int
	arrUserForT    []interface{}       = make([]interface{}, count, count)
	arrRptUserForT []interface{}       = make([]interface{}, rptCount, rptCount)
	arrUser2ForT   []interface{}       = make([]interface{}, count, count)
	arrIntForT     []int               = make([]int, count, count)
	mapForT        map[int]interface{} = make(map[int]interface{}, count)
	arrRoleForT    []interface{}       = make([]interface{}, count, count)

	arr        []interface{} = make([]interface{}, countForB, countForB)
	arrUser    []interface{} = make([]interface{}, countForB, countForB)
	arrRptUser []interface{} = make([]interface{}, rptCountForB, rptCountForB)
	arrUser2   []interface{} = make([]interface{}, countForB, countForB)

	arrRole []interface{} = make([]interface{}, countForB, countForB)
)

func init() {
	MAXPROCS = numCPU
	runtime.GOMAXPROCS(MAXPROCS)
	for i := 0; i < count; i++ {
		arrIntForT[i] = i
		arrUserForT[i] = user{i, "user" + strconv.Itoa(i)}
		arrRptUserForT[i] = user{i, "user" + strconv.Itoa(i)}
		arrUser2ForT[i] = user{i + count/2, "user" + strconv.Itoa(i+count/2)}
		mapForT[i] = user{i, "user" + strconv.Itoa(i)}
	}
	for i := 0; i < rptCount-count; i++ {
		arrRptUserForT[count+i] = user{i, "user" + strconv.Itoa(count+i)}
	}
	for i := 0; i < count/2; i++ {
		arrRoleForT[i*2] = role{i, "role" + strconv.Itoa(i)}
		arrRoleForT[i*2+1] = role{i, "role" + strconv.Itoa(i+1)}
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
type userRoles struct {
	user
	roles []role
}

func wherePanic(v interface{}) bool {
	var s []interface{}
	_ = s[2]
	return true
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

func distinctUser(v interface{}) interface{} {
	u := v.(user)
	return u.id
}

func distinctUserPanic(v interface{}) interface{} {
	var s []interface{}
	_ = s[2]
	u := v.(user)
	return u.id
}

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
func leftResultSelector(u interface{}, v interface{}) interface{} {
	if v != nil {
		return strconv.Itoa(u.(user).id) + "-" + v.(role).role
	} else {
		return strconv.Itoa(u.(user).id)
	}
}

func TestWhere(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(arrIntForT).SetSizeOfChunk(size).Where(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in where function", func() {
			_, err := From(arrIntForT).SetSizeOfChunk(size).Where(wherePanic).Results()
			//fmt.Println("the error appears in where function:", rs, "-----", err)
			//TODO: this case failed once, but cannot be reproducted now, need more testing
			//TODO: should be fixed
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

		c.Convey("Filter an interface{} channel", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Where(whereUser).Results()
			c.So(rs, shouldSlicesResemble, filteredUsers)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Filter an int channel", func() {
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

		c.Convey("Select an interface{} channel", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Select(selectUser).Results()
			c.So(rs, shouldSlicesResemble, newUsers)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Select an int channel", func() {
			rs, err := From(getIntChan(arrIntForT)).SetSizeOfChunk(size).Select(selectInt).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

	}

	c.Convey("Test Select Sequential", t, func() { test(30) })
	c.Convey("Test Select parallel", t, func() { test(7) })
}

func TestDistinct(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(arrIntForT).SetSizeOfChunk(size).DistinctBy(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in DistinctBy function", func() {
			rs, err := From(arrRptUserForT).SetSizeOfChunk(size).DistinctBy(distinctUserPanic).Results()
			if err == nil {
				//fmt.Println("\nDistinct An error get erros:", err)
				fmt.Println("\nDistinct An error returned:", rs)
			}
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Distinct an empty slice", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Distinct().Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		rptInts := make([]int, count+2)
		for i := 0; i < count; i++ {
			rptInts[i] = i
		}
		rptInts[count] = 0
		rptInts[count+1] = 1

		c.Convey("Distinct an int slice", func() {
			rs, err := From(rptInts).SetSizeOfChunk(size).Distinct().Results()
			//TODO: cannot keep order, need more check
			//c.So(rs, shouldSlicesResemble, arrIntForT)
			c.So(len(rs), c.ShouldEqual, len(arrIntForT))
			c.So(err, c.ShouldBeNil)
		})

		newUsers := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		c.Convey("DistinctBy an interface{} slice", func() {
			rs, err := From(arrRptUserForT).SetSizeOfChunk(size).DistinctBy(distinctUser).Results()
			//TODO: cannot keep order, need more check
			//c.So(rs, shouldSlicesResemble, arrUserForT)
			c.So(len(rs), c.ShouldEqual, len(arrUserForT))
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Distinct an interface{} slice", func() {
			rs, err := From(arrRptUserForT).SetSizeOfChunk(size).Distinct().Results()
			//TODO: cannot keep order, need more check
			//c.So(rs, shouldSlicesResemble, arrRptUserForT)
			c.So(len(rs), c.ShouldEqual, len(arrRptUserForT))
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

		c.Convey("Distinct an interface{} channel", func() {
			rs, err := From(getChan(arrRptUserForT)).SetSizeOfChunk(size).DistinctBy(distinctUser).Results()
			//TODO: cannot keep order, need more check
			//c.So(rs, shouldSlicesResemble, arrUserForT)
			c.So(len(rs), c.ShouldEqual, len(arrUserForT))
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Distinct an int channel", func() {
			//TODO: cannot keep order, need more check
			rs, err := From(getIntChan(rptInts)).SetSizeOfChunk(size).Distinct().Results()
			//c.So(rs, shouldSlicesResemble, arrIntForT)
			c.So(len(rs), c.ShouldEqual, len(arrIntForT))
			c.So(err, c.ShouldBeNil)
		})

	}

	c.Convey("Test Distinct Sequential", t, func() { test(30) })
	c.Convey("Test Distinct parallel", t, func() { test(7) })
}

func TestGroupBy(t *testing.T) {
	groupUser := func(v interface{}) interface{} {
		return v.(user).id / 10
	}
	groupUserPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	test := func(size int) {
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(arrIntForT).SetSizeOfChunk(size).GroupBy(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in GroupBy function", func() {
			rs, err := From(arrRptUserForT).SetSizeOfChunk(size).GroupBy(groupUserPanic).Results()
			if err == nil {
				fmt.Println("\nGroup An error should be returned----------:", rs)
			}
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("GroupBy an empty slice", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).GroupBy(groupUserPanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("groupBy an int slice", func() {
			rs, err := From(arrIntForT).SetSizeOfChunk(size).GroupBy(func(v interface{}) interface{} {
				return v.(int) / 10
			}).Results()
			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(arrIntForT), 10))
			c.So(err, c.ShouldBeNil)
		})

		newUsers := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		c.Convey("groupBy an interface{} slice", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).GroupBy(groupUser).Results()
			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(arrUserForT), 10))
			c.So(err, c.ShouldBeNil)
			//for i, v := range rs {
			//	kv := v.(*KeyValue)
			//	fmt.Println(kv)
			//	c.So(kv.key, c.ShouldEqual, i)
			//}
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

		c.Convey("groupBy an interface{} channel", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).GroupBy(groupUser).Results()
			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(arrUserForT), 10))
			c.So(err, c.ShouldBeNil)
			//for i, v := range rs {
			//	kv := v.(*KeyValue)
			//	fmt.Println(kv)
			//	c.So(kv.key, c.ShouldEqual, i)
			//}
		})

	}

	c.Convey("Test groupBy Sequential", t, func() { test(30) })
	c.Convey("Test groupBy parallel", t, func() { test(7) })
}

func TestJoin(t *testing.T) {
	userSelectorPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	roleSelectorPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	resultSelectorPanic := func(u interface{}, v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	checkOrder := func(rs []interface{}) {
		id := 0
		for _, v := range rs {
			u := v.(string)
			uid, _ := strconv.Atoi(strings.TrimSpace(strings.Split(u, "-")[0]))
			c.So(uid, c.ShouldBeGreaterThanOrEqualTo, id)
			id = uid
		}

	}

	testJoin := func(size int) {
		c.Convey("When passed nil inner, error be returned", func() {
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Join(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Join(arrRoleForT, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Join(arrUser2ForT, userSelector, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Join(arrUser2ForT, userSelector, roleSelector, nil) }, c.ShouldPanicWith, ErrResultSelector)
		})

		c.Convey("An error should be returned if the error appears in Join function", func() {
			_, err := From(arrUserForT).SetSizeOfChunk(size).Join(arrRoleForT, userSelectorPanic, roleSelector, resultSelector).Results()
			c.So(err, c.ShouldNotBeNil)

			_, err = From(arrUserForT).SetSizeOfChunk(size).Join(arrRoleForT, userSelector, roleSelectorPanic, resultSelector).Results()
			if err == nil {
				fmt.Println("\nJoin An error should be returned:", err)
			}
			c.So(err, c.ShouldNotBeNil)

			_, err = From(arrUserForT).SetSizeOfChunk(size).Join(arrRoleForT, userSelector, roleSelector, resultSelectorPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Join an empty slice as outer source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Join(arrUser2ForT, userSelector, roleSelector, resultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Join an empty slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Join([]interface{}{}, userSelector, roleSelector, resultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Join an interface{} slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Join(arrRoleForT, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

		c.Convey("Join an interface{} channel as outer source", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Join(arrRoleForT, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

		c.Convey("Join an interface{} channel as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Join(getChan(arrRoleForT), userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

	}
	c.Convey("Test Join Sequential", t, func() { testJoin(30) })
	c.Convey("Test Join parallel", t, func() { testJoin(7) })

	testLeftJoin := func(size int) {
		c.Convey("LeftJoin an empty slice as outer source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).LeftJoin(arrUser2ForT, userSelector, roleSelector, resultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftJoin an empty slice as inner source, but doesn't check if the inner is nil", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).LeftJoin([]interface{}{}, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("LeftJoin an interface{} slice as inner source, but doesn't check if the inner is nil", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).LeftJoin(arrRoleForT, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(rs, c.ShouldBeNil)
			c.So(err, c.ShouldNotBeNil)
			if err != nil {
				//TODO:need check the error message (no error stack)
				//fmt.Println("LeftJoin doesn't check if the inner is nil, get Err:", err)
			}
		})

		c.Convey("LeftJoin an empty slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).LeftJoin([]interface{}{}, userSelector, roleSelector, leftResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			checkOrder(rs)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftJoin an interface{} slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).LeftJoin(arrRoleForT, userSelector, roleSelector, leftResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

	}
	c.Convey("Test LeftJoin Sequential", t, func() { testLeftJoin(30) })
	c.Convey("Test LeftJoin parallel", t, func() { testLeftJoin(7) })

}

func TestGroupJoin(t *testing.T) {
	groupResultSelector := func(u interface{}, rs []interface{}) (r interface{}) {
		roles := make([]role, len(rs))
		for i, r := range rs {
			roles[i] = r.(role)
		}
		return &userRoles{u.(user), roles}
	}

	userSelectorPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	roleSelectorPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	resultSelectorPanic := func(u interface{}, v []interface{}) interface{} {
		panic(errors.New("panic"))
	}

	testGroupJoin := func(size int) {
		c.Convey("When passed nil inner, error be returned", func() {
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).GroupJoin(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrRoleForT, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrUser2ForT, userSelector, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
			c.So(func() {
				From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrUser2ForT, userSelector, roleSelector, nil)
			}, c.ShouldPanicWith, ErrResultSelector)
		})

		c.Convey("An error should be returned if the error appears in GroupJoin function", func() {
			_, err := From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrRoleForT, userSelectorPanic, roleSelector, groupResultSelector).Results()
			c.So(err, c.ShouldNotBeNil)

			rs, err := From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrRoleForT, userSelector, roleSelectorPanic, groupResultSelector).Results()
			//TODO: This case failed once, need more checking
			if err == nil {
				fmt.Println("/nif the error appears in GroupJoin function, return----", rs)
			}
			c.So(err, c.ShouldNotBeNil)

			_, err = From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrRoleForT, userSelector, roleSelector, resultSelectorPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("GroupJoin an empty slice as outer source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).GroupJoin(arrUser2ForT, userSelector, roleSelector, groupResultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("GroupJoin an empty slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).GroupJoin([]interface{}{}, userSelector, roleSelector, groupResultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("GroupJoin an interface{} slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).GroupJoin(arrRoleForT, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

		c.Convey("GroupJoin an interface{} channel as outer source", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).GroupJoin(arrRoleForT, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

		c.Convey("GroupJoin an interface{} channel as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).GroupJoin(getChan(arrRoleForT), userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

	}
	c.Convey("Test GroupJoin Sequential", t, func() { testGroupJoin(30) })
	c.Convey("Test GroupJoin parallel", t, func() { testGroupJoin(7) })

	testLeftGroupJoin := func(size int) {
		c.Convey("LeftGroupJoin an empty slice as outer source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).LeftGroupJoin(arrUser2ForT, userSelector, roleSelector, groupResultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftGroupJoin an empty slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).LeftGroupJoin([]interface{}{}, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftGroupJoin an interface{} slice as inner source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).LeftGroupJoin(arrRoleForT, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

	}
	c.Convey("Test LeftGroupJoin Sequential", t, func() { testLeftGroupJoin(30) })
	c.Convey("Test LeftGroupJoin parallel", t, func() { testLeftGroupJoin(7) })

}

func TestUnion(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Union(nil) }, c.ShouldPanicWith, ErrUnionNilSource)
		})

		c.Convey("Union an empty slice as first source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Union(arrUser2ForT).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an empty slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Union([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an interface{} slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Union(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an interface{} channel as first source", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Union(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an interface{} channel as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Union(getChan(arrUser2ForT)).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
		})
	}
	c.Convey("Test Union Sequential", t, func() { test(30) })
	c.Convey("Test Union parallel", t, func() { test(7) })

}

func TestConcat(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Concat(nil) }, c.ShouldPanicWith, ErrConcatNilSource)
		})

		c.Convey("Concat an empty slice as first source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Concat(arrUser2ForT).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Concat an empty slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Concat([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Concat an interface{} slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Concat(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count*2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Concat an interface{} channel as first source", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Concat(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count*2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Concat an interface{} channel as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Concat(getChan(arrUser2ForT)).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count*2)
			c.So(err, c.ShouldBeNil)
		})
	}
	c.Convey("Test Concat Sequential", t, func() { test(30) })
	c.Convey("Test Concat parallel", t, func() { test(7) })

}

func TestInterest(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Intersect(nil) }, c.ShouldPanicWith, ErrInterestNilSource)
		})

		c.Convey("Interest an empty slice as first source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Intersect(arrUser2ForT).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an empty slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Intersect([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an interface{} slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Intersect(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an interface{} channel as first source", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Intersect(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an interface{} channel as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Intersect(getChan(arrUser2ForT)).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})
	}
	c.Convey("Test Interest Sequential", t, func() { test(30) })
	c.Convey("Test Interest parallel", t, func() { test(7) })

}

func TestExcept(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(arrUserForT).SetSizeOfChunk(size).Except(nil) }, c.ShouldPanicWith, ErrExceptNilSource)
		})

		c.Convey("Except an empty slice as first source", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).Except(arrUser2ForT).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an empty slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Except([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an interface{} slice as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Except(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an interface{} channel as first source", func() {
			rs, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Except(arrUser2ForT).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an interface{} channel as secondary source", func() {
			rs, err := From(arrUserForT).SetSizeOfChunk(size).Except(getChan(arrUser2ForT)).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})
	}
	c.Convey("Test Except Sequential", t, func() { test(30) })
	c.Convey("Test Except parallel", t, func() { test(7) })

}

func orderUserById(v1 interface{}, v2 interface{}) int {
	u1, u2 := v1.(user), v2.(user)
	if u1.id < u2.id {
		return -1
	} else if u1.id == u2.id {
		return 0
	} else {
		return 1
	}
}

func orderUserByIdPanic(v1 interface{}, v2 interface{}) int {
	panic(errors.New("panic"))
}

func TestOrderBy(t *testing.T) {
	test := func(size int) {
		c.Convey("When passed nil function, should use the default compare function", func() {
			rs, err := From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).OrderBy(nil).Results()
			c.So(rs, shouldSlicesResemble, []int{1, 2, 3, 4})
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("An error should be returned if the error appears in OrderBy function", func() {
			_, err := From(arrRptUserForT).SetSizeOfChunk(size).OrderBy(orderUserByIdPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("OrderBy an empty slice", func() {
			rs, err := From([]int{}).SetSizeOfChunk(size).OrderBy(nil).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("OrderBy an interface{} slice", func() {
			rs, err := From(arrRptUserForT).SetSizeOfChunk(size).OrderBy(orderUserById).Results()
			c.So(len(rs), c.ShouldEqual, len(arrRptUserForT))
			c.So(err, c.ShouldBeNil)

			id := 0
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
				id = u.id
			}
		})

		c.Convey("OrderBy an interface{} chan", func() {
			rs, err := From(getChan(arrRptUserForT)).SetSizeOfChunk(size).OrderBy(orderUserById).Results()
			c.So(len(rs), c.ShouldEqual, len(arrRptUserForT))
			c.So(err, c.ShouldBeNil)

			id := 0
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
				id = u.id
			}
		})
	}
	c.Convey("Test Order Sequential", t, func() { test(30) })
}

func TestReverse(t *testing.T) {
	test := func(size int) {

		c.Convey("Reverse an interface{} slice", func() {
			rs, err := From(arrRptUserForT).SetSizeOfChunk(size).OrderBy(orderUserById).Reverse().Results()
			c.So(len(rs), c.ShouldEqual, len(arrRptUserForT))
			c.So(err, c.ShouldBeNil)

			id := 1000000
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
				id = u.id
			}
		})

		c.Convey("Reverse an interface{} chan", func() {
			rs, err := From(getChan(arrRptUserForT)).SetSizeOfChunk(size).OrderBy(orderUserById).Reverse().Results()
			c.So(len(rs), c.ShouldEqual, len(arrRptUserForT))
			c.So(err, c.ShouldBeNil)

			id := 1000000
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
				id = u.id
			}
		})
	}
	c.Convey("Test Reverse Sequential", t, func() { test(30) })
	c.Convey("Test Reverse parallel", t, func() { test(7) })
}

func aggregatePanic(v interface{}, summary interface{}) interface{} {
	panic(errors.New("panic"))
}
func TestAggregate(t *testing.T) {
	myAgg := &AggregateOpr{
		Seed: "",
		AggAction: func(v interface{}, t interface{}) interface{} {
			v1, t1 := v.(user), t.(string)
			return t1 + "|{" + strconv.Itoa(v1.id) + ":" + v1.name + "}"
		},
		ReduceAction: func(t1 interface{}, t2 interface{}) interface{} {
			return t1.(string) + t2.(string)
		},
	}

	test := func(size int) {
		c.Convey("When passed nil function, should use the default compare function", func() {
			_, err := From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).Aggregate(nil)
			c.So(err, c.ShouldNotBeNil)
			_, err = From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).Aggregate(([]*AggregateOpr{})...)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("An error should be returned if the error appears in Aggregate function", func() {
			_, err := From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).Aggregate(&AggregateOpr{0, aggregatePanic, nil})
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("An error appears in previous operation", func() {
			_, err := From(arrUserForT).SetSizeOfChunk(size).Select(selectPanic).Aggregate(myAgg)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Aggregate an empty slice", func() {
			_, err := From([]int{}).SetSizeOfChunk(size).Aggregate(Max)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Aggregate an interface{} slice", func() {
			r, err := From(arrUserForT).SetSizeOfChunk(size).Aggregate(myAgg)
			c.So(err, c.ShouldBeNil)
			_ = r
			//fmt.Println("\n", "Aggregate an interface{} slice return: ", r)
		})

		c.Convey("Aggregate an interface{} channel", func() {
			r, err := From(getChan(arrUserForT)).SetSizeOfChunk(size).Aggregate(myAgg)
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			_ = r
			//fmt.Println("\n", "Aggregate an interface{} channel return: ", r)
		})
	}
	c.Convey("Test Aggregate Sequential", t, func() { test(30) })
	c.Convey("Test Aggregate parallel", t, func() { test(7) })
}

func TestSumCountAvgMaxMin(t *testing.T) {

	test := func(size int) {
		c.Convey("Max an int slice", func() {
			r, err := From(arrIntForT).SetSizeOfChunk(size).Max()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 21)
		})
		c.Convey("Min an int slice", func() {
			r, err := From(arrIntForT).SetSizeOfChunk(size).Min()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 0)
		})
		c.Convey("Sum an int slice", func() {
			r, err := From(arrIntForT).SetSizeOfChunk(size).Sum()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 231)
		})
		c.Convey("Count an int slice", func() {
			r, err := From(arrIntForT).SetSizeOfChunk(size).Count()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 22)
		})
		c.Convey("Count an interface{} slice", func() {
			r, err := From(arrUserForT).SetSizeOfChunk(size).Count()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 22)
		})
		c.Convey("CountBy an interface{} slice", func() {
			r, err := From(arrUserForT).SetSizeOfChunk(size).CountBy(whereUser)
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 11)
		})
		c.Convey("send a nil predicate to CountBy", func() {
			r, err := From(arrUserForT).SetSizeOfChunk(size).CountBy(nil)
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 22)
		})
		c.Convey("Average an int slice", func() {
			r, err := From(arrIntForT).SetSizeOfChunk(size).Average()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 10.5)
		})
	}
	c.Convey("Test Sum/Count/Avg/Max/Min Sequential", t, func() { test(30) })
	c.Convey("Test Sum/Count/Avg/Max/Min parallel", t, func() { test(7) })
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

//test order-----------------------------------------------------------------------------

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
		dst, _ := From(arrRptUser).OrderBy(orderUserById).Results()
		if len(dst) != len(arrRptUser) || dst[0].(user).id != 0 || dst[10].(user).id != 5 {
			b.Fail()
			//b.Log("arr=", arr)
			b.Error("size is ", len(dst))
			b.Log("dst=", dst)
		}
	}
}

//test join-----------------------------------------------------------------
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
	for i := 0; i < b.N; i++ {
		if _, err := From(arr).Aggregate(Sum); err != nil {
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
}

func BenchmarkGoLinqAggregate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := linq.From(arr).Sum(); err != nil {
			b.Fail()
			b.Error(err)
		}
		//if len(dst) != countForB {
		//	b.Fail()
		//	b.Error("size is ", len(dst))
		//}
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
