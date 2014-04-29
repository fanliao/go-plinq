package plinq

import (
	//"errors"
	"fmt"
	//c "github.com/smartystreets/goconvey/convey"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	//"strings"
	//"testing"
)

const (
	count    int = 20
	rptCount int = 22
)

var (
	MAXPROCS  int
	tUsers    []interface{}       = make([]interface{}, count, count)
	tRptUsers []interface{}       = make([]interface{}, rptCount, rptCount)
	tUsers2   []interface{}       = make([]interface{}, count, count)
	tInts     []int               = make([]int, count, count)
	tMap      map[int]interface{} = make(map[int]interface{}, count)
	tRoles    []interface{}       = make([]interface{}, count, count)

	sequentialChunkSize int = count
	parallelChunkSize   int = count / 7
)

func init() {
	MAXPROCS = numCPU
	runtime.GOMAXPROCS(MAXPROCS)
	for i := 0; i < count; i++ {
		tInts[i] = i
		tUsers[i] = user{i, "user" + strconv.Itoa(i)}
		tRptUsers[i] = user{i, "user" + strconv.Itoa(i)}
		tUsers2[i] = user{i + count/2, "user" + strconv.Itoa(i+count/2)}
		tMap[i] = user{i, "user" + strconv.Itoa(i)}
	}
	for i := 0; i < rptCount-count; i++ {
		tRptUsers[count+i] = user{i, "user" + strconv.Itoa(count+i)}
	}
	for i := 0; i < count/2; i++ {
		tRoles[i*2] = role{i, "role" + strconv.Itoa(i)}
		tRoles[i*2+1] = role{i, "role" + strconv.Itoa(i+1)}
	}
}

// The structs for testing----------------------------------------------------
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

// The functions used in testing----------------------------------------------
func computerTask() {
	for i := 0; i < 2; i++ {
		_ = strconv.Itoa(i)
	}
}

func confusedOrder() {
	rand.Seed(10)
	j := rand.Intn(1000000)
	var sum float64 = 0
	for k := 0; k < j; k++ {
		sum += math.Cos(float64(k)) * math.Pi
	}
	_ = sum
}

func filterWithPanic(v interface{}) bool {
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

func filterInt(v interface{}) bool {
	i := v.(int)
	return i%2 == 0
}

func filterIntForConfusedOrder(v interface{}) bool {
	i := v.(int)
	confusedOrder()
	return i%2 == 0
}

func filterUser(v interface{}) bool {
	u := v.(user)
	//return u.id%2 == 0
	computerTask()
	return strconv.Itoa(u.id%2) == "0"
}

func filterMap(v interface{}) bool {
	u := v.(*KeyValue)
	return u.Key.(int)%2 == 0
}

func selectWithPanic(v interface{}) interface{} {
	var s []interface{}
	_ = s[2]
	u := v.(user)
	return strconv.Itoa(u.id) + "/" + u.name
}

func selectUser(v interface{}) interface{} {
	u := v.(user)
	computerTask()
	return strconv.Itoa(u.id) + "/" + u.name
}

func selectInt(v interface{}) interface{} {
	return v.(int) * 10
}

func selectIntForConfusedOrder(v interface{}) interface{} {
	rand.Seed(10)
	confusedOrder()
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

func resultSelectorForConfusedOrder(u interface{}, v interface{}) interface{} {
	confusedOrder()
	return strconv.Itoa(u.(user).id) + "-" + v.(role).role
}

func leftResultSelector(u interface{}, v interface{}) interface{} {
	if v != nil {
		return strconv.Itoa(u.(user).id) + "-" + v.(role).role
	} else {
		return strconv.Itoa(u.(user).id)
	}
}

//// Testing functions----------------------------------------------------------
//func TestFrom(t *testing.T) {
//	c.Convey("Nil as data source", t, func() {
//		c.So(func() { _ = From(nil) }, c.ShouldPanicWith, ErrNilSource)
//		c.So(func() { _ = From(1) }, c.ShouldPanicWith, ErrUnsupportSource)
//		var pSlice *[]interface{} = nil
//		c.So(func() { _ = From(pSlice) }, c.ShouldPanicWith, ErrNilSource)
//	})
//}

//func TestWhere(t *testing.T) {
//	expectedInts := make([]interface{}, count/2)
//	for i := 0; i < count/2; i++ {
//		expectedInts[i] = i * 2
//	}
//	expectedUsers := make([]interface{}, count/2)
//	for i := 0; i < count/2; i++ {
//		expectedUsers[i] = user{i * 2, "user" + strconv.Itoa(i*2)}
//	}
//	test := func(size int) {
//		c.Convey("When passed nil function, error be returned", func() {
//			c.So(func() { From(tInts).SetSizeOfChunk(size).Where(nil) }, c.ShouldPanicWith, ErrNilAction)
//		})

//		c.Convey("An error should be returned if the error appears in where function", func() {
//			_, err := From(tInts).SetSizeOfChunk(size).Where(filterWithPanic).Results()
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("Filter an empty slice", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Where(filterWithPanic).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Filter an int slice", func() {
//			rs, err := From(tInts).SetSizeOfChunk(size).Where(filterInt).Results()
//			c.So(rs, shouldSlicesResemble, expectedInts)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Filter an int slice, and keep original order", func() {
//			rs, err := From(tInts).SetSizeOfChunk(size).Where(filterIntForConfusedOrder).Results()
//			c.So(rs, shouldSlicesResemble, expectedInts)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Filter an interface{} slice", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Where(filterUser).Results()
//			c.So(rs, shouldSlicesResemble, expectedUsers)
//			c.So(err, c.ShouldBeNil)
//		})

//		//TODO: still have bugs
//		//c.Convey("Filter a map", func() {
//		//	rs, err := From(tMap).Where(filterMap).Results()
//		//	c.So(len(rs), c.ShouldEqual, count/2)
//		//	c.So(err, c.ShouldBeNil)
//		//})

//		c.Convey("Filter an interface{} channel", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Where(filterUser).Results()
//			c.So(rs, shouldSlicesResemble, expectedUsers)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Filter an int channel", func() {
//			rs, err := From(getIntChan(tInts)).SetSizeOfChunk(size).Where(filterInt).Results()
//			c.So(rs, shouldSlicesResemble, expectedInts)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Filter an int channel, and keep original order", func() {
//			rs, err := From(getIntChan(tInts)).SetSizeOfChunk(size).Where(filterIntForConfusedOrder).Results()
//			c.So(rs, shouldSlicesResemble, expectedInts)
//			c.So(err, c.ShouldBeNil)
//		})

//	}

//	c.Convey("Test Where Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Where parallel", t, func() { test(parallelChunkSize) })
//}

//func TestSelect(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil function, error be returned", func() {
//			c.So(func() { From(tInts).SetSizeOfChunk(size).Select(nil) }, c.ShouldPanicWith, ErrNilAction)
//		})

//		c.Convey("An error should be returned if the error appears in select function", func() {
//			_, err := From(tInts).SetSizeOfChunk(size).Select(selectWithPanic).Results()
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("Select an empty slice", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Select(selectWithPanic).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		newInts := make([]interface{}, count)
//		for i := 0; i < count; i++ {
//			newInts[i] = i * 10
//		}
//		c.Convey("select an int slice", func() {
//			rs, err := From(tInts).SetSizeOfChunk(size).Select(selectInt).Results()
//			c.So(rs, shouldSlicesResemble, newInts)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("select an int slice, and keep original order", func() {
//			rs, err := From(tInts).SetSizeOfChunk(size).Select(selectIntForConfusedOrder).Results()
//			c.So(rs, shouldSlicesResemble, newInts)
//			c.So(err, c.ShouldBeNil)
//		})

//		newUsers := make([]interface{}, count)
//		for i := 0; i < count; i++ {
//			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
//		}
//		c.Convey("Select an interface{} slice", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Select(selectUser).Results()
//			c.So(rs, shouldSlicesResemble, newUsers)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Select an interface{} channel", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Select(selectUser).Results()
//			c.So(rs, shouldSlicesResemble, newUsers)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Select an int channel", func() {
//			rs, err := From(getIntChan(tInts)).SetSizeOfChunk(size).Select(selectInt).Results()
//			c.So(rs, shouldSlicesResemble, newInts)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Select an int channel, and keep original order", func() {
//			rs, err := From(getIntChan(tInts)).SetSizeOfChunk(size).Select(selectIntForConfusedOrder).Results()
//			c.So(rs, shouldSlicesResemble, newInts)
//			c.So(err, c.ShouldBeNil)
//		})

//	}

//	c.Convey("Test Select Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Select parallel", t, func() { test(parallelChunkSize) })
//}

//func TestDistinct(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil function, error be returned", func() {
//			c.So(func() { From(tInts).SetSizeOfChunk(size).DistinctBy(nil) }, c.ShouldPanicWith, ErrNilAction)
//		})

//		c.Convey("An error should be returned if the error appears in DistinctBy function", func() {
//			rs, err := From(tRptUsers).SetSizeOfChunk(size).DistinctBy(distinctUserPanic).Results()
//			if err == nil {
//				fmt.Println("\nDistinct An error returned:", rs)
//			}
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("Distinct an empty slice", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Distinct().Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		rptInts := make([]int, count+2)
//		for i := 0; i < count; i++ {
//			rptInts[i] = i
//		}
//		rptInts[count] = 0
//		rptInts[count+1] = 1

//		c.Convey("Distinct an int slice", func() {
//			rs, err := From(rptInts).SetSizeOfChunk(size).Distinct().Results()
//			c.So(len(rs), c.ShouldEqual, len(tInts))
//			c.So(err, c.ShouldBeNil)
//		})

//		newUsers := make([]interface{}, count)
//		for i := 0; i < count; i++ {
//			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
//		}
//		c.Convey("DistinctBy an interface{} slice", func() {
//			rs, err := From(tRptUsers).SetSizeOfChunk(size).DistinctBy(distinctUser).Results()
//			c.So(len(rs), c.ShouldEqual, len(tUsers))
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Distinct an interface{} slice", func() {
//			rs, err := From(tRptUsers).SetSizeOfChunk(size).Distinct().Results()
//			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Distinct an interface{} channel", func() {
//			rs, err := From(getChan(tRptUsers)).SetSizeOfChunk(size).DistinctBy(distinctUser).Results()
//			c.So(len(rs), c.ShouldEqual, len(tUsers))
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Distinct an int channel", func() {
//			rs, err := From(getIntChan(rptInts)).SetSizeOfChunk(size).Distinct().Results()
//			c.So(len(rs), c.ShouldEqual, len(tInts))
//			c.So(err, c.ShouldBeNil)
//		})

//	}

//	c.Convey("Test Distinct Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Distinct parallel", t, func() { test(parallelChunkSize) })
//}

//func TestGroupBy(t *testing.T) {
//	groupUser := func(v interface{}) interface{} {
//		return v.(user).id / 10
//	}
//	groupUserPanic := func(v interface{}) interface{} {
//		panic(errors.New("panic"))
//	}
//	test := func(size int) {
//		c.Convey("When passed nil function, error be returned", func() {
//			c.So(func() { From(tInts).SetSizeOfChunk(size).GroupBy(nil) }, c.ShouldPanicWith, ErrNilAction)
//		})

//		c.Convey("An error should be returned if the error appears in GroupBy function", func() {
//			rs, err := From(tRptUsers).SetSizeOfChunk(size).GroupBy(groupUserPanic).Results()
//			if err == nil {
//				fmt.Println("\nGroup An error should be returned----------:", rs)
//			}
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("GroupBy an empty slice", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).GroupBy(groupUserPanic).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("groupBy an int slice", func() {
//			rs, err := From(tInts).SetSizeOfChunk(size).GroupBy(func(v interface{}) interface{} {
//				return v.(int) / 10
//			}).Results()
//			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(tInts), 10))
//			c.So(err, c.ShouldBeNil)
//		})

//		newUsers := make([]interface{}, count)
//		for i := 0; i < count; i++ {
//			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
//		}
//		c.Convey("groupBy an interface{} slice", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).GroupBy(groupUser).Results()
//			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(tUsers), 10))
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("groupBy an interface{} channel", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).GroupBy(groupUser).Results()
//			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(tUsers), 10))
//			c.So(err, c.ShouldBeNil)
//		})

//	}

//	c.Convey("Test groupBy Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test groupBy parallel", t, func() { test(parallelChunkSize) })
//}

//func TestJoin(t *testing.T) {
//	userSelectorPanic := func(v interface{}) interface{} {
//		panic(errors.New("panic"))
//	}
//	roleSelectorPanic := func(v interface{}) interface{} {
//		panic(errors.New("panic"))
//	}
//	resultSelectorPanic := func(u interface{}, v interface{}) interface{} {
//		panic(errors.New("panic"))
//	}
//	checkOrder := func(rs []interface{}) {
//		id := 0
//		for _, v := range rs {
//			u := v.(string)
//			uid, _ := strconv.Atoi(strings.TrimSpace(strings.Split(u, "-")[0]))
//			c.So(uid, c.ShouldBeGreaterThanOrEqualTo, id)
//			id = uid
//		}

//	}

//	testJoin := func(size int) {
//		c.Convey("When passed nil inner, error be returned", func() {
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Join(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Join(tRoles, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Join(tUsers2, userSelector, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Join(tUsers2, userSelector, roleSelector, nil) }, c.ShouldPanicWith, ErrResultSelector)
//		})

//		c.Convey("An error should be returned if the error appears in Join function", func() {
//			_, err := From(tUsers).SetSizeOfChunk(size).Join(tRoles, userSelectorPanic, roleSelector, resultSelector).Results()
//			c.So(err, c.ShouldNotBeNil)

//			_, err = From(tUsers).SetSizeOfChunk(size).Join(tRoles, userSelector, roleSelectorPanic, resultSelector).Results()
//			if err == nil {
//				fmt.Println("\nJoin An error should be returned:", err)
//			}
//			c.So(err, c.ShouldNotBeNil)

//			_, err = From(tUsers).SetSizeOfChunk(size).Join(tRoles, userSelector, roleSelector, resultSelectorPanic).Results()
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("Join an empty slice as outer source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Join(tUsers2, userSelector, roleSelector, resultSelector).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Join an empty slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Join([]interface{}{}, userSelector, roleSelector, resultSelector).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Join an interface{} slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Join(tRoles, userSelector, roleSelector, resultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//			checkOrder(rs)
//		})

//		expectedJoinRs := make([]interface{}, count)
//		for i := 0; i < count/2; i++ {
//			expectedJoinRs[2*i] = strconv.Itoa(i) + "-role" + strconv.Itoa(i)
//			expectedJoinRs[2*i+1] = strconv.Itoa(i) + "-role" + strconv.Itoa(i+1)
//		}

//		//Test keep original order
//		c.Convey("Join an interface{} slice as inner source, and keep original order", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Join(tRoles, userSelector, roleSelector, resultSelectorForConfusedOrder).Results()
//			//TODO: need test KeepOrder()
//			//c.So((rs), shouldSlicesResemble, expectedJoinRs)
//			c.So(len(rs), c.ShouldEqual, len(expectedJoinRs))
//			c.So(err, c.ShouldBeNil)
//			checkOrder(rs)
//		})

//		c.Convey("Join an interface{} channel as outer source", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Join(tRoles, userSelector, roleSelector, resultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//			checkOrder(rs)
//		})

//		c.Convey("Join an interface{} channel as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Join(getChan(tRoles), userSelector, roleSelector, resultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//			checkOrder(rs)
//		})

//		//Test keep original order
//		c.Convey("Join an interface{} channel as inner source, and keep original order", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Join(getChan(tRoles), userSelector, roleSelector, resultSelectorForConfusedOrder).Results()
//			c.So((rs), shouldSlicesResemble, expectedJoinRs)
//			c.So(err, c.ShouldBeNil)
//			checkOrder(rs)
//		})
//	}
//	c.Convey("Test Join Sequential", t, func() { testJoin(30) })
//	c.Convey("Test Join parallel", t, func() { testJoin(7) })

//	testLeftJoin := func(size int) {
//		c.Convey("LeftJoin an empty slice as outer source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).LeftJoin(tUsers2, userSelector, roleSelector, resultSelector).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("LeftJoin an empty slice as inner source, but doesn't check if the inner is nil", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).LeftJoin([]interface{}{}, userSelector, roleSelector, resultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("LeftJoin an interface{} slice as inner source, but doesn't check if the inner is nil", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).LeftJoin(tRoles, userSelector, roleSelector, resultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(rs, c.ShouldBeNil)
//			c.So(err, c.ShouldNotBeNil)
//			if err != nil {
//				//TODO:need check the error message (no error stack)
//				//fmt.Println("LeftJoin doesn't check if the inner is nil, get Err:", err)
//			}
//		})

//		c.Convey("LeftJoin an empty slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).LeftJoin([]interface{}{}, userSelector, roleSelector, leftResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count)
//			checkOrder(rs)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("LeftJoin an interface{} slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).LeftJoin(tRoles, userSelector, roleSelector, leftResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count+count/2)
//			c.So(err, c.ShouldBeNil)
//			checkOrder(rs)
//		})

//	}
//	c.Convey("Test LeftJoin Sequential", t, func() { testLeftJoin(30) })
//	c.Convey("Test LeftJoin parallel", t, func() { testLeftJoin(7) })

//}

//func TestGroupJoin(t *testing.T) {
//	groupResultSelector := func(u interface{}, rs []interface{}) (r interface{}) {
//		roles := make([]role, len(rs))
//		for i, r := range rs {
//			roles[i] = r.(role)
//		}
//		return &userRoles{u.(user), roles}
//	}

//	userSelectorPanic := func(v interface{}) interface{} {
//		panic(errors.New("panic"))
//	}
//	roleSelectorPanic := func(v interface{}) interface{} {
//		panic(errors.New("panic"))
//	}
//	resultSelectorPanic := func(u interface{}, v []interface{}) interface{} {
//		panic(errors.New("panic"))
//	}

//	testGroupJoin := func(size int) {
//		c.Convey("When passed nil inner, error be returned", func() {
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).GroupJoin(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).GroupJoin(tRoles, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).GroupJoin(tUsers2, userSelector, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
//			c.So(func() {
//				From(tUsers).SetSizeOfChunk(size).GroupJoin(tUsers2, userSelector, roleSelector, nil)
//			}, c.ShouldPanicWith, ErrResultSelector)
//		})

//		c.Convey("An error should be returned if the error appears in GroupJoin function", func() {
//			_, err := From(tUsers).SetSizeOfChunk(size).GroupJoin(tRoles, userSelectorPanic, roleSelector, groupResultSelector).Results()
//			c.So(err, c.ShouldNotBeNil)

//			rs, err := From(tUsers).SetSizeOfChunk(size).GroupJoin(tRoles, userSelector, roleSelectorPanic, groupResultSelector).Results()
//			//TODO: This case failed once, need more checking
//			if err == nil {
//				fmt.Println("/nif the error appears in GroupJoin function, return----", rs)
//			}
//			c.So(err, c.ShouldNotBeNil)

//			_, err = From(tUsers).SetSizeOfChunk(size).GroupJoin(tRoles, userSelector, roleSelector, resultSelectorPanic).Results()
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("GroupJoin an empty slice as outer source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).GroupJoin(tUsers2, userSelector, roleSelector, groupResultSelector).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("GroupJoin an empty slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).GroupJoin([]interface{}{}, userSelector, roleSelector, groupResultSelector).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("GroupJoin an interface{} slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).GroupJoin(tRoles, userSelector, roleSelector, groupResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//			for _, v := range rs {
//				ur := v.(*userRoles)
//				c.So(len(ur.roles), c.ShouldEqual, 2)
//			}
//		})

//		c.Convey("GroupJoin an interface{} channel as outer source", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).GroupJoin(tRoles, userSelector, roleSelector, groupResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//			for _, v := range rs {
//				ur := v.(*userRoles)
//				c.So(len(ur.roles), c.ShouldEqual, 2)
//			}
//		})

//		c.Convey("GroupJoin an interface{} channel as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).GroupJoin(getChan(tRoles), userSelector, roleSelector, groupResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//			for _, v := range rs {
//				ur := v.(*userRoles)
//				c.So(len(ur.roles), c.ShouldEqual, 2)
//			}
//		})

//	}
//	c.Convey("Test GroupJoin Sequential", t, func() { testGroupJoin(30) })
//	c.Convey("Test GroupJoin parallel", t, func() { testGroupJoin(7) })

//	testLeftGroupJoin := func(size int) {
//		c.Convey("LeftGroupJoin an empty slice as outer source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).LeftGroupJoin(tUsers2, userSelector, roleSelector, groupResultSelector).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("LeftGroupJoin an empty slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).LeftGroupJoin([]interface{}{}, userSelector, roleSelector, groupResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("LeftGroupJoin an interface{} slice as inner source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).LeftGroupJoin(tRoles, userSelector, roleSelector, groupResultSelector).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//	}
//	c.Convey("Test LeftGroupJoin Sequential", t, func() { testLeftGroupJoin(30) })
//	c.Convey("Test LeftGroupJoin parallel", t, func() { testLeftGroupJoin(7) })

//}

//func TestUnion(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil source, error be returned", func() {
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Union(nil) }, c.ShouldPanicWith, ErrUnionNilSource)
//		})

//		c.Convey("Union an empty slice as first source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Union(tUsers2).Results()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Union an empty slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Union([]interface{}{}).Results()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Union an interface{} slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Union(tUsers2).Results()
//			c.So(len(rs), c.ShouldEqual, count+count/2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Union an interface{} channel as first source", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Union(tUsers2).Results()
//			c.So(len(rs), c.ShouldEqual, count+count/2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Union an interface{} channel as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Union(getChan(tUsers2)).Results()
//			c.So(len(rs), c.ShouldEqual, count+count/2)
//			c.So(err, c.ShouldBeNil)
//		})
//	}
//	c.Convey("Test Union Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Union parallel", t, func() { test(parallelChunkSize) })

//}

//func TestConcat(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil source, error be returned", func() {
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Concat(nil) }, c.ShouldPanicWith, ErrConcatNilSource)
//		})

//		c.Convey("Concat an empty slice as first source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Concat(tUsers2).Results()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Concat an empty slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Concat([]interface{}{}).Results()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Concat an interface{} slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Concat(tUsers2).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count*2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Concat an interface{} channel as first source", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Concat(tUsers2).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count*2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Concat an interface{} channel as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Concat(getChan(tUsers2)).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count*2)
//			c.So(err, c.ShouldBeNil)
//		})
//	}
//	c.Convey("Test Concat Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Concat parallel", t, func() { test(parallelChunkSize) })

//}

//func TestInterest(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil source, error be returned", func() {
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Intersect(nil) }, c.ShouldPanicWith, ErrInterestNilSource)
//		})

//		c.Convey("Interest an empty slice as first source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Intersect(tUsers2).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Interest an empty slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Intersect([]interface{}{}).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Interest an interface{} slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Intersect(tUsers2).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Interest an interface{} channel as first source", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Intersect(tUsers2).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Interest an interface{} channel as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Intersect(getChan(tUsers2)).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//		})
//	}
//	c.Convey("Test Interest Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Interest parallel", t, func() { test(parallelChunkSize) })

//}

//func TestExcept(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil source, error be returned", func() {
//			c.So(func() { From(tUsers).SetSizeOfChunk(size).Except(nil) }, c.ShouldPanicWith, ErrExceptNilSource)
//		})

//		c.Convey("Except an empty slice as first source", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).Except(tUsers2).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Except an empty slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Except([]interface{}{}).Results()
//			c.So(len(rs), c.ShouldEqual, count)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Except an interface{} slice as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Except(tUsers2).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Except an interface{} channel as first source", func() {
//			rs, err := From(getChan(tUsers)).SetSizeOfChunk(size).Except(tUsers2).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("Except an interface{} channel as secondary source", func() {
//			rs, err := From(tUsers).SetSizeOfChunk(size).Except(getChan(tUsers2)).Results()
//			//TODO: need test KeepOrder()
//			c.So(len(rs), c.ShouldEqual, count/2)
//			c.So(err, c.ShouldBeNil)
//		})
//	}
//	c.Convey("Test Except Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Except parallel", t, func() { test(parallelChunkSize) })

//}

//func orderUserById(v1 interface{}, v2 interface{}) int {
//	u1, u2 := v1.(user), v2.(user)
//	if u1.id < u2.id {
//		return -1
//	} else if u1.id == u2.id {
//		return 0
//	} else {
//		return 1
//	}
//}

//func orderUserByIdPanic(v1 interface{}, v2 interface{}) int {
//	panic(errors.New("panic"))
//}

//func TestOrderBy(t *testing.T) {
//	test := func(size int) {
//		c.Convey("When passed nil function, should use the default compare function", func() {
//			rs, err := From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).OrderBy(nil).Results()
//			c.So(rs, shouldSlicesResemble, []int{1, 2, 3, 4})
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("An error should be returned if the error appears in OrderBy function", func() {
//			_, err := From(tRptUsers).SetSizeOfChunk(size).OrderBy(orderUserByIdPanic).Results()
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("OrderBy an empty slice", func() {
//			rs, err := From([]int{}).SetSizeOfChunk(size).OrderBy(nil).Results()
//			c.So(len(rs), c.ShouldEqual, 0)
//			c.So(err, c.ShouldBeNil)
//		})

//		c.Convey("OrderBy an interface{} slice", func() {
//			rs, err := From(tRptUsers).SetSizeOfChunk(size).OrderBy(orderUserById).Results()
//			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
//			c.So(err, c.ShouldBeNil)

//			id := 0
//			for _, v := range rs {
//				u := v.(user)
//				c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
//				id = u.id
//			}
//		})

//		c.Convey("OrderBy an interface{} chan", func() {
//			rs, err := From(getChan(tRptUsers)).SetSizeOfChunk(size).OrderBy(orderUserById).Results()
//			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
//			c.So(err, c.ShouldBeNil)

//			id := 0
//			for _, v := range rs {
//				u := v.(user)
//				c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
//				id = u.id
//			}
//		})
//	}
//	c.Convey("Test Order Sequential", t, func() { test(sequentialChunkSize) })
//}

//func TestReverse(t *testing.T) {
//	test := func(size int) {

//		c.Convey("Reverse an interface{} slice", func() {
//			rs, err := From(tRptUsers).SetSizeOfChunk(size).OrderBy(orderUserById).Reverse().Results()
//			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
//			c.So(err, c.ShouldBeNil)

//			id := 1000000
//			for _, v := range rs {
//				u := v.(user)
//				c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
//				id = u.id
//			}
//		})

//		c.Convey("Reverse an interface{} chan", func() {
//			rs, err := From(getChan(tRptUsers)).SetSizeOfChunk(size).OrderBy(orderUserById).Reverse().Results()
//			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
//			c.So(err, c.ShouldBeNil)

//			id := 1000000
//			for _, v := range rs {
//				u := v.(user)
//				c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
//				id = u.id
//			}
//		})
//	}
//	c.Convey("Test Reverse Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Reverse parallel", t, func() { test(parallelChunkSize) })
//}

//func aggregatePanic(v interface{}, summary interface{}) interface{} {
//	panic(errors.New("panic"))
//}
//func TestAggregate(t *testing.T) {
//	myAgg := &AggregateOpretion{
//		Seed: "",
//		AggAction: func(v interface{}, t interface{}) interface{} {
//			v1, t1 := v.(user), t.(string)
//			return t1 + "|{" + strconv.Itoa(v1.id) + ":" + v1.name + "}"
//		},
//		ReduceAction: func(t1 interface{}, t2 interface{}) interface{} {
//			return t1.(string) + t2.(string)
//		},
//	}

//	test := func(size int) {
//		c.Convey("When passed nil function, should use the default compare function", func() {
//			_, err := From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).Aggregate(nil)
//			c.So(err, c.ShouldNotBeNil)
//			_, err = From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).Aggregate(([]*AggregateOpretion{})...)
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("An error should be returned if the error appears in Aggregate function", func() {
//			_, err := From([]int{4, 2, 3, 1}).SetSizeOfChunk(size).Aggregate(&AggregateOpretion{0, aggregatePanic, nil})
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("An error appears in previous operation", func() {
//			_, err := From(tUsers).SetSizeOfChunk(size).Select(selectWithPanic).Aggregate(myAgg)
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("Aggregate an empty slice", func() {
//			_, err := From([]int{}).SetSizeOfChunk(size).Aggregate(Max)
//			c.So(err, c.ShouldNotBeNil)
//		})

//		c.Convey("Aggregate an interface{} slice", func() {
//			r, err := From(tUsers).SetSizeOfChunk(size).Aggregate(myAgg)
//			c.So(err, c.ShouldBeNil)
//			_ = r
//		})

//		c.Convey("Aggregate an interface{} channel", func() {
//			r, err := From(getChan(tUsers)).SetSizeOfChunk(size).Aggregate(myAgg)
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			_ = r
//		})
//	}
//	c.Convey("Test Aggregate Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Aggregate parallel", t, func() { test(parallelChunkSize) })
//}

//func TestSumCountAvgMaxMin(t *testing.T) {

//	test := func(size int) {
//		c.Convey("Max an int slice", func() {
//			r, err := From(tInts).SetSizeOfChunk(size).Max()
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, count-1)
//		})
//		c.Convey("Min an int slice", func() {
//			r, err := From(tInts).SetSizeOfChunk(size).Min()
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, 0)
//		})
//		c.Convey("Sum an int slice", func() {
//			r, err := From(tInts).SetSizeOfChunk(size).Sum()
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, (count-1)*(count/2))
//		})
//		c.Convey("Count an int slice", func() {
//			r, err := From(tInts).SetSizeOfChunk(size).Count()
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, count)
//		})
//		c.Convey("Count an interface{} slice", func() {
//			r, err := From(tUsers).SetSizeOfChunk(size).Count()
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, count)
//		})
//		c.Convey("CountBy an interface{} slice", func() {
//			r, err := From(tUsers).SetSizeOfChunk(size).CountBy(filterUser)
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, count/2)
//		})
//		c.Convey("send a nil predicate to CountBy", func() {
//			r, err := From(tUsers).SetSizeOfChunk(size).CountBy(nil)
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, count)
//		})
//		c.Convey("Average an int slice", func() {
//			r, err := From(tInts).SetSizeOfChunk(size).Average()
//			//TODO: need test keep order
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, float32(count-1)/float32(2))
//		})
//	}
//	c.Convey("Test Sum/Count/Avg/Max/Min Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Sum/Count/Avg/Max/Min parallel", t, func() { test(parallelChunkSize) })
//}

////获取0到4所有的组合
//func getIndexses(l int) (indexses [][]int) {
//	for i := 0; i < l; i++ {
//		//开始创建一个随机组合
//		for j := 0; j < l; j++ {
//			if j == i {
//				continue
//			}
//			for k := 0; k < l; k++ {
//				if k == j || k == i {
//					continue
//				}
//				for m := 0; m < l; m++ {
//					if m == k || m == j || m == i {
//						continue
//					}
//					for n := 0; n < l; n++ {
//						if n == k || n == j || n == i || n == m {
//							continue
//						}
//						indexs := []int{i, j, k, m, n}
//						indexses = append(indexses, indexs)
//					}
//				}
//			}
//		}
//	}
//	return
//}

//var l int = 5

//func getChunkByi(i int, ints []interface{}) *Chunk {
//	size := count / l
//	return &Chunk{NewSlicer(ints[i*size : (i+1)*size]), i, 0}
//}

//func getCChunkSrc(indexs []int, ints []interface{}) chan *Chunk {
//	chunkSrc := make(chan *Chunk)
//	go func() {
//		//indexs := []int{3, 0, 1, 4, 2}
//		defer func() {
//			if e := recover(); e != nil {
//				_ = e
//			}
//		}()
//		//fmt.Println("\nsend----------------")
//		for _, i := range indexs {
//			//fmt.Println("\nsend", getChunkByi(i))
//			chunkSrc <- getChunkByi(i, ints)
//		}
//		close(chunkSrc)
//	}()
//	return chunkSrc
//}

//func TestSkipAndTake(t *testing.T) {
//	ints := make([]interface{}, count)
//	for i := 0; i < count; i++ {
//		ints[i] = i
//	}

//	indexses := getIndexses(l)
//	//indexses := [][]int{[]int{0, 1, 3, 2, 4}}

//	getSkipResult := func(i int) []interface{} {
//		r := make([]interface{}, count-i)
//		for j := 0; j < count-i; j++ {
//			r[j] = j + i
//		}
//		return r
//	}

//	getTakeResult := func(i int) []interface{} {
//		r := make([]interface{}, i)
//		for j := 0; j < i; j++ {
//			r[j] = j
//		}
//		return r
//	}

//	_ = getTakeResult

//	test := func(size int) {
//		//test list source -----------------------------------------------
//		c.Convey("Test Skip in list", func() {
//			c.Convey("Skip nothing", func() {
//				r, err := From(ints).SetSizeOfChunk(size).Skip(-1).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, ints)
//			})
//			c.Convey("Skip all", func() {
//				r, err := From(ints).SetSizeOfChunk(size).Skip(100).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, []interface{}{})
//			})
//			c.Convey("Skip 12", func() {
//				r, err := From(ints).SetSizeOfChunk(size).Skip(12).Results()
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, getSkipResult(12))
//			})
//		})

//		c.Convey("Test SkipWhile in list", func() {
//			c.Convey("SkipWhile with nil predicateFunc", func() {
//				c.So(func() { From(ints).SetSizeOfChunk(size).SkipWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
//			})

//			c.Convey("SkipWhile using a predicate func with panic error", func() {
//				_, err := From(ints).SetSizeOfChunk(size).SkipWhile(filterWithPanic).Results()
//				c.So(err, c.ShouldNotBeNil)
//			})

//			c.Convey("SkipWhile nothing", func() {
//				//fmt.Println("SkipWhile nothing")
//				r, err := From(ints).SetSizeOfChunk(size).SkipWhile(func(v interface{}) bool {
//					return v.(int) < -1
//				}).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, ints)
//			})

//			c.Convey("SkipWhile all", func() {
//				//fmt.Println("SkipWhile all")
//				r, err := From(ints).SetSizeOfChunk(size).SkipWhile(func(v interface{}) bool {
//					return v.(int) < 100
//				}).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, []interface{}{})
//			})

//			c.Convey("SkipWhile 12", func() {
//				//fmt.Println("SkipWhile 12")
//				r, err := From(ints).SetSizeOfChunk(size).SkipWhile(func(v interface{}) bool {
//					return v.(int) < 12
//				}).Results()
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, getSkipResult(12))
//			})
//		})

//		c.Convey("Test Take in list", func() {
//			c.Convey("Take nothing", func() {
//				r, err := From(ints).SetSizeOfChunk(size).Take(-1).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, []interface{}{})
//			})
//			c.Convey("Take all", func() {
//				r, err := From(ints).SetSizeOfChunk(size).Take(100).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, ints)
//			})
//			c.Convey("Take 12", func() {
//				r, err := From(ints).SetSizeOfChunk(size).Take(12).Results()
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, getTakeResult(12))
//			})
//		})

//		c.Convey("Test TakeWhile in list", func() {
//			c.Convey("TakeWhile using nil predicateFunc", func() {
//				c.So(func() { From(ints).SetSizeOfChunk(size).TakeWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
//			})

//			c.Convey("TakeWhile using a predicate func with panic error", func() {
//				_, err := From(ints).SetSizeOfChunk(size).TakeWhile(filterWithPanic).Results()
//				c.So(err, c.ShouldNotBeNil)
//			})

//			c.Convey("TakeWhile nothing", func() {
//				//fmt.Println("TakeWhile nothing")
//				r, err := From(ints).SetSizeOfChunk(size).TakeWhile(func(v interface{}) bool {
//					return v.(int) < -1
//				}).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, []interface{}{})
//			})

//			c.Convey("TakeWhile all", func() {
//				//fmt.Println("TakeWhile all")
//				r, err := From(ints).SetSizeOfChunk(size).TakeWhile(func(v interface{}) bool {
//					return v.(int) < 100
//				}).Results()
//				//TODO: need test keep order
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, ints)
//			})

//			c.Convey("TakeWhile 12", func() {
//				//fmt.Println("TakeWhile 12")
//				r, err := From(ints).SetSizeOfChunk(size).TakeWhile(func(v interface{}) bool {
//					return v.(int) < 12
//				}).Results()
//				c.So(err, c.ShouldBeNil)
//				c.So(r, shouldSlicesResemble, getTakeResult(12))
//			})
//		})

//		//--------------------------------------------------------
//		c.Convey("Test Skip in channel", func() {
//			c.Convey("Skip nothing", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).Skip(-1).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, ints) != "" {
//						fmt.Println("skip -1 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, ints)
//				}
//			})
//			c.Convey("Skip all", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).Skip(100).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, []interface{}{}) != "" {
//						fmt.Println("skip 100 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, []interface{}{})
//				}
//			})
//			c.Convey("Skip 12", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).Skip(12).Results()
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, getSkipResult(12)) != "" {
//						fmt.Println("skip 12 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, getSkipResult(12))
//				}
//			})
//		})

//		c.Convey("Test SkipWhile in channel", func() {
//			c.Convey("TakeWhile using a predicate func with panic error", func() {
//				_, err := From(getChan(ints)).SetSizeOfChunk(size).SkipWhile(filterWithPanic).Results()
//				c.So(err, c.ShouldNotBeNil)
//			})

//			c.Convey("SkipWhile nothing", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).SkipWhile(func(v interface{}) bool {
//						return v.(int) < -1
//					}).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, ints) != "" {
//						fmt.Println("SkipWhile -1 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, ints)
//				}
//			})
//			c.Convey("SkipWhile all", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).SkipWhile(func(v interface{}) bool {
//						return v.(int) < 100
//					}).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, []interface{}{}) != "" {
//						fmt.Println("SkipWhile 100 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, []interface{}{})
//				}
//			})
//			c.Convey("SkipWhile 12", func() {
//				for _, v := range indexses {
//					//fmt.Println("\nSkipWhile 12 for", v)
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).SkipWhile(func(v interface{}) bool {
//						return v.(int) < 12
//					}).Results()
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, getSkipResult(12)) != "" {
//						fmt.Println("!!!!!!!SkipWhile 12 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, getSkipResult(12))
//				}
//			})
//		})

//		c.Convey("Test Take in channel", func() {
//			c.Convey("Take nothing", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).Take(-1).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					c.So(r, shouldSlicesResemble, []interface{}{})
//				}
//			})
//			c.Convey("Take all", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).Take(100).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					c.So(r, shouldSlicesResemble, ints)
//				}
//			})
//			c.Convey("Take 12", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).Take(12).Results()
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, getTakeResult(12)) != "" {
//						fmt.Println("take 12 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, getTakeResult(12))
//				}
//			})
//		})

//		c.Convey("Test TakeWhile in channel", func() {
//			c.Convey("TakeWhile using a predicate func with panic error", func() {
//				_, err := From(getChan(ints)).SetSizeOfChunk(size).TakeWhile(filterWithPanic).Results()
//				c.So(err, c.ShouldNotBeNil)
//			})

//			c.Convey("TakeWhile nothing", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).TakeWhile(func(v interface{}) bool {
//						return v.(int) < -1
//					}).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, []interface{}{}) != "" {
//						fmt.Println("TakeWhile -1 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, []interface{}{})
//				}
//			})
//			c.Convey("TakeWhile all", func() {
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).TakeWhile(func(v interface{}) bool {
//						return v.(int) < 100
//					}).Results()
//					//TODO: need test keep order
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, ints) != "" {
//						fmt.Println("TakeWhile 100 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, ints)
//				}
//			})
//			c.Convey("TakeWhile 12", func() {
//				//fmt.Println("\nTakeWhile 12")
//				for _, v := range indexses {
//					r, err := From(getCChunkSrc(v, ints)).SetSizeOfChunk(size).TakeWhile(func(v interface{}) bool {
//						return v.(int) < 12
//					}).Results()
//					c.So(err, c.ShouldBeNil)
//					if shouldSlicesResemble(r, getTakeResult(12)) != "" {
//						fmt.Println("TakeWhile 12 for", v, "return", r)
//					}
//					c.So(r, shouldSlicesResemble, getTakeResult(12))
//				}
//			})
//		})

//	}
//	c.Convey("Test Skip and Take Sequential", t, func() { test(sequentialChunkSize) })
//	c.Convey("Test Skip and Take parallel", t, func() { test(parallelChunkSize) })
//}

//func TestElementAt(t *testing.T) {
//	ints := make([]interface{}, count)
//	for i := 0; i < count; i++ {
//		ints[i] = i
//	}

//	indexses := getIndexses(l)

//	c.Convey("Test ElementAt in channel", t, func() {
//		//c.Convey("ElementAt nothing", func() {
//		//	for _, v := range indexses {
//		//		_, found, err := From(getCChunkSrc(v, ints)).ElementAt(-1)
//		//		c.So(err, c.ShouldBeNil)
//		//		c.So(found, c.ShouldEqual, false)
//		//	}
//		//})
//		//c.Convey("ElementAt nothing", func() {
//		//	for _, v := range indexses {
//		//		_, found, err := From(getCChunkSrc(v, ints)).ElementAt(100)
//		//		c.So(err, c.ShouldBeNil)
//		//		c.So(found, c.ShouldEqual, false)
//		//	}
//		//})
//		c.Convey("ElementAt 12", func() {
//			for _, v := range indexses {
//				//_ = indexses
//				r, found, err := From(getCChunkSrc(v, ints)).ElementAt(12)
//				c.So(err, c.ShouldBeNil)
//				c.So(r, c.ShouldEqual, 12)
//				if !found {
//					fmt.Println("found is", found, ", r=", r)
//				}
//				c.So(found, c.ShouldEqual, true)
//			}
//		})
//	})

//	c.Convey("Test ElementAt in list", t, func() {
//		c.Convey("ElementAt nothing", func() {
//			_, found, err := From(tInts).ElementAt(-1)
//			c.So(err, c.ShouldBeNil)
//			c.So(found, c.ShouldEqual, false)
//		})
//		c.Convey("ElementAt nothing", func() {
//			_, found, err := From(tInts).ElementAt(100)
//			c.So(err, c.ShouldBeNil)
//			c.So(found, c.ShouldEqual, false)
//		})
//		c.Convey("ElementAt 12", func() {
//			r, found, err := From(tInts).ElementAt(12)
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, 12)
//			c.So(found, c.ShouldEqual, true)
//		})
//	})
//}

//func TestFirstBy(t *testing.T) {
//	ints := make([]interface{}, count)
//	for i := 0; i < count; i++ {
//		ints[i] = i
//	}

//	indexses := getIndexses(l)

//	c.Convey("Test FirstBy in channel", t, func() {
//		c.Convey("FirstBy with panic an error", func() {
//			for _, v := range indexses {
//				_, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
//					panic(errors.New("!error"))
//				})
//				c.So(err, c.ShouldNotBeNil)
//				c.So(found, c.ShouldEqual, false)
//			}
//		})
//		c.Convey("FirstBy nothing", func() {
//			for _, v := range indexses {
//				_, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
//					return v.(int) == -1
//				})
//				c.So(err, c.ShouldBeNil)
//				c.So(found, c.ShouldEqual, false)
//			}
//		})
//		c.Convey("FirstBy nothing", func() {
//			for _, v := range indexses {
//				_, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
//					return v.(int) == count
//				})
//				c.So(err, c.ShouldBeNil)
//				c.So(found, c.ShouldEqual, false)
//			}
//		})
//		c.Convey("FirstBy 12", func() {
//			for _, v := range indexses {
//				r, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
//					return v.(int) == 12
//				})
//				c.So(err, c.ShouldBeNil)
//				c.So(r, c.ShouldEqual, 12)
//				c.So(found, c.ShouldEqual, true)
//			}
//		})
//	})

//	c.Convey("Test FirstBy in list", t, func() {
//		c.Convey("FirstBy with panic an error", func() {
//			_, found, err := From(tInts).FirstBy(func(v interface{}) bool {
//				panic(errors.New("!error"))
//			})
//			c.So(err, c.ShouldNotBeNil)
//			c.So(found, c.ShouldEqual, false)
//		})
//		c.Convey("FirstBy nothing", func() {
//			_, found, err := From(tInts).FirstBy(func(v interface{}) bool {
//				return v.(int) == -1
//			})
//			c.So(err, c.ShouldBeNil)
//			c.So(found, c.ShouldEqual, false)
//		})
//		c.Convey("FirstBy nothing", func() {
//			_, found, err := From(tInts).FirstBy(func(v interface{}) bool {
//				return v.(int) == 100
//			})
//			c.So(err, c.ShouldBeNil)
//			c.So(found, c.ShouldEqual, false)
//		})
//		c.Convey("FirstBy 12", func() {
//			r, found, err := From(tInts).FirstBy(func(v interface{}) bool {
//				return v.(int) == 12
//			})
//			c.So(err, c.ShouldBeNil)
//			c.So(r, c.ShouldEqual, 12)
//			c.So(found, c.ShouldEqual, true)
//		})
//	})
//}

//func chanToSlice(out chan interface{}) (rs []interface{}) {
//	rs = make([]interface{}, 0, 4)
//	for v := range out {
//		rs = append(rs, v)
//	}
//	return rs
//}

//func TestToChannel(t *testing.T) {
//	c.Convey("Test ToChan of list source", t, func() {
//		c.Convey("For emtpy list", func() {
//			ds := &listSource{NewSlicer([]interface{}{})}
//			out := ds.ToChan()
//			rs := chanToSlice(out)
//			c.So(rs, shouldSlicesResemble, []interface{}{})
//		})
//		c.Convey("For emtpy list", func() {
//			ds := &listSource{NewSlicer([]interface{}{1, 2, 3, 4})}
//			out := ds.ToChan()
//			rs := chanToSlice(out)
//			c.So(rs, shouldSlicesResemble, []interface{}{1, 2, 3, 4})
//		})
//	})

//	expectedInts := make([]interface{}, count/2)
//	for i := 0; i < count/2; i++ {
//		expectedInts[i] = i * 2
//	}
//	c.Convey("Test ToChan of channel source", t, func() {
//		c.Convey("For emtpy channel", func() {
//			out, err := From([]int{}).Where(filterWithPanic).ToChan()
//			c.So(err, c.ShouldBeNil)
//			rs := chanToSlice(out)
//			c.So(rs, shouldSlicesResemble, []interface{}{})
//		})

//		c.Convey("For channel", func() {
//			out, err := From(tInts).Where(filterInt).ToChan()
//			c.So(err, c.ShouldBeNil)
//			rs := chanToSlice(out)
//			c.So(rs, shouldSlicesResemble, expectedInts)
//		})

//		c.Convey("For origin channel", func() {
//			src := make(chan int)
//			go func() {
//				for i := 0; i < count; i++ {
//					src <- i
//				}
//				close(src)
//			}()
//			out, err := From(src).ToChan()
//			c.So(err, c.ShouldBeNil)
//			rs := chanToSlice(out)
//			c.So(rs, shouldSlicesResemble, tInts)
//		})
//	})
//}

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
