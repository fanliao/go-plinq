package plinq

import (
	"errors"
	"fmt"
	c "github.com/smartystreets/goconvey/convey"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const (
	count     int = 20
	rptCount  int = 22
	countP    int = 300
	rptCountP int = 300
)

var (
	taUsers    = [][]interface{}{make([]interface{}, count), make([]interface{}, countP)}
	taRptUsers = [][]interface{}{make([]interface{}, rptCount), make([]interface{}, rptCountP)}
	taUsers2   = [][]interface{}{make([]interface{}, count), make([]interface{}, countP)}
	taInts     = [][]int{make([]int, count), make([]int, countP)}
	taRoles    = [][]interface{}{make([]interface{}, count), make([]interface{}, countP)}
	taEmptys   = [][]int{[]int{}, []int{}}

	tUsers    = taUsers[0]
	tRptUsers = taRptUsers[0]
	tUsers2   = taUsers2[0]
	tInts     = taInts[0]
	tRoles    = taRoles[0]
)

func init() {
	runtime.GOMAXPROCS(numCPU)

	fillTestDatas := func(seq int) {
		size := len(taInts[seq])
		rptSize := len(taRptUsers[seq])
		for i := 0; i < size; i++ {
			taInts[seq][i] = i
			taUsers[seq][i], taRptUsers[seq][i] = user{i, "user" + strconv.Itoa(i)}, user{i, "user" + strconv.Itoa(i)}
			taUsers2[seq][i] = user{i + size/2, "user" + strconv.Itoa(i+size/2)}
		}
		for i := 0; i < rptSize-size; i++ {
			taRptUsers[seq][size+i] = user{i, "user" + strconv.Itoa(size+i)}
		}
		for i := 0; i < size/2; i++ {
			taRoles[seq][i*2] = role{i, "role" + strconv.Itoa(i)}
			taRoles[seq][i*2+1] = role{i, "role" + strconv.Itoa(i+1)}
		}
	}

	//full datas for testing sequential
	fillTestDatas(0)
	//full datas for testing parallel
	fillTestDatas(1)
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
var (
	filterWithPanic = func(v interface{}) bool {
		panicInLast(v)
		return true
	}
	isEven = func(v interface{}) bool {
		outOfOrder()
		return v.(int)%2 == 0
	}
	idIsEven = func(v interface{}) bool {
		outOfOrder()
		return strconv.Itoa(v.(user).id%2) == "0"
	}

	projectWithPanic = func(v interface{}) interface{} {
		panicInLast(v)
		return v
	}
	multiply10 = func(v interface{}) interface{} {
		computerTask()
		return v.(int) * 10
	}
	userToStr = func(v interface{}) interface{} {
		computerTask()
		u := v.(user)
		return strconv.Itoa(u.id) + "/" + u.name
	}

	orderUserById = func(v1 interface{}, v2 interface{}) int {
		return defCompare(v1.(user).id, v2.(user).id)
	}

	getUserId = func(v interface{}) interface{} {
		return v.(user).id
	}
	getRoleUid = func(v interface{}) interface{} {
		return v.(role).uid
	}
	getUserIdAndRole = func(u interface{}, v interface{}) interface{} {
		return strconv.Itoa(u.(user).id) + "-" + v.(role).role
	}
)

// Testing functions----------------------------------------------------------
func TestFrom(t *testing.T) {
	c.Convey("Nil as data source", t, func() {
		c.So(func() { _ = From(nil) }, c.ShouldPanicWith, ErrNilSource)
		c.So(func() { _ = From(1) }, c.ShouldPanicWith, ErrUnsupportSource)
		var pSlice *[]interface{} = nil
		c.So(func() { _ = From(pSlice) }, c.ShouldPanicWith, ErrNilSource)
	})

	c.Convey("Test lazy execution", t, func() {
		ints := make([]int, len(tInts))
		_ = copy(ints, tInts)
		pSrc := &ints
		q := From(pSrc).Select(multiply10)

		for i := count; i < count+10; i++ {
			ints = append(ints, i)
		}
		rs, err := q.Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, count+10)
	})
}

func TestCommonOperations(t *testing.T) {
	expectedInts := make([]interface{}, count/2)
	for i := 0; i < count/2; i++ {
		expectedInts[i] = multiply10(i * 2).(int)
	}
	c.Convey("Get even elements from []int, multiply 10 for each even elements, and returns slice as output", t, func() {
		rs, err := From(tInts).Where(isEven).Select(multiply10).Results()
		c.So(err, c.ShouldBeNil)
		c.So(rs, shouldSlicesResemble, expectedInts)
	})

	c.Convey("Get even from elements chan int, multiply 10 for each even elements, and returns slice as output", t, func() {
		rs, err := From(getChan(tInts)).Where(isEven).Select(multiply10).Results()
		c.So(err, c.ShouldBeNil)
		c.So(rs, shouldSlicesResemble, expectedInts)
	})

	c.Convey("Get even from int channel, project to multiply 10, and return channel as output", t, func() {
		rsChan, errChan, err := From(getChan(tInts)).Where(isEven).Select(multiply10).ToChan()
		c.So(err, c.ShouldBeNil)
		rs, stepErr := getChanResult(rsChan, errChan)
		c.So(stepErr, c.ShouldBeNil)
		c.So(rs, shouldSlicesResemble, expectedInts)
	})

	c.Convey("Distinct user list by user id", t, func() {
		rs, err := From(tRptUsers).DistinctBy(getUserId).Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, len(tUsers))
	})

	c.Convey("Group []int by divide 10", t, func() {
		rs, err := From(tInts).GroupBy(func(v interface{}) interface{} {
			return v.(int) / 10
		}).Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, len(tInts)/10)
	})

	c.Convey("Order user list by user id", t, func() {
		rs, err := From(tRptUsers).OrderBy(orderUserById).Results()
		c.So(len(rs), c.ShouldEqual, len(tRptUsers))
		c.So(err, c.ShouldBeNil)

		id := 0
		for _, v := range rs {
			u := v.(user)
			c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
			id = u.id
		}
	})

	c.Convey("Join user list and role list base on user id, and return user id and role", t, func() {
		rs, err := From(tUsers).Join(tRoles, getUserId, getRoleUid, getUserIdAndRole).Results()
		c.So(len(rs), c.ShouldEqual, count)
		c.So(err, c.ShouldBeNil)
	})

	c.Convey("Union two user list", t, func() {
		rs, err := From(tUsers).Union(tUsers2).Results()
		c.So(len(rs), c.ShouldEqual, count+count/2)
		c.So(err, c.ShouldBeNil)
	})

	c.Convey("Skip int slice While item be less than count", t, func() {
		r, err := From(tInts).SkipWhile(func(v interface{}) bool {
			return v.(int) < count
		}).Results()
		c.So(err, c.ShouldBeNil)
		c.So(r, shouldSlicesResemble, []interface{}{})
	})

	c.Convey("Compute the average value of []int", t, func() {
		r, err := From(tInts).Average()
		c.So(err, c.ShouldBeNil)
		c.So(r, c.ShouldEqual, float32(count-1)/float32(2))
	})

	c.Convey("Execute two aggregate funcs once", t, func() {
		r, err := From(tInts).Aggregate(Max(), Min())
		c.So(err, c.ShouldBeNil)
		rs := r.([]interface{})
		c.So(rs[0], c.ShouldEqual, count-1)
		c.So(rs[1], c.ShouldEqual, 0)
	})
}

func TestWhere(t *testing.T) {
	c.Convey("When passed nil function, error returned", t, func() {
		c.So(func() { From(tInts).Where(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in filter function", t,
		taInts,
		NewQuery().Where(filterWithPanic),
		expectErr,
	)

	testLazyOpr("When error returned in previous operation", t,
		taInts,
		NewQuery().Select(projectWithPanic).Where(isEven),
		expectErr,
	)

	testLazyOpr("Filter an empty slice", t,
		taEmptys,
		NewQuery().Where(filterWithPanic),
		expectEmptySlice,
	)

	expectedInts := func(n int) []interface{} {
		expecteds := make([]interface{}, n/2)
		for i := 0; i < n/2; i++ {
			expecteds[i] = i * 2
		}
		return expecteds
	}
	testLazyOpr("Filter an []int", t,
		taInts,
		NewQuery().Where(isEven),
		expectSlice(expectedInts),
	)

	expectedUsers := func(n int) []interface{} {
		expecteds := make([]interface{}, n/2)
		for i := 0; i < n/2; i++ {
			expecteds[i] = user{i * 2, "user" + strconv.Itoa(i*2)}
		}
		return expecteds
	}
	testLazyOpr("Filter an []interface{}", t,
		taUsers,
		NewQuery().Where(idIsEven),
		expectSlice(expectedUsers),
	)

}

func TestSelect(t *testing.T) {
	c.Convey("When passed nil function, error be returned", t, func() {
		c.So(func() { From(tInts).Select(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in passed project function", t,
		taInts,
		NewQuery().Select(projectWithPanic),
		expectErr,
	)

	testLazyOpr("When error returned in previous operations", t,
		taInts,
		NewQuery().Where(filterWithPanic).Select(multiply10),
		expectErr,
	)

	testLazyOpr("Select from an empty slice", t,
		taEmptys,
		NewQuery().Where(filterWithPanic).Select(multiply10),
		expectEmptySlice,
	)

	expectedInts := func(n int) []interface{} {
		ints := make([]interface{}, n)
		for i := 0; i < n; i++ {
			ints[i] = multiply10(i).(int)
		}
		return ints
	}
	testLazyOpr("Multiply 10 for each elements in []int", t,
		taInts, NewQuery().Select(multiply10),
		expectSlice(expectedInts),
	)

	expectedUsers := func(n int) []interface{} {
		users := make([]interface{}, n)
		for i := 0; i < n; i++ {
			users[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		return users
	}
	testLazyOpr("Convert user to string from an []interface{}", t,
		taUsers,
		NewQuery().Select(userToStr),
		expectSlice(expectedUsers),
	)
}

func TestSelectMany(t *testing.T) {
	c.Convey("When passed nil function, error be returned", t, func() {
		c.So(func() { From(tInts).SelectMany(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	selectManyWithPanic := func(v interface{}) []interface{} {
		if v.(int) == count-1 {
			panic("force an error")
		}
		return []interface{}{}
	}

	oneToTwo := func(v interface{}) []interface{} {
		outOfOrder()
		rs := make([]interface{}, 2)
		rs[0], rs[1] = v.(int)*10, v.(int)+count
		return rs
	}

	expectedInts := func(n int) []interface{} {
		ints := make([]interface{}, n*2)
		for i := 0; i < n; i++ {
			ints[2*i], ints[2*i+1] = i*10, i+count
		}
		return ints
	}

	testLazyOpr("When error returned in select function", t,
		taInts,
		NewQuery().SelectMany(selectManyWithPanic),
		expectErr,
	)

	testLazyOpr("selectMany an empty slice", t,
		taEmptys,
		NewQuery().SelectMany(selectManyWithPanic),
		expectEmptySlice,
	)

	testLazyOpr("selectMany an int slice, and keep original order", t,
		taInts,
		NewQuery().SelectMany(oneToTwo),
		expectSlice(expectedInts),
	)

}

func TestDistinct(t *testing.T) {
	c.Convey("When passed nil function, error returned", t, func() {
		c.So(func() { From(tInts).DistinctBy(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in passed DistinctBy function", t,
		taInts,
		NewQuery().Select(projectWithPanic).DistinctBy(getUserId),
		expectErr,
	)

	testLazyOpr("When error returned in previous operations", t,
		taInts,
		NewQuery().DistinctBy(projectWithPanic),
		expectErr,
	)

	testLazyOpr("Distinct an empty slice", t,
		taEmptys,
		NewQuery().Distinct(),
		expectEmptySlice,
	)

	testLazyOpr("Distinct users by user id", t,
		taRptUsers,
		NewQuery().DistinctBy(getUserId),
		expectSliceSize(func(n int) int {
			if n == rptCount {
				return count
			} else {
				return countP
			}
		}),
	)
}

func TestGroupBy(t *testing.T) {
	groupUser := func(v interface{}) interface{} {
		return v.(user).id / 10
	}

	c.Convey("When passed nil function, error returned", t, func() {
		c.So(func() { From(tInts).GroupBy(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in previous operations", t,
		taUsers,
		NewQuery().Select(projectWithPanic).GroupBy(groupUser),
		expectErr,
	)

	testLazyOpr("When error returned in GroupBy function", t,
		taUsers,
		NewQuery().GroupBy(projectWithPanic),
		expectErr,
	)

	testLazyOpr("Group an empty slice", t,
		taEmptys,
		NewQuery().GroupBy(projectWithPanic),
		expectEmptySlice,
	)

	testLazyOpr("Group an []int", t,
		taInts,
		NewQuery().GroupBy(func(v interface{}) interface{} {
			return v.(int) / 10
		}),
		expectSliceSize(func(n int) int {
			return ceilChunkSize(n, 10)
		}),
	)

	testLazyOpr("group an []interface{}", t,
		taUsers,
		NewQuery().GroupBy(groupUser),
		expectSliceSize(func(n int) int {
			return ceilChunkSize(n, 10)
		}),
	)
}

//test functions for Join operation-------------------------------
func resultSelectorForConfusedOrder(u interface{}, v interface{}) interface{} {
	outOfOrder()
	return strconv.Itoa(u.(user).id) + "-" + v.(role).role
}

func leftResultSelector(u interface{}, v interface{}) interface{} {
	if v != nil {
		return strconv.Itoa(u.(user).id) + "-" + v.(role).role
	} else {
		return strconv.Itoa(u.(user).id)
	}
}

func TestJoin(t *testing.T) {
	SelectorPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	resultSelectorPanic := func(u interface{}, v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	checkOrder := func(rs []interface{}) {
		id := 0
		for _, v := range rs {
			uid, _ := strconv.Atoi(strings.TrimSpace(strings.Split(v.(string), "-")[0]))
			c.So(uid, c.ShouldBeGreaterThanOrEqualTo, id)
			id = uid
		}

	}
	expectOrdered := func(rs []interface{}, err error, n int, chanAsOut bool) {
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, count)
		if !chanAsOut {
			checkOrder(rs)
		}
	}

	c.Convey("When passed nil inner, error returned", t, func() {
		c.So(func() { From(tUsers).Join(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
		c.So(func() { From(tUsers).Join(tRoles, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
		c.So(func() { From(tUsers).Join(tUsers2, getUserId, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
		c.So(func() { From(tUsers).Join(tUsers2, getUserId, getRoleUid, nil) }, c.ShouldPanicWith, ErrResultSelector)
	})

	c.Convey("When error returned in Join functions", t, func() {
		_, err := From(tUsers).Join(tRoles, SelectorPanic, getRoleUid, getUserIdAndRole).Results()
		c.So(err, c.ShouldNotBeNil)

		_, err = From(tUsers).Join(tRoles, getUserId, SelectorPanic, getUserIdAndRole).Results()
		if err == nil {
			fmt.Println("\nJoin An error should be returned:", err)
		}
		c.So(err, c.ShouldNotBeNil)

		_, err = From(tUsers).Join(tRoles, getUserId, getRoleUid, resultSelectorPanic).Results()
		c.So(err, c.ShouldNotBeNil)
	})

	testLazyOpr("When error returned in previous operations", t,
		taUsers,
		NewQuery().Select(projectWithPanic).Join(tRoles, getUserId, getRoleUid, getUserIdAndRole),
		expectErr,
	)

	testLazyOpr("Join an empty slice as outer source", t,
		taEmptys,
		NewQuery().Join(tUsers2, getUserId, getRoleUid, getUserIdAndRole),
		expectEmptySlice,
	)

	testLazyOpr("Join an empty slice as inner source", t,
		taUsers,
		NewQuery().Join([]interface{}{}, getUserId, getRoleUid, getUserIdAndRole),
		expectEmptySlice,
	)

	testLazyOpr("Join an [][]interface{} as inner source", t,
		taUsers,
		NewQuery().Join(tRoles, getUserId, getRoleUid, getUserIdAndRole),
		expectOrdered,
	)

	testLazyOpr("Join an channel interface{} as inner source", t,
		taUsers,
		func() *Queryable {
			return NewQuery().Join(getChan(tRoles), getUserId, getRoleUid, getUserIdAndRole)
		},
		expectOrdered,
	)

	testLazyOpr("LeftJoin an empty slice as inner source", t,
		taUsers,
		NewQuery().LeftJoin([]interface{}{}, getUserId, getRoleUid, leftResultSelector),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("LeftJoin an []interface{} as inner source", t,
		taUsers,
		NewQuery().LeftJoin(tRoles, getUserId, getRoleUid, leftResultSelector),
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, n+count/2)
			if !chanAsOut {
				checkOrder(rs)
			}
		})

	testLazyOpr("LeftJoin an channel interface{} as inner source", t,
		taUsers,
		func() *Queryable {
			return NewQuery().LeftJoin(getChan(tRoles), getUserId, getRoleUid, leftResultSelector)
		},
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, n+count/2)
			if !chanAsOut {
				checkOrder(rs)
			}
		})

}

func TestGroupJoin(t *testing.T) {
	groupResultSelector := func(u interface{}, rs []interface{}) (r interface{}) {
		roles := make([]role, len(rs))
		for i, r := range rs {
			roles[i] = r.(role)
		}
		return &userRoles{u.(user), roles}
	}

	SelectorPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	resultSelectorPanic := func(u interface{}, v []interface{}) interface{} {
		panic(errors.New("panic"))
	}

	c.Convey("When passed nil inner, error returned", t, func() {
		c.So(func() { From(tUsers).GroupJoin(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
		c.So(func() { From(tUsers).GroupJoin(tRoles, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
		c.So(func() { From(tUsers).GroupJoin(tUsers2, getUserId, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
		c.So(func() {
			From(tUsers).GroupJoin(tUsers2, getUserId, getRoleUid, nil)
		}, c.ShouldPanicWith, ErrResultSelector)
	})

	c.Convey("When error returned in GroupJoin function", t, func() {
		_, err := From(tUsers).GroupJoin(tRoles, SelectorPanic, getRoleUid, groupResultSelector).Results()
		c.So(err, c.ShouldNotBeNil)

		_, err = From(tUsers).GroupJoin(tRoles, getUserId, SelectorPanic, groupResultSelector).Results()
		c.So(err, c.ShouldNotBeNil)

		_, err = From(tUsers).GroupJoin(tRoles, getUserId, getRoleUid, resultSelectorPanic).Results()
		c.So(err, c.ShouldNotBeNil)
	})

	testLazyOpr("GroupJoin an empty slice as outer source", t,
		taEmptys,
		NewQuery().GroupJoin(tUsers2, getUserId, getRoleUid, groupResultSelector),
		expectEmptySlice,
	)

	testLazyOpr("GroupJoin an empty slice as inner source", t,
		taUsers,
		NewQuery().GroupJoin([]interface{}{}, getUserId, getRoleUid, groupResultSelector),
		expectEmptySlice,
	)

	testLazyOpr("GroupJoin an []interface{} as inner source", t,
		taUsers,
		NewQuery().GroupJoin(tRoles, getUserId, getRoleUid, groupResultSelector),
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count/2)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

	testLazyOpr("GroupJoin an channel interface{} as inner source", t,
		taUsers,
		func() *Queryable {
			return NewQuery().GroupJoin(getChan(tRoles), getUserId, getRoleUid, groupResultSelector)
		},
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count/2)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

	testLazyOpr("LeftGroupJoin an empty slice as outer source", t,
		taEmptys,
		NewQuery().LeftGroupJoin(tUsers2, getUserId, getRoleUid, groupResultSelector),
		expectEmptySlice,
	)

	testLazyOpr("LeftGroupJoin an empty slice as inner source", t,
		taUsers,
		NewQuery().LeftGroupJoin([]interface{}{}, getUserId, getRoleUid, groupResultSelector),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("LeftGroupJoin an []interface{} as inner source", t,
		taUsers,
		NewQuery().LeftGroupJoin([]interface{}{}, getUserId, getRoleUid, groupResultSelector),
		expectSliceSizeEqualsN,
	)

}

func TestUnion(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Union(nil) }, c.ShouldPanicWith, ErrUnionNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		taUsers,
		NewQuery().Select(projectWithPanic).Union([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty union []interface{}", t,
		taEmptys,
		NewQuery().Union(tUsers2),
		expectSliceSizeEquals(len(tUsers2)),
	)

	testLazyOpr("Slice or channel union empty", t,
		taUsers,
		NewQuery().Union([]interface{}{}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel union []interface{}", t,
		taUsers,
		NewQuery().Union(tUsers2),
		expectSliceSize(func(n int) int {
			if n == count {
				return n + n/2
			} else {
				return n
			}
		}),
	)

	testLazyOpr("slice or channel union chan interface{}", t,
		taUsers,
		func() *Queryable {
			return NewQuery().Union(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			if n == count {
				return n + n/2
			} else {
				return n
			}
		}),
	)

}

func TestConcat(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Concat(nil) }, c.ShouldPanicWith, ErrConcatNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		taUsers,
		NewQuery().Select(projectWithPanic).Concat([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty concat []interface{}", t,
		taEmptys,
		NewQuery().Concat(tUsers2),
		expectSliceSizeEquals(count),
	)

	testLazyOpr("slice or channel concat empty", t,
		taUsers,
		NewQuery().Concat([]interface{}{}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel concat []interface{}", t,
		taUsers,
		NewQuery().Concat(tUsers2),
		expectSliceSize(func(n int) int {
			return n + count
		}),
	)

	testLazyOpr("slice or channel concat chan interface{}", t,
		taUsers,
		func() *Queryable {
			return NewQuery().Concat(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			return n + count
		}),
	)

}

func TestInterest(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Intersect(nil) }, c.ShouldPanicWith, ErrInterestNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		taUsers,
		NewQuery().Select(projectWithPanic).Intersect([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty interest []interface{}", t,
		taEmptys,
		NewQuery().Intersect(tUsers2),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel interest []interface{}", t,
		taUsers,
		NewQuery().Intersect([]interface{}{}),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel interest []interface{}", t,
		taUsers,
		NewQuery().Intersect(tUsers2),
		expectSliceSize(func(n int) int {
			if n == count {
				return n / 2
			} else {
				return count
			}
		}),
	)

	testLazyOpr("slice or channel interest chan interface{}", t,
		taUsers,
		func() *Queryable {
			return NewQuery().Intersect(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			if n == count {
				return n / 2
			} else {
				return count
			}
		}),
	)

}

func TestExcept(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Except(nil) }, c.ShouldPanicWith, ErrExceptNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		taUsers,
		NewQuery().Select(projectWithPanic).Except([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty except []interface{}", t,
		taEmptys,
		NewQuery().Except(tUsers2),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel except empty", t,
		taUsers,
		NewQuery().Except([]interface{}{}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel except []interface{}", t,
		taUsers,
		NewQuery().Except(tUsers2),
		expectSliceSize(func(n int) int {
			if n == count {
				return n / 2
			} else {
				return n - count
			}
		}),
	)

	testLazyOpr("slice or channel except chan interface{}", t,
		taUsers,
		func() *Queryable {
			return NewQuery().Except(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			if n == count {
				return n / 2
			} else {
				return n - count
			}
		}),
	)
}

func TestOrderBy(t *testing.T) {
	orderWithPanic := func(v1 interface{}, v2 interface{}) int {
		panic(errors.New("panic"))
	}

	c.Convey("Test Order", t, func() {
		c.Convey("When passed nil function, will use the default compare function", func() {
			rs, err := From([]int{4, 2, 3, 1}).OrderBy(nil).Results()
			c.So(rs, shouldSlicesResemble, []int{1, 2, 3, 4})
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("When error returned in OrderBy function", func() {
			_, err := From(tRptUsers).OrderBy(orderWithPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("When error returned in previous operation", func() {
			_, err := From(getChan(tRptUsers)).Select(projectWithPanic).OrderBy(orderUserById).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("OrderBy empty slice", func() {
			rs, err := From([]int{}).OrderBy(nil).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		checkIfOrdered := func(rs []interface{}, err error) {
			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
			c.So(err, c.ShouldBeNil)

			id := 0
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
				id = u.id
			}
		}
		c.Convey("OrderBy []interface{}", func() {
			checkIfOrdered(From(tRptUsers).OrderBy(orderUserById).Results())
		})

		c.Convey("OrderBy chan interface{}", func() {
			checkIfOrdered(From(getChan(tRptUsers)).OrderBy(orderUserById).Results())
		})
	})
}

func TestReverse(t *testing.T) {
	testLazyOpr("When error returned in previous operation", t,
		taUsers,
		NewQuery().Select(projectWithPanic).Reverse(),
		expectErr,
	)

	testLazyOpr("Reverse slice or channel", t,
		taUsers,
		NewQuery().OrderBy(orderUserById).Reverse(),
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(len(rs), c.ShouldEqual, n)
			c.So(err, c.ShouldBeNil)

			if !chanAsOut {
				id := 1000000
				for _, v := range rs {
					u := v.(user)
					c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
					id = u.id
				}
			}
		})

}

func aggregatePanic(v interface{}, summary interface{}) interface{} {
	panic(errors.New("panic"))
}
func TestAggregate(t *testing.T) {
	myAgg := &AggregateOperation{
		Seed: "",
		AggAction: func(v interface{}, t interface{}) interface{} {
			v1, t1 := v.(user), t.(string)
			return t1 + "|{" + strconv.Itoa(v1.id) + ":" + v1.name + "}"
		},
		ReduceAction: func(t1 interface{}, t2 interface{}) interface{} {
			return t1.(string) + t2.(string)
		},
	}

	testImmediateOpr("When passed nil function", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Aggregate(nil)
			c.So(err, c.ShouldNotBeNil)

			_, err = q.Aggregate(([]*AggregateOperation{})...)
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("When error returned in Aggregate function", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Aggregate(&AggregateOperation{0, aggregatePanic, nil})
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("When error returned in previous operation", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Select(projectWithPanic).Aggregate(myAgg)
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("When source is empty, returned error", t,
		taEmptys, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Aggregate(Max())
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("Compute customized aggregate operation by []interface{}", t,
		taUsers, NewQuery(),
		func(q *Queryable, n int) {
			r, err := q.Aggregate(myAgg)
			c.So(err, c.ShouldBeNil)
			_ = r
		})

}

func TestSumCountAvgMaxMin(t *testing.T) {
	expectEqual := func(expect int) func(r interface{}, err error) {
		return func(r interface{}, err error) {
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, expect)
		}
	}

	testImmediateOpr("Max an int slice", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n - 1)(q.Max())
		})

	testImmediateOpr("MaxBy an interface slice", t,
		taUsers, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n - 1)(q.Max(func(v interface{}) interface{} {
				return v.(user).id
			}))
		})

	testImmediateOpr("Min an interface slice", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(0)(q.Min())
		})

	testImmediateOpr("MinBy an interface slice", t,
		taUsers, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(0)(q.Min(func(v interface{}) interface{} {
				return v.(user).id
			}))
		})

	testImmediateOpr("Sum an interface slice", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual((n - 1) * (n / 2))(q.Sum())
		})

	testImmediateOpr("SumBy an interface slice", t,
		taUsers, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual((n - 1) * (n / 2))(q.Sum(func(v interface{}) interface{} {
				return v.(user).id
			}))
		})

	testImmediateOpr("Count an interface slice", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n)(q.Count())
		})

	testImmediateOpr("CountBy an interface slice", t,
		taUsers, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n / 2)(q.Count(idIsEven))
		})

	testImmediateOpr("Average an interface slice", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			r, err := q.Average()
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, float32(n-1)/float32(2))
		})

}

func TestAnyAndAll(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	expectErr := func(found bool, err error) {
		c.So(err, c.ShouldNotBeNil)
		c.So(found, c.ShouldEqual, false)
	}
	expectFound := func(found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(found, c.ShouldEqual, true)
	}

	expectNotFound := func(found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(found, c.ShouldEqual, false)
	}

	testImmediateOpr("Predicate with panic an error", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectErr(q.Any(func(v interface{}) bool {
				panic(errors.New("!error"))
			}))
		})

	testImmediateOpr("When an error returned in previous operation", t,
		taInts, NewQuery().Select(projectWithPanic),
		func(q *Queryable, n int) {
			expectErr(q.Any(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Any nothing", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.Any(func(v interface{}) bool {
				return v.(int) == -1
			}))

		})

	testImmediateOpr("All nothing", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.All(func(v interface{}) bool {
				r := v.(int) == -1
				return r
			}))
		})

	testImmediateOpr("Find any int == 12", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectFound(q.Any(func(v interface{}) bool {
				return v.(int) == 12
			}))
		})

	testImmediateOpr("Find any int >= 100000", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.Any(func(v interface{}) bool {
				return v.(int) >= 100000
			}))
		})

	testImmediateOpr("Find all int >= 0", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectFound(q.All(func(v interface{}) bool {
				return v.(int) >= 0
			}))
		})

	testImmediateOpr("Find all int >= 2", t,
		taInts, NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.All(func(v interface{}) bool {
				return v.(int) >= 2
			}))
		})

}

func TestSkipAndTake(t *testing.T) {
	c.Convey("SkipWhile with nil predicateFunc", t, func() {
		c.So(func() { From(tInts).SkipWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
	})

	c.Convey("TakeWhile with nil predicateFunc", t, func() {
		c.So(func() { From(tInts).SkipWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When skip nothing", t,
		taInts,
		NewQuery().Skip(-1),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When skip all", t,
		taInts,
		NewQuery().Skip(10000),
		expectEmptySlice,
	)

	testLazyOpr("When skip 12", t,
		taInts,
		NewQuery().Skip(12),
		expectSliceSize(func(n int) int {
			return n - 12
		}),
	)

	testLazyOpr("SkipWhile using a predicate func with panic error", t,
		taInts,
		NewQuery().SkipWhile(filterWithPanic),
		expectErr,
	)

	testLazyOpr("When skip while item be less than zero", t,
		taInts,
		NewQuery().SkipWhile(func(v interface{}) bool {
			return v.(int) < -1
		}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When skip while item be less than 10000", t,
		taInts,
		NewQuery().SkipWhile(func(v interface{}) bool {
			return v.(int) < 10000
		}),
		expectEmptySlice,
	)

	testLazyOpr("When skip while item mod 50 be less than 12", t,
		taInts,
		NewQuery().SkipWhile(func(v interface{}) bool {
			return v.(int)%50 < 12
		}),
		expectSliceSize(func(n int) int {
			return n - 12
		}),
	)

	testLazyOpr("When take nothing", t,
		taInts,
		NewQuery().Take(-1),
		expectEmptySlice,
	)

	testLazyOpr("When take all", t,
		taInts,
		NewQuery().Take(10000),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When take 12", t,
		taInts,
		NewQuery().Take(12),
		expectSliceSizeEquals(12),
	)

	testLazyOpr("TakeWhile using a predicate func with panic error", t,
		taInts,
		NewQuery().TakeWhile(filterWithPanic),
		expectErr,
	)

	testLazyOpr("When take while item be less than zero", t,
		taInts,
		NewQuery().TakeWhile(func(v interface{}) bool {
			return v.(int) < -1
		}),
		expectEmptySlice,
	)

	testLazyOpr("When take while item be less than 10000", t,
		taInts,
		NewQuery().TakeWhile(func(v interface{}) bool {
			return v.(int) < 10000
		}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When take while item mod 50 be less than 12", t,
		taInts,
		NewQuery().TakeWhile(func(v interface{}) bool {
			return v.(int)%50 < 12
		}),
		expectSliceSizeEquals(12),
	)
}

func TestElementAt(t *testing.T) {
	testImmediateOpr("ElementAt -1", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			_, found, err := q.ElementAt(-1)
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})

	testImmediateOpr("ElementAt 10000", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			_, found, err := q.ElementAt(10000)
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})

	testImmediateOpr("ElementAt 12", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			r, found, err := q.ElementAt(12)
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 12)
			c.So(found, c.ShouldEqual, true)
		})

}

func TestFirstBy(t *testing.T) {
	expectErr := func(r interface{}, found bool, err error) {
		c.So(err, c.ShouldNotBeNil)
		c.So(found, c.ShouldEqual, false)
	}

	expectNotFound := func(r interface{}, found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(found, c.ShouldEqual, false)
	}

	expectBe12 := func(r interface{}, found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(r, c.ShouldEqual, 12)
		c.So(found, c.ShouldEqual, true)
	}

	testImmediateOpr("FirstBy with panic an error", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			expectErr(q.FirstBy(func(v interface{}) bool {
				panic(errors.New("!error"))
			}))
		})

	testImmediateOpr("FirstBy item equals -1", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.FirstBy(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("FirstBy item equals 10000", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.FirstBy(func(v interface{}) bool {
				return v.(int) == 10000
			}))
		})

	testImmediateOpr("FirstBy item mod 100 equals 12", t,
		taInts,
		NewQuery(),
		func(q *Queryable, n int) {
			expectBe12(q.FirstBy(func(v interface{}) bool {
				return v.(int)%100 == 12
			}))
		})

	testImmediateOpr("When panic an error in previous operation", t,
		taInts,
		NewQuery().Where(filterWithPanic),
		func(q *Queryable, n int) {
			expectErr(q.FirstBy(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Filter then FirstBy item mod 100 equals 12", t,
		taInts,
		NewQuery().Where(isEven),
		func(q *Queryable, n int) {
			expectBe12(q.FirstBy(func(v interface{}) bool {
				return v.(int)%100 == 12
			}))
		})

	fmt.Println("廖恩正小朋友，该要念书刷牙洗澡澡睡觉觉啦！！！！！！不听话的小朋友没有蛋糕！！！！！！！！！！！！！！！！！！！")
}

func getChanResult(out chan interface{}, errChan chan error) (rs []interface{}, err error) {
	rs = make([]interface{}, 0, 1)
	for v := range out {
		rs = append(rs, v)
	}

	if e, ok := <-errChan; ok {
		err = e
	}
	return
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

var (
	//用来给测试的操作增加计算量
	computerTask = func() {
		for i := 0; i < 2; i++ {
			_ = strconv.Itoa(i)
		}
	}

	//随机运算，用于打乱次序
	outOfOrder = func() {
		rand.Seed(10)
		j := rand.Intn(50000)
		var sum float64 = 0
		for k := 0; k < j; k++ {
			sum += math.Cos(float64(k)) * math.Pi
		}
		_ = sum
	}

	getChan = func(src interface{}) interface{} {
		switch s := src.(type) {
		case []interface{}:
			chanSrc := make(chan interface{})
			go func() {
				for _, v := range s {
					chanSrc <- v
				}
				close(chanSrc)
			}()
			return chanSrc
		case []int:
			chanSrc := make(chan int)
			go func() {
				for _, v := range s {
					chanSrc <- v
				}
				close(chanSrc)
			}()
			return chanSrc
		default:
			return nil
		}
	}

	//panic when is last item
	panicInLast = func(v interface{}) {
		var s []interface{}
		switch val := v.(type) {
		case int:
			if val == count-1 {
				_ = s[2]
			}
		case user:
			if val.id == count-1 {
				_ = s[2]
			}
		}
	}

	//全面测试所有的linq操作，包括串行和并行两种模式-------------------------------
	//full testing the lazy executing operations,
	testLazyOpr = func(desc string, t *testing.T,
		srcs interface{},
		qry interface{},
		assert func([]interface{}, error, int, bool)) {
		defaultChunkSize = 20

		var getQry func() *Queryable
		if q, ok := qry.(func() *Queryable); ok {
			getQry = q
		} else if q, ok := qry.(*Queryable); ok {
			getQry = func() *Queryable {
				return q
			}
		}

		testResults := func(src interface{}, n int) {
			rs, err := getQry().SetDataSource(src).Results()
			assert(rs, err, n, false)
		}
		testToChan := func(src interface{}, n int) {
			rsChan, errChan, err := getQry().SetDataSource(src).ToChan()
			if err != nil {
				assert(nil, err, n, true)
				return
			}
			rs, err1 := getChanResult(rsChan, errChan)
			assert(rs, err1, n, true)
		}

		test := func(src interface{}) {
			n := reflect.ValueOf(src).Len()
			c.Convey("Test the slicer -> slicer", func() {
				testResults(src, n)
			})

			c.Convey("Test the channel -> slicer", func() {
				testResults(getChan(src), n)
			})

			c.Convey("Test the slicer -> channel", func() {
				testToChan(src, n)
			})

			c.Convey("Test the channel -> channel", func() {
				testToChan(getChan(src), n)
			})
		}

		v := reflect.ValueOf(srcs)
		c.Convey(desc, t, func() {
			c.Convey("in seq mode", func() {
				test(v.Index(0).Interface())
			})
		})
		c.Convey(desc, t, func() {
			c.Convey("in parallel mode", func() {
				test(v.Index(1).Interface())
			})
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}

	//testingthe immediate opretion
	testImmediateOpr = func(desc string, t *testing.T,
		srcs interface{},
		qry *Queryable,
		assert func(*Queryable, int)) {
		defaultChunkSize = 20

		test := func(src interface{}) {
			n := reflect.ValueOf(src).Len()
			c.Convey("Test the slicer -> slicer", func() {
				assert(qry.SetDataSource(src), n)
			})

			c.Convey("Test the channel -> slicer", func() {
				assert(qry.SetDataSource(getChan(src)), n)
			})

		}

		v := reflect.ValueOf(srcs)
		c.Convey(desc, t, func() {
			c.Convey("in seq mode", func() {
				test(v.Index(0).Interface())
			})
		})
		c.Convey(desc, t, func() {
			c.Convey("in parallel mode", func() {
				test(v.Index(1).Interface())
			})
		})
		defaultChunkSize = DEFAULTCHUNKSIZE

	}

	//functions for valid the lazy operation result
	expectErr = func(rs []interface{}, err error, n int, chanAsOut bool) {
		c.So(err, c.ShouldNotBeNil)
	}

	expectEmptySlice = func(rs []interface{}, err error, n int, chanAsOut bool) {
		c.So(len(rs), c.ShouldEqual, 0)
		c.So(err, c.ShouldBeNil)
	}

	expectSliceSize = func(size func(int) int) func(rs []interface{}, err error, n int, chanAsOut bool) {
		return func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, size(n))
		}
	}

	expectSliceSizeEquals = func(size int) func(rs []interface{}, err error, n int, chanAsOut bool) {
		return func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, size)
		}
	}

	expectSliceSizeEqualsN = func(rs []interface{}, err error, n int, chanAsOut bool) {
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, n)
	}

	expectSlice = func(expected func(n int) []interface{}) func(rs []interface{}, err error, n int, chanAsOut bool) {
		return func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			//NOTE: don't keep the original order when output the channel
			if chanAsOut {
				c.So(len(rs), c.ShouldEqual, len(expected(n)))
			} else {
				c.So(rs, shouldSlicesResemble, expected(n))
			}
		}
	}
)
