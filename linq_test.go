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
	"time"

	//"github.com/astaxie/beego/orm"
	"github.com/fanliao/go-promise"
)

const (
	countS           int = 20
	rptCountS        int = 22
	countP           int = 500
	rptCountP        int = 500
	arrForSequential     = 0
	arrForParallel       = 1
)

var (
	runTest    = true
	tUserss    = [][]interface{}{make([]interface{}, countS), make([]interface{}, countP)}
	tRptUserss = [][]interface{}{make([]interface{}, rptCountS), make([]interface{}, rptCountP)}
	tUserss2   = [][]interface{}{make([]interface{}, countS), make([]interface{}, countP)}
	tIntss     = [][]int{make([]int, countS), make([]int, countP)}
	tRoless    = [][]interface{}{make([]interface{}, countS), make([]interface{}, countP)}
	tEmptyss   = [][]int{[]int{}, []int{}}

	tUsers    = tUserss[arrForSequential]
	tRptUsers = tRptUserss[arrForSequential]
	tUsers2   = tUserss2[arrForSequential]
	tInts     = tIntss[arrForSequential]
	tRoles    = tRoless[arrForSequential]
)

func init() {
	runtime.GOMAXPROCS(numCPU)

	fillTestDatas := func(seq int) {
		size := len(tIntss[seq])
		rptSize := len(tRptUserss[seq])
		for i := 0; i < size; i++ {
			tIntss[seq][i] = i
			tUserss[seq][i], tRptUserss[seq][i] = user{i, "user" + strconv.Itoa(i)}, user{i, "user" + strconv.Itoa(i)}
			tUserss2[seq][i] = user{i + size/2, "user" + strconv.Itoa(i+size/2)}
		}
		for i := 0; i < rptSize-size; i++ {
			tRptUserss[seq][size+i] = user{i, "user" + strconv.Itoa(size+i)}
		}
		for i := 0; i < size/2; i++ {
			tRoless[seq][i*2] = role{i, "role" + strconv.Itoa(i)}
			tRoless[seq][i*2+1] = role{i, "role" + strconv.Itoa(i+1)}
		}
	}

	//full datas for testing sequential
	fillTestDatas(arrForSequential)
	//full datas for testing parallel
	fillTestDatas(arrForParallel)
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

type User struct {
	Id   int
	Name string `orm:"size(100)"`
}

// Testing functions----------------------------------------------------------
func TestFrom(t *testing.T) {
	//future testing
	//_ = orm
	//p := promise.NewPromise()
	//go func() {
	//	o := orm.NewOrm()
	//	u := User{Id: 1}
	//	if err := o.Read(&u); err != nil {
	//		p.Reject(err)
	//	} else {
	//		p.Resolve(u)
	//	}
	//}()

	//p.OnSuccess(func(r interface{}) {
	//	fmt.Println("Get user", r)
	//}).OnFailure(func(e interface{}) {
	//	fmt.Println("Get an error", e.(error))
	//}).OnComplete(func(e interface{}) {
	//	fmt.Println("Future completed")
	//})

	//u, err := p.Get()
	//_, _ = u, err

	//_ = promise.Start(func() (u interface{}, err error) {
	//	o := orm.NewOrm()
	//	u = User{Id: 1}
	//	err = o.Read(&u)
	//	return
	//})

	//task := func(canceller promise.Canceller) (r interface{}, err error) {
	//	i := 0
	//	for i < 50 {
	//		//检测Future是否已经被要求取消
	//		if canceller.IsCancellationRequested() {
	//			//如果已经被要求取消，那么设置Future为取消状态，中止任务的执行。
	//			//根据业务逻辑的要求，也可以直接调用canceller.Cancel()来设置Future为取消状态
	//			canceller.Cancel()
	//			return 0, nil
	//		}
	//		time.Sleep(100 * time.Millisecond)
	//	}
	//	return 1, nil
	//}
	//f := promise.Start(task).OnCancel(func() {
	//	fmt.Println("Future is cancelled")
	//})
	////要求取消Future任务的执行
	//f.RequestCancel()

	//r, err := f.Get() //return nil, promise.CANCELLED
	//fmt.Println(r, err)
	//fmt.Println(f.IsCancelled()) //print true

	//task := func(canceller promise.Canceller) (r interface{}, err error) {
	//	time.Sleep(300 * time.Millisecond)
	//	fmt.Println("Run done")
	//	return 0, nil
	//}

	//f := promise.Start(task).SetTimeout(100).OnCancel(func() {
	//	fmt.Println("Future is cancelled")
	//})

	//r, err := f.Get() //return nil, promise.CANCELLED
	//fmt.Println(r, err)
	//fmt.Println(f.IsCancelled()) //print true
	task := func() (r interface{}, err error) {
		time.Sleep(300 * time.Millisecond)
		return 0, nil
	}

	//设置100毫秒后超时，Future将自动被取消
	f := promise.Start(task)

	//Get将在100毫秒后超时
	r, _, timeout := f.GetOrTimeout(100) //return nil, promise.CANCELLED
	fmt.Println(timeout)                 //print true

	//Get将在300毫秒后超时，这次将返回task的结果
	r, _, timeout = f.GetOrTimeout(300) //return nil, promise.CANCELLED
	fmt.Println(r, timeout)             //print 0 false
	//----------------------------------------------------------
	if !runTest {
		return
	}
	c.Convey("When nil as data source, error returned", t, func() {
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

		for i := countS; i < countS+10; i++ {
			ints = append(ints, i)
		}
		rs, err := q.Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, countS+10)
	})

	c.Convey("Test SetDataSource", t, func() {
		q := NewQuery().Select(multiply10)

		c.Convey("When doesn't set data source, error returned", func() {
			_, err := q.Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Set data source for existed Queryable", func() {
			rs, err := q.SetDataSource(tIntss[arrForSequential]).Results()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, countS)
		})

		c.Convey("Set data source again for existed Queryable, get results again", func() {
			rs, err := q.SetDataSource(tIntss[arrForParallel]).Results()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, countP)
		})
	})
}

func TestCommonOperations(t *testing.T) {
	if !runTest {
		return
	}
	expectedInts := make([]interface{}, countS/2)
	for i := 0; i < countS/2; i++ {
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
		c.So(len(rs), c.ShouldEqual, countS)
		c.So(err, c.ShouldBeNil)
	})

	c.Convey("Union two user list", t, func() {
		rs, err := From(tUsers).Union(tUsers2).Results()
		c.So(len(rs), c.ShouldEqual, countS+countS/2)
		c.So(err, c.ShouldBeNil)
	})

	c.Convey("Skip int slice While item be less than count", t, func() {
		r, err := From(tInts).SkipWhile(func(v interface{}) bool {
			return v.(int) < countS
		}).Results()
		c.So(err, c.ShouldBeNil)
		c.So(r, shouldSlicesResemble, []interface{}{})
	})

	c.Convey("Compute the average value of []int", t, func() {
		r, err := From(tInts).Average()
		c.So(err, c.ShouldBeNil)
		c.So(r, c.ShouldEqual, float32(countS-1)/float32(2))
	})

	c.Convey("Execute two aggregate funcs once", t, func() {
		r, err := From(tInts).Aggregate(Max(), Min())
		c.So(err, c.ShouldBeNil)
		rs := r.([]interface{})
		c.So(rs[0], c.ShouldEqual, countS-1)
		c.So(rs[1], c.ShouldEqual, 0)
	})
}

func TestWhere(t *testing.T) {
	c.Convey("When passed nil function, error returned", t, func() {
		c.So(func() { From(tInts).Where(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in filter function", t,
		tIntss,
		NewQuery().Where(filterWithPanic),
		expectErr,
	)

	testLazyOpr("When error returned in previous operation", t,
		tIntss,
		NewQuery().Select(projectWithPanic).Where(isEven),
		expectErr,
	)

	testLazyOpr("Filter empty slice", t,
		tEmptyss,
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
	testLazyOpr("Filter []int", t,
		tIntss,
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
	testLazyOpr("Filter []interface{}", t,
		tUserss,
		NewQuery().Where(idIsEven),
		expectSlice(expectedUsers),
	)

}

func TestSelect(t *testing.T) {
	c.Convey("When passed nil function, error be returned", t, func() {
		c.So(func() { From(tInts).Select(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in passed project function", t,
		tIntss,
		NewQuery().Select(projectWithPanic),
		expectErr,
	)

	testLazyOpr("When error returned in previous operations", t,
		tIntss,
		NewQuery().Where(filterWithPanic).Select(multiply10),
		expectErr,
	)

	testLazyOpr("Select from empty", t,
		tEmptyss,
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
	testLazyOpr("Multiply 10 for each elements from []int or chan int", t,
		tIntss, NewQuery().Select(multiply10),
		expectSlice(expectedInts),
	)

	expectedUsers := func(n int) []interface{} {
		users := make([]interface{}, n)
		for i := 0; i < n; i++ {
			users[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		return users
	}
	testLazyOpr("Convert user to string from slice or channel", t,
		tUserss,
		NewQuery().Select(userToStr),
		expectSlice(expectedUsers),
	)
}

func TestSelectMany(t *testing.T) {
	c.Convey("When passed nil function, error returned", t, func() {
		c.So(func() { From(tInts).SelectMany(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	selectManyWithPanic := func(v interface{}) []interface{} {
		if v.(int) == countS-1 {
			panic("force an error")
		}
		return []interface{}{}
	}

	oneToTwo := func(v interface{}) []interface{} {
		outOfOrder()
		rs := make([]interface{}, 2)
		rs[0], rs[1] = v.(int)*10, v.(int)+countS
		return rs
	}

	expectedInts := func(n int) []interface{} {
		ints := make([]interface{}, n*2)
		for i := 0; i < n; i++ {
			ints[2*i], ints[2*i+1] = i*10, i+countS
		}
		return ints
	}

	testLazyOpr("When error returned in select function", t,
		tIntss,
		NewQuery().SelectMany(selectManyWithPanic),
		expectErr,
	)

	testLazyOpr("selectMany from empty", t,
		tEmptyss,
		NewQuery().SelectMany(selectManyWithPanic),
		expectEmptySlice,
	)

	testLazyOpr("selectMany from []int or chan int", t,
		tIntss,
		NewQuery().SelectMany(oneToTwo),
		expectSlice(expectedInts),
	)

}

func TestDistinct(t *testing.T) {
	c.Convey("When passed nil function, error returned", t, func() {
		c.So(func() { From(tInts).DistinctBy(nil) }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When error returned in passed DistinctBy function", t,
		tIntss,
		NewQuery().Select(projectWithPanic).DistinctBy(getUserId),
		expectErr,
	)

	testLazyOpr("When error returned in previous operations", t,
		tIntss,
		NewQuery().DistinctBy(projectWithPanic),
		expectErr,
	)

	testLazyOpr("Distinct empty", t,
		tEmptyss,
		NewQuery().Distinct(),
		expectEmptySlice,
	)

	testLazyOpr("Distinct []interface or chan interface{} by user id", t,
		tRptUserss,
		NewQuery().DistinctBy(getUserId),
		expectSliceSize(func(n int) int {
			if n == rptCountS {
				return countS
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
		tUserss,
		NewQuery().Select(projectWithPanic).GroupBy(groupUser),
		expectErr,
	)

	testLazyOpr("When error returned in GroupBy function", t,
		tUserss,
		NewQuery().GroupBy(projectWithPanic),
		expectErr,
	)

	testLazyOpr("Group empty", t,
		tEmptyss,
		NewQuery().GroupBy(projectWithPanic),
		expectEmptySlice,
	)

	ceil := func(a, b int) int {
		if a%b != 0 {
			return a/b + 1
		} else {
			return a / b
		}
	}
	testLazyOpr("Group []int or chan int", t,
		tIntss,
		NewQuery().GroupBy(func(v interface{}) interface{} {
			return v.(int) / 10
		}),
		expectSliceSize(func(n int) int {
			return ceil(n, 10)
		}),
	)

	testLazyOpr("Group slice or channel", t,
		tUserss,
		NewQuery().GroupBy(groupUser),
		expectSliceSize(func(n int) int {
			return ceil(n, 10)
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
		c.So(len(rs), c.ShouldEqual, countS)
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
		tUserss,
		NewQuery().Select(projectWithPanic).Join(tRoles, getUserId, getRoleUid, getUserIdAndRole),
		expectErr,
	)

	testLazyOpr("Empty Join slice or channel", t,
		tEmptyss,
		NewQuery().Join(tUsers2, getUserId, getRoleUid, getUserIdAndRole),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel Join empty", t,
		tUserss,
		NewQuery().Join([]interface{}{}, getUserId, getRoleUid, getUserIdAndRole),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel Join []interface{}", t,
		tUserss,
		NewQuery().Join(tRoles, getUserId, getRoleUid, getUserIdAndRole),
		expectOrdered,
	)

	testLazyOpr("slice or channel Join chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().Join(getChan(tRoles), getUserId, getRoleUid, getUserIdAndRole)
		},
		expectOrdered,
	)

	testLazyOpr("slice or channel LeftJoin an empty slice as inner source", t,
		tUserss,
		NewQuery().LeftJoin([]interface{}{}, getUserId, getRoleUid, leftResultSelector),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel LeftJoin []interface{}", t,
		tUserss,
		NewQuery().LeftJoin(tRoles, getUserId, getRoleUid, leftResultSelector),
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, n+countS/2)
			if !chanAsOut {
				checkOrder(rs)
			}
		})

	testLazyOpr("slice or channel LeftJoin chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().LeftJoin(getChan(tRoles), getUserId, getRoleUid, leftResultSelector)
		},
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, n+countS/2)
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

	testLazyOpr("Empty GroupJoin []interface{}", t,
		tEmptyss,
		NewQuery().GroupJoin(tUsers2, getUserId, getRoleUid, groupResultSelector),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel GroupJoin empty", t,
		tUserss,
		NewQuery().GroupJoin([]interface{}{}, getUserId, getRoleUid, groupResultSelector),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel GroupJoin []interface{}", t,
		tUserss,
		NewQuery().GroupJoin(tRoles, getUserId, getRoleUid, groupResultSelector),
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, countS/2)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

	testLazyOpr("slice or channel GroupJoin chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().GroupJoin(getChan(tRoles), getUserId, getRoleUid, groupResultSelector)
		},
		func(rs []interface{}, err error, n int, chanAsOut bool) {
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, countS/2)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

	testLazyOpr("Empty LeftGroupJoin empty", t,
		tEmptyss,
		NewQuery().LeftGroupJoin(tUsers2, getUserId, getRoleUid, groupResultSelector),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel LeftGroupJoin empty", t,
		tUserss,
		NewQuery().LeftGroupJoin([]interface{}{}, getUserId, getRoleUid, groupResultSelector),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel LeftGroupJoin []interface{}", t,
		tUserss,
		NewQuery().LeftGroupJoin(tRoles, getUserId, getRoleUid, groupResultSelector),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel LeftGroupJoin chan interface{}", t,
		tUserss,
		NewQuery().LeftGroupJoin(getChan(tRoles), getUserId, getRoleUid, groupResultSelector),
		expectSliceSizeEqualsN,
	)

}

func TestUnion(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Union(nil) }, c.ShouldPanicWith, ErrUnionNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		tUserss,
		NewQuery().Select(projectWithPanic).Union([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty union []interface{}", t,
		tEmptyss,
		NewQuery().Union(tUsers2),
		expectSliceSizeEquals(len(tUsers2)),
	)

	testLazyOpr("Slice or channel union empty", t,
		tUserss,
		NewQuery().Union([]interface{}{}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel union []interface{}", t,
		tUserss,
		NewQuery().Union(tUsers2),
		expectSliceSize(func(n int) int {
			if n == countS {
				return n + n/2
			} else {
				return n
			}
		}),
	)

	testLazyOpr("slice or channel union chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().Union(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			if n == countS {
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
		tUserss,
		NewQuery().Select(projectWithPanic).Concat([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty concat []interface{}", t,
		tEmptyss,
		NewQuery().Concat(tUsers2),
		expectSliceSizeEquals(countS),
	)

	testLazyOpr("slice or channel concat empty", t,
		tUserss,
		NewQuery().Concat([]interface{}{}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel concat []interface{}", t,
		tUserss,
		NewQuery().Concat(tUsers2),
		expectSliceSize(func(n int) int {
			return n + countS
		}),
	)

	testLazyOpr("slice or channel concat chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().Concat(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			return n + countS
		}),
	)

}

func TestInterest(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Intersect(nil) }, c.ShouldPanicWith, ErrInterestNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		tUserss,
		NewQuery().Select(projectWithPanic).Intersect([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty interest []interface{}", t,
		tEmptyss,
		NewQuery().Intersect(tUsers2),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel interest []interface{}", t,
		tUserss,
		NewQuery().Intersect([]interface{}{}),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel interest []interface{}", t,
		tUserss,
		NewQuery().Intersect(tUsers2),
		expectSliceSize(func(n int) int {
			if n == countS {
				return n / 2
			} else {
				return countS
			}
		}),
	)

	testLazyOpr("slice or channel interest chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().Intersect(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			if n == countS {
				return n / 2
			} else {
				return countS
			}
		}),
	)

}

func TestExcept(t *testing.T) {
	c.Convey("When passed nil source, error returned", t, func() {
		c.So(func() { From(tUsers).Except(nil) }, c.ShouldPanicWith, ErrExceptNilSource)
	})

	testLazyOpr("When error returned in previous operation", t,
		tUserss,
		NewQuery().Select(projectWithPanic).Except([]interface{}{}),
		expectErr,
	)

	testLazyOpr("Empty except []interface{}", t,
		tEmptyss,
		NewQuery().Except(tUsers2),
		expectEmptySlice,
	)

	testLazyOpr("slice or channel except empty", t,
		tUserss,
		NewQuery().Except([]interface{}{}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("slice or channel except []interface{}", t,
		tUserss,
		NewQuery().Except(tUsers2),
		expectSliceSize(func(n int) int {
			if n == countS {
				return n / 2
			} else {
				return n - countS
			}
		}),
	)

	testLazyOpr("slice or channel except chan interface{}", t,
		tUserss,
		func() *Queryable {
			return NewQuery().Except(getChan(tUsers2))
		},
		expectSliceSize(func(n int) int {
			if n == countS {
				return n / 2
			} else {
				return n - countS
			}
		}),
	)
}

func TestOrderBy(t *testing.T) {
	if !runTest {
		return
	}
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
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, len(tRptUsers))

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
		tUserss,
		NewQuery().Select(projectWithPanic).Reverse(),
		expectErr,
	)

	testLazyOpr("Reverse slice or channel", t,
		tUserss,
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
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Aggregate(nil)
			c.So(err, c.ShouldNotBeNil)

			_, err = q.Aggregate(([]*AggregateOperation{})...)
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("When error returned in Aggregate function", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Aggregate(&AggregateOperation{0, aggregatePanic, nil})
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("When error returned in previous operation", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Select(projectWithPanic).Aggregate(myAgg)
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("When source is empty, returned error", t,
		tEmptyss, NewQuery(),
		func(q *Queryable, n int) {
			_, err := q.Aggregate(Max())
			c.So(err, c.ShouldNotBeNil)
		})

	testImmediateOpr("Compute customized aggregate operation by []interface{}", t,
		tUserss, NewQuery(),
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

	testImmediateOpr("Get maximum int value", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n - 1)(q.Max())
		})

	testImmediateOpr("Get maximum user id", t,
		tUserss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n - 1)(q.Max(func(v interface{}) interface{} {
				return v.(user).id
			}))
		})

	testImmediateOpr("Get minimal int value", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(0)(q.Min())
		})

	testImmediateOpr("Get minimal user id", t,
		tUserss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(0)(q.Min(func(v interface{}) interface{} {
				return v.(user).id
			}))
		})

	testImmediateOpr("Get summary of all integer", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual((n - 1) * (n / 2))(q.Sum())
		})

	testImmediateOpr("Get summary of all user id", t,
		tUserss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual((n - 1) * (n / 2))(q.Sum(func(v interface{}) interface{} {
				return v.(user).id
			}))
		})

	testImmediateOpr("Get count of []int", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n)(q.Count())
		})

	testImmediateOpr("Get count of user which id is even", t,
		tUserss, NewQuery(),
		func(q *Queryable, n int) {
			expectEqual(n / 2)(q.Count(idIsEven))
		})

	testImmediateOpr("Get average of []int", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			r, err := q.Average()
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, float32(n-1)/float32(2))
		})

}

func TestAnyAndAll(t *testing.T) {
	//runTest = true
	ints := make([]interface{}, countS)
	for i := 0; i < countS; i++ {
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

	expectAllMatch := func(found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(found, c.ShouldEqual, true)
	}

	expectNotAllMatch := func(found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(found, c.ShouldEqual, false)
	}

	testImmediateOpr("When error returned in any function", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectErr(q.Any(func(v interface{}) bool {
				panic(errors.New("!error"))
			}))
		})

	testImmediateOpr("When error returned in previous operation", t,
		tIntss, NewQuery().Select(projectWithPanic),
		func(q *Queryable, n int) {
			expectErr(q.Any(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Any item == -1? no matched", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.Any(func(v interface{}) bool {
				return v.(int) == -1
			}))

		})

	testImmediateOpr("All item > 100000? no", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectNotAllMatch(q.All(func(v interface{}) bool {
				return v.(int) > 100000
			}))
		})

	testImmediateOpr("Any int == 12? yes", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectFound(q.Any(func(v interface{}) bool {
				return v.(int) == 12
			}))
		})

	testImmediateOpr("All int >= 0? yes", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectAllMatch(q.All(func(v interface{}) bool {
				return v.(int) >= 0
			}))
		})

	testImmediateOpr("All int >= 2? no", t,
		tIntss, NewQuery(),
		func(q *Queryable, n int) {
			expectNotAllMatch(q.All(func(v interface{}) bool {
				return v.(int) >= 2
			}))
		})
	//runTest = false

}

func TestSkipAndTake(t *testing.T) {
	c.Convey("When passed nil function in SkipWhile operation, error returned", t, func() {
		c.So(func() { From(tInts).SkipWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
	})

	c.Convey("When passed nil function in TakeWhile operation, error returned", t, func() {
		c.So(func() { From(tInts).TakeWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
	})

	testLazyOpr("When skip 0, all items returned", t,
		tIntss,
		NewQuery().Skip(0),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When skip all, empty returned", t,
		tIntss,
		NewQuery().Skip(10000),
		expectEmptySlice,
	)

	testLazyOpr("When skip 12", t,
		tIntss,
		NewQuery().Skip(12),
		expectSliceSize(func(n int) int {
			return n - 12
		}),
	)

	testLazyOpr("When error returned in predicate func, error returned", t,
		tIntss,
		NewQuery().SkipWhile(filterWithPanic),
		expectErr,
	)

	testLazyOpr("When skip while item < zero, all items returned", t,
		tIntss,
		NewQuery().SkipWhile(func(v interface{}) bool {
			return v.(int) < 0
		}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When skip while item < 10000, empty returned", t,
		tIntss,
		NewQuery().SkipWhile(func(v interface{}) bool {
			return v.(int) < 10000
		}),
		expectEmptySlice,
	)

	testLazyOpr("When skip while item % 50 < 12", t,
		tIntss,
		NewQuery().SkipWhile(func(v interface{}) bool {
			return v.(int)%50 < 12
		}),
		expectSliceSize(func(n int) int {
			return n - 12
		}),
	)

	testLazyOpr("When take 0, empty returned", t,
		tIntss,
		NewQuery().Take(0),
		expectEmptySlice,
	)

	testLazyOpr("When take 10000, all items returned", t,
		tIntss,
		NewQuery().Take(10000),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When take 12", t,
		tIntss,
		NewQuery().Take(12),
		expectSliceSizeEquals(12),
	)

	testLazyOpr("When error paniced in predicate func, error returned", t,
		tIntss,
		NewQuery().TakeWhile(filterWithPanic),
		expectErr,
	)

	testLazyOpr("When take while item < 0, empty returned", t,
		tIntss,
		NewQuery().TakeWhile(func(v interface{}) bool {
			return v.(int) < 0
		}),
		expectEmptySlice,
	)

	testLazyOpr("When take while item < 10000, all items returned", t,
		tIntss,
		NewQuery().TakeWhile(func(v interface{}) bool {
			return v.(int) < 10000
		}),
		expectSliceSizeEqualsN,
	)

	testLazyOpr("When take while item % 50 < 12", t,
		tIntss,
		NewQuery().TakeWhile(func(v interface{}) bool {
			return v.(int)%50 < 12
		}),
		expectSliceSizeEquals(12),
	)

	expectNotErr := func(rs []interface{}, err error, n int, chanAsOut bool) {
		c.So(err, c.ShouldBeNil)
	}

	testLazyOpr("Union and TakeWhile", t,
		tUserss,
		func() *Queryable {
			return NewQuery().Union(getChan(tUsers2)).TakeWhile(func(v interface{}) bool {
				return (v.(user).id)%50 < 12
			})
		},
		expectNotErr,
	)
}

func TestElementAt(t *testing.T) {
	testImmediateOpr("Find ElementAt -1? no, ", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			_, found, err := q.ElementAt(-1)
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})

	testImmediateOpr("Find ElementAt 10000? no", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			_, found, err := q.ElementAt(10000)
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})

	testImmediateOpr("Find ElementAt 12? yes", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			r, found, err := q.ElementAt(12)
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 12)
			c.So(found, c.ShouldEqual, true)
		})

}

func TestFirstByAndLastBy(t *testing.T) {
	expectErr := func(r interface{}, found bool, err error) {
		c.So(err, c.ShouldNotBeNil)
		c.So(found, c.ShouldEqual, false)
	}

	expectNotFound := func(r interface{}, found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(found, c.ShouldEqual, false)
	}

	expectBe14 := func(r interface{}, found bool, err error) {
		c.So(err, c.ShouldBeNil)
		c.So(r, c.ShouldEqual, 14)
		c.So(found, c.ShouldEqual, true)
	}

	testImmediateOpr("When error paniced in FirstBy func, error returned", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectErr(q.FirstBy(func(v interface{}) bool {
				panic(errors.New("!error"))
			}))
		})

	testImmediateOpr("When error paniced in previous operation, FirstBy return error", t,
		tIntss,
		NewQuery().Where(filterWithPanic),
		func(q *Queryable, n int) {
			expectErr(q.FirstBy(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Find FirstBy item == -1? no", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.FirstBy(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Find FirstBy item == 10000? no", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.FirstBy(func(v interface{}) bool {
				return v.(int) == 10000
			}))
		})

	testImmediateOpr("Find FirstBy item % 100 == 14? 14 returned", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectBe14(q.FirstBy(func(v interface{}) bool {
				return v.(int)%100 == 14
			}))
		})

	testImmediateOpr("Filter even then Find FirstBy item > 0 && item % 7 == 0, 14 returned", t,
		tIntss,
		NewQuery().Where(isEven),
		func(q *Queryable, n int) {
			expectBe14(q.FirstBy(func(v interface{}) bool {
				return v.(int) > 0 && v.(int)%7 == 0
			}))
		})

	testImmediateOpr("Find First 14? 14 returned", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectBe14(q.First(14))
		})

	testImmediateOpr("When error paniced in LastBy func, error returned", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectErr(q.LastBy(func(v interface{}) bool {
				panic(errors.New("!error"))
			}))
		})

	testImmediateOpr("When error paniced in previous operation, LastBy return error", t,
		tIntss,
		NewQuery().Where(filterWithPanic),
		func(q *Queryable, n int) {
			expectErr(q.LastBy(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Find LastBy item == -1? no", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.LastBy(func(v interface{}) bool {
				return v.(int) == -1
			}))
		})

	testImmediateOpr("Find LastBy item == 10000? no", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectNotFound(q.LastBy(func(v interface{}) bool {
				return v.(int) == 10000
			}))
		})

	testImmediateOpr("Find LastBy item < 20 && item % 7 == 0? 14 returned", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectBe14(q.LastBy(func(v interface{}) bool {
				return v.(int) < 20 && v.(int)%7 == 0
			}))
		})

	testImmediateOpr("Filter even then Find LastBy item < 15 && item % 2 == 0, 14 returned", t,
		tIntss,
		NewQuery().Where(isEven),
		func(q *Queryable, n int) {
			expectBe14(q.LastBy(func(v interface{}) bool {
				return v.(int) > 0 && v.(int) < 15 && v.(int)%2 == 0
			}))
		})

	testImmediateOpr("Find Last 14? 14 returned", t,
		tIntss,
		NewQuery(),
		func(q *Queryable, n int) {
			expectBe14(q.Last(14))
		})
}

//util functions for unit testing
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
				s1 := s[:]
				for _, v := range s1 {
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
			if val == countS-1 {
				_ = s[2]
			}
		case user:
			if val.id == countS-1 {
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

		if !runTest {
			return
		}
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

		if !runTest {
			return
		}
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
