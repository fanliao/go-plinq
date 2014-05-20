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
	countP    int = 1000
	rptCountP int = 1000
)

var (
	maxProcs int

	taUsers    = [][]interface{}{make([]interface{}, count), make([]interface{}, countP)}
	taRptUsers = [][]interface{}{make([]interface{}, rptCount), make([]interface{}, rptCountP)}
	taUsers2   = [][]interface{}{make([]interface{}, count), make([]interface{}, countP)}
	taInts     = [][]int{make([]int, count), make([]int, countP)}
	taRoles    = [][]interface{}{make([]interface{}, count), make([]interface{}, countP)}

	tUsers    = taUsers[0]
	tRptUsers = taRptUsers[0]
	tUsers2   = taUsers2[0]
	tInts     = taInts[0]
	tRoles    = taRoles[0]

	tUserPs    = taUsers[1]
	tRptUserPs = taRptUsers[1]
	tUsersP2   = taUsers2[1]
	tIntPs     = taInts[1]
	tRolePs    = taRoles[1]

	tMap map[int]interface{} = make(map[int]interface{}, count)

	sequentialChunkSize int = count
	parallelChunkSize   int = count / 7
)

func init() {
	maxProcs = numCPU
	runtime.GOMAXPROCS(maxProcs)

	fullTestDatas := func(seq int) {
		size := len(taInts[seq])
		rptSize := len(taRptUsers[seq])
		for i := 0; i < size; i++ {
			taInts[seq][i] = i
			taUsers[seq][i] = user{i, "user" + strconv.Itoa(i)}
			taRptUsers[seq][i] = user{i, "user" + strconv.Itoa(i)}
			taUsers2[seq][i] = user{i + size/2, "user" + strconv.Itoa(i+size/2)}
			//tMap[seq][i] = user{i, "user" + strconv.Itoa(i)}
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
	fullTestDatas(0)

	//full datas for testing parallel
	fullTestDatas(1)

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
	//用来给测试的操作增加计算量
	computerTask = func() {
		for i := 0; i < 2; i++ {
			_ = strconv.Itoa(i)
		}
	}

	//随机运算，用于打乱次序
	confusedOrder = func() {
		rand.Seed(10)
		j := rand.Intn(1000000)
		var sum float64 = 0
		for k := 0; k < j; k++ {
			sum += math.Cos(float64(k)) * math.Pi
		}
		_ = sum
	}

	//转化slice of interface{} 为chan of interface{}
	getChan = func(src []interface{}) chan interface{} {
		chanSrc := make(chan interface{})
		go func() {
			for _, v := range src {
				chanSrc <- v
			}
			close(chanSrc)
		}()
		return chanSrc
	}

	//转化slice of int 为chan of int
	getIntChan = func(src []int) chan int {
		chanSrc := make(chan int)
		go func() {
			for _, v := range src {
				chanSrc <- v
			}
			close(chanSrc)
		}()
		return chanSrc
	}

	//将在最后一个数据抛出错误的过滤函数，用于测试错误处理
	filterWithPanic = func(v interface{}) bool {
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
		return true
	}

	//测试用的过滤函数
	filterFunc = func(v interface{}) bool {
		computerTask()
		switch val := v.(type) {
		case int:
			return val%2 == 0
		case user:
			return strconv.Itoa(val.id%2) == "0"
		}
		panic(errors.New("error!"))
	}

	//测试用的过滤函数，加入随机运算来打乱返回的顺序
	filterFuncForConfusedOrder = func(v interface{}) bool {
		i := v.(int)
		confusedOrder()
		return i%2 == 0
	}

	//将抛出错误的转换函数，用于测试错误处理
	projectWithPanic = func(v interface{}) interface{} {
		switch val := v.(type) {
		case int:
			if val == count-1 {
				var s []interface{}
				_ = s[2]
			}
		case user:
			if val.id == count-1 {
				var s []interface{}
				_ = s[2]
			}
		}
		return v
	}

	//测试用的转换函数
	projectFunc = func(v interface{}) interface{} {
		computerTask()
		switch val := v.(type) {
		case int:
			return val * 10
		case user:
			return strconv.Itoa(val.id) + "/" + val.name
		}
		panic(errors.New(fmt.Sprintf("error!, %v", v)))
	}

	distinctUser = func(v interface{}) interface{} {
		u := v.(user)
		return u.id
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
		q := From(pSrc).Select(projectFunc)

		for i := count; i < count+10; i++ {
			ints = append(ints, i)
		}
		rs, err := q.Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, count+10)
	})
}

//对常见操作进行基本的测试
func TestBasicOperations(t *testing.T) {
	expectedInts := make([]interface{}, count/2)
	for i := 0; i < count/2; i++ {
		expectedInts[i] = i * 2 * 10
	}
	c.Convey("Test where then select the slice of int", t, func() {
		rs, err := From(tInts).Where(filterFunc).Select(projectFunc).Results()
		c.So(err, c.ShouldBeNil)
		c.So(rs, shouldSlicesResemble, expectedInts)
	})

	c.Convey("Test where then select the channel of int", t, func() {
		rs, err := From(getIntChan(tInts)).Where(filterFunc).Select(projectFunc).Results()
		c.So(err, c.ShouldBeNil)
		c.So(rs, shouldSlicesResemble, expectedInts)
	})

	c.Convey("Test where then select, and use channel as output", t, func() {
		rsChan, errChan, err := From(getIntChan(tInts)).Where(filterFunc).Select(projectFunc).ToChan()
		c.So(err, c.ShouldBeNil)
		rs, stepErr := getChanResult(rsChan, errChan)
		c.So(stepErr, c.ShouldBeNil)
		c.So(rs, shouldSlicesResemble, expectedInts)
	})

	c.Convey("DistinctBy a slice of interface{}", t, func() {
		rs, err := From(tRptUsers).DistinctBy(distinctUser).Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, len(tUsers))
	})

	c.Convey("groupBy a slice of int", t, func() {
		rs, err := From(tInts).GroupBy(func(v interface{}) interface{} {
			return v.(int) / 10
		}).Results()
		c.So(err, c.ShouldBeNil)
		c.So(len(rs), c.ShouldEqual, len(tInts)/10)
	})

	c.Convey("OrderBy a slice of interface{} ", t, func() {
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

	c.Convey("Join a slice of interface{} as inner source", t, func() {
		rs, err := From(tUsers).Join(tRoles, userSelector, roleSelector, resultSelector).Results()
		c.So(len(rs), c.ShouldEqual, count)
		c.So(err, c.ShouldBeNil)
	})

	c.Convey("Union a  slice of interface{} as secondary source", t, func() {
		rs, err := From(tUsers).Union(tUsers2).Results()
		c.So(len(rs), c.ShouldEqual, count+count/2)
		c.So(err, c.ShouldBeNil)
	})

	c.Convey("SkipWhile all", t, func() {
		r, err := From(tInts).SkipWhile(func(v interface{}) bool {
			return v.(int) < count
		}).Results()
		c.So(err, c.ShouldBeNil)
		c.So(r, shouldSlicesResemble, []interface{}{})
	})

	c.Convey("Average a slice of int", t, func() {
		r, err := From(tInts).Average()
		c.So(err, c.ShouldBeNil)
		c.So(r, c.ShouldEqual, float32(count-1)/float32(2))
	})

	c.Convey("Executes two aggregate funcs once", t, func() {
		r, err := From(tInts).Aggregate(Max(), Min())
		c.So(err, c.ShouldBeNil)
		rs := r.([]interface{})
		c.So(rs[0], c.ShouldEqual, count-1)
		c.So(rs[1], c.ShouldEqual, 0)
	})
}

//全面测试所有的linq操作，包括串行和并行两种模式-------------------------------
//testingthe opretion returns the collecion
func testPlinqLazyOpr(srcs interface{},
	opr func(q *Queryable) *Queryable,
	assert func([]interface{}, error)) {

	getC := func(src interface{}) interface{} {
		switch s := src.(type) {
		case []interface{}:
			return getChan(s)
		case []int:
			return getIntChan(s)
		default:
			return nil
		}
	}

	test := func(desc string, src interface{}) {
		fmt.Println("Test the data~~~~~~~~~~~~~~~", src)
		//c.Convey(desc, func() {
		//fmt.Println("Test the data====", src)
		c.Convey("Test the slicer -> slicer", func() {
			fmt.Println("Test the slicer -> slicer", src)
			rs, err := opr(From(src)).Results()
			assert(rs, err)
		})

		c.Convey("Test the channel -> slicer", func() {
			fmt.Println("Test the channel -> slicer", src)
			rs, err := opr(From(getC(src))).Results()
			assert(rs, err)
		})

		c.Convey("Test the slicer -> channel", func() {
			fmt.Println("Test the slicer -> channel", src)
			rsChan, errChan, err := opr(From(src)).ToChan()
			if err != nil {
				assert(nil, err)
				return
			}
			rs, err := getChanResult(rsChan, errChan)
			assert(rs, err)
		})

		c.Convey("Test the channel -> channel", func() {
			fmt.Println("Test the channel -> channel", src)
			rsChan, errChan, err := opr(From(getC(src))).ToChan()
			if err != nil {
				assert(nil, err)
				return
			}
			rs, err := getChanResult(rsChan, errChan)
			assert(rs, err)
		})
		//})
	}

	switch ss := srcs.(type) {
	case [][]int:
		c.Convey("Test seq", func() {
			//	fmt.Println("Test seq")
			test("Test seq~~~~~~", ss[0])
			fmt.Println("seq done!!!!!!!!!!!!!!!")
		})
		c.Convey("Test parallel", func() {
			//	fmt.Println("Test parallel")
			test("Test parallel", ss[1])
		})
	case [][]interface{}:
		//c.Convey("Test seq", func() {
		test("Test seq", ss[0])
		//})
		//c.Convey("Test Parallel", func() {
		test("Test Parallel", ss[1])
		//})
	}
}

func TestWhere(t *testing.T) {
	expectedInts := make([]interface{}, count/2)
	for i := 0; i < count/2; i++ {
		expectedInts[i] = i * 2
	}
	expectedUsers := make([]interface{}, count/2)
	for i := 0; i < count/2; i++ {
		expectedUsers[i] = user{i * 2, "user" + strconv.Itoa(i*2)}
	}

	c.Convey("If the error appears in where function from list source", t, func() {
		testPlinqLazyOpr(taInts, func(q *Queryable) *Queryable {
			return q.Where(filterWithPanic)
		}, func(rs []interface{}, err error) {
			c.So(err, c.ShouldNotBeNil)
		})
	})

	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(tInts).Where(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("If the error appears in where function from list source", func() {
			_, err := From(tInts).Where(filterWithPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("If the error appears in where function from channel source", func() {
			_, err := From(getIntChan(tInts)).Where(filterWithPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("If the error appears in before operation", func() {
			_, err := From(getIntChan(tInts)).Select(projectWithPanic).Where(filterFunc).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Filter an empty slice", func() {
			rs, err := From([]int{}).Where(filterWithPanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Filter an int slice", func() {
			rs, err := From(tInts).Where(filterFunc).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, expectedInts)
		})

		c.Convey("Filter an int slice, and keep original order", func() {
			rs, err := From(tInts).Where(filterFuncForConfusedOrder).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, expectedInts)
		})

		c.Convey("Filter an interface{} slice", func() {
			rs, err := From(tUsers).Where(filterFunc).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, expectedUsers)
		})

		//TODO: still have bugs
		//c.Convey("Filter a map", func() {
		//	rs, err := From(tMap).Where(filterMap).Results()
		//	c.So(len(rs), c.ShouldEqual, count/2)
		//	c.So(err, c.ShouldBeNil)
		//})

		c.Convey("Filter an interface{} channel", func() {
			rs, err := From(getChan(tUsers)).Where(filterFunc).Results()
			c.So(rs, shouldSlicesResemble, expectedUsers)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Filter an int channel", func() {
			rs, err := From(getIntChan(tInts)).Where(filterFunc).Results()
			c.So(rs, shouldSlicesResemble, expectedInts)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Filter an int channel, and keep original order", func() {
			rs, err := From(getIntChan(tInts)).Where(filterFuncForConfusedOrder).Results()
			c.So(rs, shouldSlicesResemble, expectedInts)
			c.So(err, c.ShouldBeNil)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}

	//设置chunk size==count，测试串行模式
	c.Convey("Test Where Sequential", t, func() { test(sequentialChunkSize) })

	//设置chunk size<count，测试并行模式
	c.Convey("Test Where parallel", t, func() { test(parallelChunkSize) })
}

func TestSelect(t *testing.T) {
	//插入随机的计算来打乱原始的顺序，测试结果是否可以保持顺序
	selectIntForConfusedOrder := func(v interface{}) interface{} {
		rand.Seed(10)
		confusedOrder()
		return v.(int) * 10
	}

	test := func(size int) {
		defaultChunkSize = size

		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(tInts).Select(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("If the error appears in select function", func() {
			_, err := From(tInts).Select(projectWithPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("If the error appears in before operations", func() {
			_, err := From(getIntChan(tInts)).Where(filterWithPanic).Select(projectFunc).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Select an empty slice", func() {
			rs, err := From([]int{}).Select(projectWithPanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		newInts := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newInts[i] = i * 10
		}
		c.Convey("select an int slice", func() {
			rs, err := From(tInts).Select(projectFunc).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("select an int slice, and keep original order", func() {
			rs, err := From(tInts).Select(selectIntForConfusedOrder).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		newUsers := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		c.Convey("Select an interface{} slice", func() {
			rs, err := From(tUsers).Select(projectFunc).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, newUsers)
		})

		c.Convey("Select an interface{} channel", func() {
			rs, err := From(getChan(tUsers)).Select(projectFunc).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, newUsers)
		})

		c.Convey("Select an int channel", func() {
			rs, err := From(getIntChan(tInts)).Select(projectFunc).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, newInts)
		})

		c.Convey("Select an int channel, and keep original order", func() {
			rs, err := From(getIntChan(tInts)).Select(selectIntForConfusedOrder).Results()
			c.So(err, c.ShouldBeNil)
			c.So(rs, shouldSlicesResemble, newInts)
		})

		defaultChunkSize = DEFAULTCHUNKSIZE
	}

	c.Convey("Test Select Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Select parallel", t, func() { test(parallelChunkSize) })
}

func TestSelectMany(t *testing.T) {
	selectManyWithPanic := func(v interface{}) []interface{} {
		if v.(int) == count-1 {
			var s []interface{}
			_ = s[2]
		}
		return []interface{}{}
	}

	selectIntMany := func(v interface{}) []interface{} {
		rs := make([]interface{}, 2)
		rs[0] = v.(int) * 10
		rs[1] = v.(int) + count
		return rs
	}

	selectIntManyForConfusedOrder := func(v interface{}) []interface{} {
		rand.Seed(10)
		confusedOrder()
		rs := make([]interface{}, 2)
		rs[0] = v.(int) * 10
		rs[1] = v.(int) + count
		return rs
	}

	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(tInts).SelectMany(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in select function", func() {
			_, err := From(tInts).SelectMany(selectManyWithPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("selectMany an empty slice", func() {
			rs, err := From([]int{}).SelectMany(selectManyWithPanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		newInts := make([]interface{}, count*2)
		for i := 0; i < count; i++ {
			newInts[2*i] = i * 10
			newInts[2*i+1] = i + count
		}
		c.Convey("selectMany an int slice", func() {
			rs, err := From(tInts).SelectMany(selectIntMany).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("selectMany an int slice, and keep original order", func() {
			rs, err := From(tInts).SelectMany(selectIntManyForConfusedOrder).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("selectMany an int channel", func() {
			rs, err := From(getIntChan(tInts)).SelectMany(selectIntMany).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("selectMany an int channel, and keep original order", func() {
			rs, err := From(getIntChan(tInts)).SelectMany(selectIntManyForConfusedOrder).Results()
			c.So(rs, shouldSlicesResemble, newInts)
			c.So(err, c.ShouldBeNil)
		})

		defaultChunkSize = DEFAULTCHUNKSIZE
	}

	c.Convey("Test selectMany Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test selectMany parallel", t, func() { test(parallelChunkSize) })
}

func distinctUserPanic(v interface{}) interface{} {
	var s []interface{}
	_ = s[2]
	u := v.(user)
	return u.id
}

func TestDistinct(t *testing.T) {
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(tInts).DistinctBy(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("An error should be returned if the error appears in DistinctBy function", func() {
			_, err := From(tRptUsers).DistinctBy(distinctUserPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("If the error appears in before operations", func() {
			_, err := From(getChan(tRptUsers)).Select(projectWithPanic).DistinctBy(distinctUser).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Distinct an empty slice", func() {
			rs, err := From([]int{}).Distinct().Results()
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
			rs, err := From(rptInts).Distinct().Results()
			c.So(len(rs), c.ShouldEqual, len(tInts))
			c.So(err, c.ShouldBeNil)
		})

		newUsers := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		c.Convey("DistinctBy an interface{} slice", func() {
			rs, err := From(tRptUsers).DistinctBy(distinctUser).Results()
			c.So(len(rs), c.ShouldEqual, len(tUsers))
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Distinct an interface{} slice", func() {
			rs, err := From(tRptUsers).Distinct().Results()
			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Distinct an interface{} channel", func() {
			rs, err := From(getChan(tRptUsers)).DistinctBy(distinctUser).Results()
			c.So(len(rs), c.ShouldEqual, len(tUsers))
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Distinct an int channel", func() {
			rs, err := From(getIntChan(rptInts)).Distinct().Results()
			c.So(len(rs), c.ShouldEqual, len(tInts))
			c.So(err, c.ShouldBeNil)
		})

		defaultChunkSize = DEFAULTCHUNKSIZE
	}

	c.Convey("Test Distinct Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Distinct parallel", t, func() { test(parallelChunkSize) })
}

func TestGroupBy(t *testing.T) {
	groupUser := func(v interface{}) interface{} {
		return v.(user).id / 10
	}
	groupUserPanic := func(v interface{}) interface{} {
		panic(errors.New("panic"))
	}
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil function, error be returned", func() {
			c.So(func() { From(tInts).GroupBy(nil) }, c.ShouldPanicWith, ErrNilAction)
		})

		c.Convey("If the error appears in before operations", func() {
			_, err := From(getChan(tUsers)).Select(projectWithPanic).GroupBy(groupUser).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("An error should be returned if the error appears in GroupBy function", func() {
			_, err := From(tRptUsers).GroupBy(groupUserPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("GroupBy an empty slice", func() {
			rs, err := From([]int{}).GroupBy(groupUserPanic).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("groupBy an int slice", func() {
			rs, err := From(tInts).GroupBy(func(v interface{}) interface{} {
				return v.(int) / 10
			}).Results()
			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(tInts), 10))
			c.So(err, c.ShouldBeNil)
		})

		newUsers := make([]interface{}, count)
		for i := 0; i < count; i++ {
			newUsers[i] = strconv.Itoa(i) + "/" + "user" + strconv.Itoa(i)
		}
		c.Convey("groupBy an interface{} slice", func() {
			rs, err := From(tUsers).GroupBy(groupUser).Results()
			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(tUsers), 10))
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("groupBy an interface{} channel", func() {
			rs, err := From(getChan(tUsers)).GroupBy(groupUser).Results()
			c.So(len(rs), c.ShouldEqual, ceilChunkSize(len(tUsers), 10))
			c.So(err, c.ShouldBeNil)
		})

		defaultChunkSize = DEFAULTCHUNKSIZE
	}

	c.Convey("Test groupBy Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test groupBy parallel", t, func() { test(parallelChunkSize) })
}

//test functions for Join operation-------------------------------
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
		defaultChunkSize = size
		c.Convey("When passed nil inner, error be returned", func() {
			c.So(func() { From(tUsers).Join(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
			c.So(func() { From(tUsers).Join(tRoles, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
			c.So(func() { From(tUsers).Join(tUsers2, userSelector, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
			c.So(func() { From(tUsers).Join(tUsers2, userSelector, roleSelector, nil) }, c.ShouldPanicWith, ErrResultSelector)
		})

		c.Convey("An error should be returned if the error appears in Join function", func() {
			_, err := From(tUsers).Join(tRoles, userSelectorPanic, roleSelector, resultSelector).Results()
			c.So(err, c.ShouldNotBeNil)

			_, err = From(tUsers).Join(tRoles, userSelector, roleSelectorPanic, resultSelector).Results()
			if err == nil {
				fmt.Println("\nJoin An error should be returned:", err)
			}
			c.So(err, c.ShouldNotBeNil)

			_, err = From(tUsers).Join(tRoles, userSelector, roleSelector, resultSelectorPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("If the error appears in before operations", func() {
			_, err := From(getChan(tUsers)).Select(projectWithPanic).Join(tRoles, userSelector, roleSelector, resultSelector).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Join an empty slice as outer source", func() {
			rs, err := From([]int{}).Join(tUsers2, userSelector, roleSelector, resultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Join an empty slice as inner source", func() {
			rs, err := From(tUsers).Join([]interface{}{}, userSelector, roleSelector, resultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Join an interface{} slice as inner source", func() {
			rs, err := From(tUsers).Join(tRoles, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

		expectedJoinRs := make([]interface{}, count)
		for i := 0; i < count/2; i++ {
			expectedJoinRs[2*i] = strconv.Itoa(i) + "-role" + strconv.Itoa(i)
			expectedJoinRs[2*i+1] = strconv.Itoa(i) + "-role" + strconv.Itoa(i+1)
		}

		//Test keep original order
		c.Convey("Join an interface{} slice as inner source, and keep original order", func() {
			rs, err := From(tUsers).Join(tRoles, userSelector, roleSelector, resultSelectorForConfusedOrder).Results()
			//TODO: need test KeepOrder()
			//c.So((rs), shouldSlicesResemble, expectedJoinRs)
			c.So(len(rs), c.ShouldEqual, len(expectedJoinRs))
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

		c.Convey("Join an interface{} channel as outer source", func() {
			rs, err := From(getChan(tUsers)).Join(tRoles, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

		c.Convey("Join an interface{} channel as inner source", func() {
			rs, err := From(tUsers).Join(getChan(tRoles), userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})

		//Test keep original order
		c.Convey("Join an interface{} channel as inner source, and keep original order", func() {
			rs, err := From(tUsers).Join(getChan(tRoles), userSelector, roleSelector, resultSelectorForConfusedOrder).Results()
			//cannot keep the original order of inner
			//c.So((rs), shouldSlicesResemble, expectedJoinRs)
			c.So(len(rs), c.ShouldEqual, len(expectedJoinRs))
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Join Sequential", t, func() { testJoin(30) })
	c.Convey("Test Join parallel", t, func() { testJoin(7) })

	testLeftJoin := func(size int) {
		defaultChunkSize = size
		c.Convey("LeftJoin an empty slice as outer source", func() {
			rs, err := From([]int{}).LeftJoin(tUsers2, userSelector, roleSelector, resultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftJoin an empty slice as inner source, but doesn't check if the inner is nil", func() {
			rs, err := From(tUsers).LeftJoin([]interface{}{}, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("LeftJoin an interface{} slice as inner source, but doesn't check if the inner is nil", func() {
			rs, err := From(tUsers).LeftJoin(tRoles, userSelector, roleSelector, resultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(rs, c.ShouldBeNil)
			c.So(err, c.ShouldNotBeNil)
			if err != nil {
				//TODO:need check the error message (no error stack)
				//fmt.Println("LeftJoin doesn't check if the inner is nil, get Err:", err)
			}
		})

		c.Convey("LeftJoin an empty slice as inner source", func() {
			rs, err := From(tUsers).LeftJoin([]interface{}{}, userSelector, roleSelector, leftResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			checkOrder(rs)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftJoin an interface{} slice as inner source", func() {
			rs, err := From(tUsers).LeftJoin(tRoles, userSelector, roleSelector, leftResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
			checkOrder(rs)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
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
		defaultChunkSize = size
		c.Convey("When passed nil inner, error be returned", func() {
			c.So(func() { From(tUsers).GroupJoin(nil, nil, nil, nil) }, c.ShouldPanicWith, ErrJoinNilSource)
			c.So(func() { From(tUsers).GroupJoin(tRoles, nil, nil, nil) }, c.ShouldPanicWith, ErrOuterKeySelector)
			c.So(func() { From(tUsers).GroupJoin(tUsers2, userSelector, nil, nil) }, c.ShouldPanicWith, ErrInnerKeySelector)
			c.So(func() {
				From(tUsers).GroupJoin(tUsers2, userSelector, roleSelector, nil)
			}, c.ShouldPanicWith, ErrResultSelector)
		})

		c.Convey("An error should be returned if the error appears in GroupJoin function", func() {
			_, err := From(tUsers).GroupJoin(tRoles, userSelectorPanic, roleSelector, groupResultSelector).Results()
			c.So(err, c.ShouldNotBeNil)

			rs, err := From(tUsers).GroupJoin(tRoles, userSelector, roleSelectorPanic, groupResultSelector).Results()
			//TODO: This case failed once, need more checking
			if err == nil {
				fmt.Println("/nif the error appears in GroupJoin function, return----", rs)
			}
			c.So(err, c.ShouldNotBeNil)

			_, err = From(tUsers).GroupJoin(tRoles, userSelector, roleSelector, resultSelectorPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("GroupJoin an empty slice as outer source", func() {
			rs, err := From([]int{}).GroupJoin(tUsers2, userSelector, roleSelector, groupResultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("GroupJoin an empty slice as inner source", func() {
			rs, err := From(tUsers).GroupJoin([]interface{}{}, userSelector, roleSelector, groupResultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("GroupJoin an interface{} slice as inner source", func() {
			rs, err := From(tUsers).GroupJoin(tRoles, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

		c.Convey("GroupJoin an interface{} channel as outer source", func() {
			rs, err := From(getChan(tUsers)).GroupJoin(tRoles, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})

		c.Convey("GroupJoin an interface{} channel as inner source", func() {
			rs, err := From(tUsers).GroupJoin(getChan(tRoles), userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
			for _, v := range rs {
				ur := v.(*userRoles)
				c.So(len(ur.roles), c.ShouldEqual, 2)
			}
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test GroupJoin Sequential", t, func() { testGroupJoin(30) })
	c.Convey("Test GroupJoin parallel", t, func() { testGroupJoin(7) })

	testLeftGroupJoin := func(size int) {
		defaultChunkSize = size
		c.Convey("LeftGroupJoin an empty slice as outer source", func() {
			rs, err := From([]int{}).LeftGroupJoin(tUsers2, userSelector, roleSelector, groupResultSelector).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftGroupJoin an empty slice as inner source", func() {
			rs, err := From(tUsers).LeftGroupJoin([]interface{}{}, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("LeftGroupJoin an interface{} slice as inner source", func() {
			rs, err := From(tUsers).LeftGroupJoin(tRoles, userSelector, roleSelector, groupResultSelector).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test LeftGroupJoin Sequential", t, func() { testLeftGroupJoin(30) })
	c.Convey("Test LeftGroupJoin parallel", t, func() { testLeftGroupJoin(7) })

}

func TestUnion(t *testing.T) {
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(tUsers).Union(nil) }, c.ShouldPanicWith, ErrUnionNilSource)
		})

		c.Convey("If error appears in before operation", func() {
			_, err := From(getChan(tUsers)).Select(projectWithPanic).Union([]interface{}{}).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Union an empty slice as first source", func() {
			rs, err := From([]int{}).Union(tUsers2).Results()
			if len(rs) != count {
				fmt.Println("Union an empty slice as first source failed!", len(rs))
			}
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an empty slice as secondary source", func() {
			rs, err := From(tUsers).Union([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an interface{} slice as secondary source", func() {
			rs, err := From(tUsers).Union(tUsers2).Results()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an interface{} channel as first source", func() {
			rs, err := From(getChan(tUsers)).Union(tUsers2).Results()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Union an interface{} channel as secondary source", func() {
			rs, err := From(tUsers).Union(getChan(tUsers2)).Results()
			c.So(len(rs), c.ShouldEqual, count+count/2)
			c.So(err, c.ShouldBeNil)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Union Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Union parallel", t, func() { test(parallelChunkSize) })

}

func TestConcat(t *testing.T) {
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(tUsers).Concat(nil) }, c.ShouldPanicWith, ErrConcatNilSource)
		})

		c.Convey("If error appears in before operation from channel source", func() {
			_, err := From(getChan(tUsers)).Select(projectWithPanic).Concat([]interface{}{}).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Concat an empty slice as first source", func() {
			rs, err := From([]int{}).Concat(tUsers2).Results()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count)
		})

		c.Convey("Concat an empty slice as secondary source", func() {
			rs, err := From(tUsers).Concat([]interface{}{}).Results()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count)
		})

		c.Convey("Concat an interface{} slice as secondary source", func() {
			rs, err := From(tUsers).Concat(tUsers2).Results()
			//TODO: need test KeepOrder()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count*2)
		})

		c.Convey("Concat an interface{} channel as first source", func() {
			rs, err := From(getChan(tUsers)).Concat(tUsers2).Results()
			//TODO: need test KeepOrder()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count*2)
		})

		c.Convey("Concat an interface{} channel as secondary source", func() {
			rs, err := From(tUsers).Concat(getChan(tUsers2)).Results()
			//TODO: need test KeepOrder()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, count*2)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Concat Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Concat parallel", t, func() { test(parallelChunkSize) })

}

func TestInterest(t *testing.T) {
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(tUsers).Intersect(nil) }, c.ShouldPanicWith, ErrInterestNilSource)
		})

		c.Convey("If error appears in before operation from channel source", func() {
			_, err := From(getChan(tUsers)).Select(projectWithPanic).Intersect([]interface{}{}).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Interest an empty slice as first source", func() {
			rs, err := From([]int{}).Intersect(tUsers2).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an empty slice as secondary source", func() {
			rs, err := From(tUsers).Intersect([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an interface{} slice as secondary source", func() {
			rs, err := From(tUsers).Intersect(tUsers2).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an interface{} channel as first source", func() {
			rs, err := From(getChan(tUsers)).Intersect(tUsers2).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Interest an interface{} channel as secondary source", func() {
			rs, err := From(tUsers).Intersect(getChan(tUsers2)).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Interest Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Interest parallel", t, func() { test(parallelChunkSize) })

}

func TestExcept(t *testing.T) {
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil source, error be returned", func() {
			c.So(func() { From(tUsers).Except(nil) }, c.ShouldPanicWith, ErrExceptNilSource)
		})

		c.Convey("If error appears in before operation from channel source", func() {
			_, err := From(getChan(tUsers)).Select(projectWithPanic).Except([]interface{}{}).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Except an empty slice as first source", func() {
			rs, err := From([]int{}).Except(tUsers2).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an empty slice as secondary source", func() {
			rs, err := From(tUsers).Except([]interface{}{}).Results()
			c.So(len(rs), c.ShouldEqual, count)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an interface{} slice as secondary source", func() {
			rs, err := From(tUsers).Except(tUsers2).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an interface{} channel as first source", func() {
			rs, err := From(getChan(tUsers)).Except(tUsers2).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("Except an interface{} channel as secondary source", func() {
			rs, err := From(tUsers).Except(getChan(tUsers2)).Results()
			//TODO: need test KeepOrder()
			c.So(len(rs), c.ShouldEqual, count/2)
			c.So(err, c.ShouldBeNil)
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Except Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Except parallel", t, func() { test(parallelChunkSize) })

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
		defaultChunkSize = size
		c.Convey("When passed nil function, should use the default compare function", func() {
			rs, err := From([]int{4, 2, 3, 1}).OrderBy(nil).Results()
			c.So(rs, shouldSlicesResemble, []int{1, 2, 3, 4})
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("An error should be returned if the error appears in OrderBy function", func() {
			_, err := From(tRptUsers).OrderBy(orderUserByIdPanic).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("OrderBy an empty slice", func() {
			rs, err := From([]int{}).OrderBy(nil).Results()
			c.So(len(rs), c.ShouldEqual, 0)
			c.So(err, c.ShouldBeNil)
		})

		c.Convey("OrderBy an interface{} slice, but before operation appears error", func() {
			_, err := From(getChan(tRptUsers)).Select(projectWithPanic).OrderBy(orderUserById).Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("OrderBy an interface{} slice", func() {
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

		c.Convey("OrderBy an interface{} chan", func() {
			rs, err := From(getChan(tRptUsers)).OrderBy(orderUserById).Results()
			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
			c.So(err, c.ShouldBeNil)

			id := 0
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeGreaterThanOrEqualTo, id)
				id = u.id
			}
		})
		defaultChunkSize = size
	}
	c.Convey("Test Order Sequential", t, func() { test(sequentialChunkSize) })
}

func TestReverse(t *testing.T) {
	test := func(size int) {
		defaultChunkSize = size
		c.Convey("An error appears in before operation", func() {
			_, err := From(getChan(tRptUsers)).Select(projectWithPanic).Reverse().Results()
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Reverse an interface{} slice", func() {
			rs, err := From(tRptUsers).OrderBy(orderUserById).Reverse().Results()
			c.So(len(rs), c.ShouldEqual, len(tRptUsers))
			c.So(err, c.ShouldBeNil)

			id := 1000000
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
				id = u.id
			}
		})

		//TODO: OrderBy will always return list, this case must be updated
		c.Convey("Reverse an interface{} chan", func() {
			rs, err := From(getChan(tRptUsers)).OrderBy(orderUserById).Reverse().Results()
			c.So(err, c.ShouldBeNil)
			c.So(len(rs), c.ShouldEqual, len(tRptUsers))

			id := 1000000
			for _, v := range rs {
				u := v.(user)
				c.So(u.id, c.ShouldBeLessThanOrEqualTo, id)
				id = u.id
			}
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Reverse Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Reverse parallel", t, func() { test(parallelChunkSize) })
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

	test := func(size int) {
		defaultChunkSize = size
		c.Convey("When passed nil function, should use the default compare function", func() {
			_, err := From([]int{4, 2, 3, 1}).Aggregate(nil)
			c.So(err, c.ShouldNotBeNil)
			_, err = From([]int{4, 2, 3, 1}).Aggregate(([]*AggregateOperation{})...)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("If the error appears in Aggregate function", func() {
			_, err := From([]int{4, 2, 3, 1}).Aggregate(&AggregateOperation{0, aggregatePanic, nil})
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("An error appears in previous operation", func() {
			_, err := From(tUsers).Select(projectWithPanic).Aggregate(myAgg)
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Aggregate an empty slice", func() {
			_, err := From([]int{}).Aggregate(Max())
			c.So(err, c.ShouldNotBeNil)
		})

		c.Convey("Aggregate an interface{} slice", func() {
			r, err := From(tUsers).Aggregate(myAgg)
			c.So(err, c.ShouldBeNil)
			_ = r
		})

		c.Convey("Aggregate an interface{} channel", func() {
			r, err := From(getChan(tUsers)).Aggregate(myAgg)
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			_ = r
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Aggregate Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Aggregate parallel", t, func() { test(parallelChunkSize) })
}

func TestSumCountAvgMaxMin(t *testing.T) {

	test := func(size int) {
		defaultChunkSize = size
		c.Convey("Max an int slice", func() {
			r, err := From(tInts).Max()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, count-1)
		})
		c.Convey("MaxBy an int slice", func() {
			r, err := From(tUsers).Aggregate(Max(func(v interface{}) interface{} {
				return v.(user).id
			}))
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, count-1)
		})

		c.Convey("Min an int slice", func() {
			r, err := From(tInts).Min()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 0)
		})
		c.Convey("MinBy an int slice", func() {
			r, err := From(tUsers).Aggregate(Min(func(v interface{}) interface{} {
				return v.(user).id
			}))
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 0)
		})

		c.Convey("Sum an int slice", func() {
			r, err := From(tInts).Sum()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, (count-1)*(count/2))
		})
		c.Convey("SumBy an int slice", func() {
			r, err := From(tUsers).Aggregate(Sum(func(v interface{}) interface{} {
				return v.(user).id
			}))
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, (count-1)*(count/2))
		})

		c.Convey("Count an int slice", func() {
			r, err := From(tInts).Count()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, count)
		})
		c.Convey("Count an interface{} slice", func() {
			r, err := From(tUsers).Count()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, count)
		})
		c.Convey("CountBy an interface{} slice", func() {
			r, err := From(tUsers).Count(filterFunc)
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, count/2)
		})
		c.Convey("send a nil predicate to CountBy", func() {
			r, err := From(tUsers).Count(nil)
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, count)
		})
		c.Convey("Average an int slice", func() {
			r, err := From(tInts).Average()
			//TODO: need test keep order
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, float32(count-1)/float32(2))
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Sum/Count/Avg/Max/Min Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Sum/Count/Avg/Max/Min parallel", t, func() { test(parallelChunkSize) })
}

//获取0到4所有的组合, 用于测试随机顺序下Skip/Take/ElementAt/FirstBy等操作是否正确
func getIndexses(countOfSkipTestData int) (indexses [][]int) {
	for i := 0; i < countOfSkipTestData; i++ {
		//开始创建一个随机组合
		for j := 0; j < countOfSkipTestData; j++ {
			if j == i {
				continue
			}
			for k := 0; k < countOfSkipTestData; k++ {
				if k == j || k == i {
					continue
				}
				for m := 0; m < countOfSkipTestData; m++ {
					if m == k || m == j || m == i {
						continue
					}
					for n := 0; n < countOfSkipTestData; n++ {
						if n == k || n == j || n == i || n == m {
							continue
						}
						indexs := []int{i, j, k, m, n}
						indexses = append(indexses, indexs)
					}
				}
			}
		}
	}
	return
}

var countOfSkipTestData int = 5

func getChunkByi(i int, ints []interface{}) *Chunk {
	size := count / countOfSkipTestData
	return &Chunk{NewSlicer(ints[i*size : (i+1)*size]), i, 0}
}

//根据指定的顺序发送chunk到channel
func getCChunkSrc(indexs []int, ints []interface{}) chan *Chunk {
	chunkSrc := make(chan *Chunk)
	go func() {
		//indexs := []int{3, 0, 1, 4, 2}
		defer func() {
			if e := recover(); e != nil {
				_ = e
			}
		}()
		//fmt.Println("\nsend----------------")
		for _, i := range indexs {
			//fmt.Println("\nsend", getChunkByi(i))
			chunkSrc <- getChunkByi(i, ints)
		}
		close(chunkSrc)
	}()
	return chunkSrc
}

///TODO: outstanding testing item:
// 1. SkipWhile/TakeWhile after Union operation
// 2. if the data source includes the count of match item are more than one.
func TestSkipAndTake(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	indexses := getIndexses(countOfSkipTestData)
	//indexses := [][]int{[]int{0, 1, 3, 2, 4}}

	getSkipResult := func(i int) []interface{} {
		r := make([]interface{}, count-i)
		for j := 0; j < count-i; j++ {
			r[j] = j + i
		}
		return r
	}

	getTakeResult := func(i int) []interface{} {
		r := make([]interface{}, i)
		for j := 0; j < i; j++ {
			r[j] = j
		}
		return r
	}

	_ = getTakeResult

	test := func(size int) {
		defaultChunkSize = size
		//test list source -----------------------------------------------
		c.Convey("Test Skip in list", func() {
			c.Convey("Skip nothing", func() {
				r, err := From(ints).Skip(-1).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, ints)
			})
			c.Convey("Skip all", func() {
				r, err := From(ints).Skip(100).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, []interface{}{})
			})
			c.Convey("Skip 12", func() {
				r, err := From(ints).Skip(12).Results()
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, getSkipResult(12))
			})
		})

		c.Convey("Test SkipWhile in list", func() {
			c.Convey("SkipWhile with nil predicateFunc", func() {
				c.So(func() { From(ints).SkipWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
			})

			c.Convey("SkipWhile using a predicate func with panic error", func() {
				_, err := From(ints).SkipWhile(filterWithPanic).Results()
				c.So(err, c.ShouldNotBeNil)
			})

			c.Convey("SkipWhile nothing", func() {
				//fmt.Println("SkipWhile nothing")
				r, err := From(ints).SkipWhile(func(v interface{}) bool {
					return v.(int) < -1
				}).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, ints)
			})

			c.Convey("SkipWhile all", func() {
				//fmt.Println("SkipWhile all")
				r, err := From(ints).SkipWhile(func(v interface{}) bool {
					return v.(int) < 100
				}).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, []interface{}{})
			})

			c.Convey("SkipWhile 12", func() {
				//fmt.Println("SkipWhile 12")
				r, err := From(ints).SkipWhile(func(v interface{}) bool {
					return v.(int) < 12
				}).Results()
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, getSkipResult(12))
			})
		})

		c.Convey("Test Take in list", func() {
			c.Convey("Take nothing", func() {
				r, err := From(ints).Take(-1).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, []interface{}{})
			})
			c.Convey("Take all", func() {
				r, err := From(ints).Take(100).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, ints)
			})
			c.Convey("Take 12", func() {
				r, err := From(ints).Take(12).Results()
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, getTakeResult(12))
			})
		})

		c.Convey("Test TakeWhile in list", func() {
			c.Convey("TakeWhile using nil predicateFunc", func() {
				c.So(func() { From(ints).TakeWhile(nil).Results() }, c.ShouldPanicWith, ErrNilAction)
			})

			c.Convey("TakeWhile using a predicate func with panic error", func() {
				_, err := From(ints).TakeWhile(filterWithPanic).Results()
				c.So(err, c.ShouldNotBeNil)
			})

			c.Convey("TakeWhile nothing", func() {
				//fmt.Println("TakeWhile nothing")
				r, err := From(ints).TakeWhile(func(v interface{}) bool {
					return v.(int) < -1
				}).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, []interface{}{})
			})

			c.Convey("TakeWhile all", func() {
				//fmt.Println("TakeWhile all")
				r, err := From(ints).TakeWhile(func(v interface{}) bool {
					return v.(int) < 100
				}).Results()
				//TODO: need test keep order
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, ints)
			})

			c.Convey("TakeWhile 12", func() {
				//fmt.Println("TakeWhile 12")
				r, err := From(ints).TakeWhile(func(v interface{}) bool {
					return v.(int) < 12
				}).Results()
				c.So(err, c.ShouldBeNil)
				c.So(r, shouldSlicesResemble, getTakeResult(12))
			})
		})

		//--------------------------------------------------------
		c.Convey("Test Skip in channel", func() {
			c.Convey("Skip nothing", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).Skip(-1).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, ints) != "" {
						fmt.Println("skip -1 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("Skip all", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).Skip(100).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, []interface{}{}) != "" {
						fmt.Println("skip 100 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("Skip 12", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).Skip(12).Results()
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, getSkipResult(12)) != "" {
						fmt.Println("skip 12 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, getSkipResult(12))
				}
			})
		})

		c.Convey("Test SkipWhile in channel", func() {
			c.Convey("TakeWhile using a predicate func with panic error", func() {
				_, err := From(getChan(ints)).SkipWhile(filterWithPanic).Results()
				c.So(err, c.ShouldNotBeNil)
			})

			c.Convey("If an error appears in before operation", func() {
				_, err := From(getChan(ints)).Select(projectWithPanic).SkipWhile(filterWithPanic).Results()
				c.So(err, c.ShouldNotBeNil)
			})

			c.Convey("SkipWhile nothing", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).SkipWhile(func(v interface{}) bool {
						return v.(int) < -1
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, ints) != "" {
						fmt.Println("SkipWhile -1 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("SkipWhile all", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).SkipWhile(func(v interface{}) bool {
						return v.(int) < 100
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, []interface{}{}) != "" {
						fmt.Println("SkipWhile 100 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("SkipWhile 12", func() {
				for _, v := range indexses {
					//fmt.Println("\nSkipWhile 12 for", v)
					r, err := From(getCChunkSrc(v, ints)).SkipWhile(func(v interface{}) bool {
						return v.(int) < 12
					}).Results()
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, getSkipResult(12)) != "" {
						fmt.Println("!!!!!!!SkipWhile 12 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, getSkipResult(12))
				}
			})
		})

		c.Convey("Test Take in channel", func() {
			c.Convey("Take nothing", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).Take(-1).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("Take all", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).Take(100).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("Take 12", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).Take(12).Results()
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, getTakeResult(12)) != "" {
						fmt.Println("take 12 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, getTakeResult(12))
				}
			})
		})

		c.Convey("Test TakeWhile in channel", func() {
			c.Convey("TakeWhile using a predicate func with panic error", func() {
				_, err := From(getChan(ints)).TakeWhile(filterWithPanic).Results()
				c.So(err, c.ShouldNotBeNil)
			})

			c.Convey("TakeWhile using a predicate func with panic error after a where operation with panic error", func() {
				_, err := From(getChan(ints)).Where(filterWithPanic).TakeWhile(filterWithPanic).Results()
				c.So(err, c.ShouldNotBeNil)
			})

			c.Convey("TakeWhile nothing", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).TakeWhile(func(v interface{}) bool {
						return v.(int) < -1
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, []interface{}{}) != "" {
						fmt.Println("TakeWhile -1 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("TakeWhile all", func() {
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).TakeWhile(func(v interface{}) bool {
						return v.(int) < 100
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, ints) != "" {
						fmt.Println("TakeWhile 100 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("TakeWhile 12", func() {
				//fmt.Println("\nTakeWhile 12")
				for _, v := range indexses {
					r, err := From(getCChunkSrc(v, ints)).TakeWhile(func(v interface{}) bool {
						return v.(int) < 12
					}).Results()
					c.So(err, c.ShouldBeNil)
					if shouldSlicesResemble(r, getTakeResult(12)) != "" {
						fmt.Println("TakeWhile 12 for", v, "return", r)
					}
					c.So(r, shouldSlicesResemble, getTakeResult(12))
				}
			})
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Skip and Take Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Skip and Take parallel", t, func() { test(parallelChunkSize) })
}

func TestElementAt(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	indexses := getIndexses(countOfSkipTestData)

	c.Convey("Test ElementAt in channel", t, func() {
		c.Convey("ElementAt nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getCChunkSrc(v, ints)).ElementAt(-1)
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("ElementAt nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getCChunkSrc(v, ints)).ElementAt(100)
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("If an error appears in before operation", func() {
			for _, v := range indexses {
				_, _, err := From(getCChunkSrc(v, ints)).Select(projectWithPanic).ElementAt(100)
				c.So(err, c.ShouldNotBeNil)
			}
		})

		c.Convey("ElementAt 12", func() {
			for _, v := range indexses {
				//_ = indexses
				r, found, err := From(getCChunkSrc(v, ints)).ElementAt(12)
				c.So(err, c.ShouldBeNil)
				c.So(r, c.ShouldEqual, 12)
				if !found {
					fmt.Println("found is", found, ", r=", r)
				}
				c.So(found, c.ShouldEqual, true)
			}
		})
	})

	c.Convey("Test ElementAt in list", t, func() {
		c.Convey("ElementAt nothing", func() {
			_, found, err := From(tInts).ElementAt(-1)
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("ElementAt nothing", func() {
			_, found, err := From(tInts).ElementAt(100)
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("ElementAt 12", func() {
			r, found, err := From(tInts).ElementAt(12)
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 12)
			c.So(found, c.ShouldEqual, true)
		})
	})
}

func TestFirstBy(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	indexses := getIndexses(countOfSkipTestData)

	c.Convey("Test FirstBy in channel", t, func() {
		c.Convey("FirstBy with panic an error", func() {
			for _, v := range indexses {
				_, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
					panic(errors.New("!error"))
				})
				c.So(err, c.ShouldNotBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("If an error appears in before operation", func() {
			for _, v := range indexses {
				_, _, err := From(getCChunkSrc(v, ints)).Select(projectWithPanic).FirstBy(func(v interface{}) bool {
					return v.(int) == -1
				})
				c.So(err, c.ShouldNotBeNil)
			}
		})
		c.Convey("FirstBy nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
					return v.(int) == -1
				})
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("FirstBy nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
					return v.(int) == count
				})
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("FirstBy 12", func() {
			for _, v := range indexses {
				r, found, err := From(getCChunkSrc(v, ints)).FirstBy(func(v interface{}) bool {
					return v.(int) == 12
				})
				c.So(err, c.ShouldBeNil)
				c.So(r, c.ShouldEqual, 12)
				c.So(found, c.ShouldEqual, true)
			}
		})
	})

	c.Convey("Test FirstBy in list", t, func() {
		c.Convey("FirstBy with panic an error", func() {
			_, found, err := From(tInts).FirstBy(func(v interface{}) bool {
				panic(errors.New("!error"))
			})
			c.So(err, c.ShouldNotBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("FirstBy nothing", func() {
			_, found, err := From(tInts).FirstBy(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("FirstBy nothing", func() {
			_, found, err := From(tInts).FirstBy(func(v interface{}) bool {
				return v.(int) == 100
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("FirstBy 12", func() {
			r, found, err := From(tInts).FirstBy(func(v interface{}) bool {
				return v.(int) == 12
			})
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 12)
			c.So(found, c.ShouldEqual, true)
		})
	})

	c.Convey("Test FirstBy after where", t, func() {
		c.Convey("FirstBy with panic an error in both two operations", func() {
			_, found, err := From(tInts).Where(filterWithPanic, parallelChunkSize).FirstBy(func(v interface{}) bool {
				panic(errors.New("!error"))
			})
			c.So(err, c.ShouldNotBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("FirstBy nothing with panic an error in where operations", func() {
			_, found, err := From(tInts).Where(filterWithPanic, parallelChunkSize).FirstBy(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldNotBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("FirstBy nothing", func() {
			_, found, err := From(tInts).Where(filterFunc, parallelChunkSize).FirstBy(func(v interface{}) bool {
				return v.(int) == 100
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("FirstBy 12", func() {
			r, found, err := From(tInts).Where(filterFunc, parallelChunkSize).FirstBy(func(v interface{}) bool {
				return v.(int) == 12
			})
			c.So(err, c.ShouldBeNil)
			c.So(r, c.ShouldEqual, 12)
			c.So(found, c.ShouldEqual, true)
		})
	})
}

func TestAnyAndAll(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	c.Convey("Test Any and All in channel", t, func() {
		c.Convey("Predicate with panic an error", func() {
			_, err := From(getIntChan(tInts)).Any(func(v interface{}) bool {
				panic(errors.New("!error"))
			})
			c.So(err, c.ShouldNotBeNil)
		})
		c.Convey("If an error appears in before operation", func() {
			_, err := From(getIntChan(tInts)).Select(projectWithPanic).Any(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldNotBeNil)
		})
		c.Convey("Any and All nothing", func() {
			found, err := From(getIntChan(tInts)).Any(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)

			found, err = From(getIntChan(tInts)).All(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("Find any int == 12", func() {
			found, err := From(getIntChan(tInts)).Any(func(v interface{}) bool {
				return v.(int) == 12
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, true)
		})
		c.Convey("Find all int >= 0", func() {
			found, err := From(getIntChan(tInts)).Any(func(v interface{}) bool {
				return v.(int) >= 0
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, true)
		})
	})

	c.Convey("Test Any in slice", t, func() {
		c.Convey("Any with panic an error", func() {
			_, err := From(tInts).Any(func(v interface{}) bool {
				panic(errors.New("!error"))
			})
			c.So(err, c.ShouldNotBeNil)
		})
		c.Convey("If an error appears in before operation", func() {
			_, err := From(tInts).Select(projectWithPanic).Any(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldNotBeNil)
		})
		c.Convey("Any nothing", func() {
			found, err := From(tInts).Any(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)

			found, err = From(tInts).All(func(v interface{}) bool {
				return v.(int) == -1
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, false)
		})
		c.Convey("Find any int == 12", func() {
			found, err := From(tInts).Any(func(v interface{}) bool {
				return v.(int) == 12
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, true)
		})
		c.Convey("Find all int >= 0", func() {
			found, err := From(tInts).Any(func(v interface{}) bool {
				return v.(int) >= 0
			})
			c.So(err, c.ShouldBeNil)
			c.So(found, c.ShouldEqual, true)
		})
	})

}

func chanToSlice(out chan interface{}) (rs []interface{}) {
	rs = make([]interface{}, 0, 4)
	for v := range out {
		rs = append(rs, v)
	}
	return rs
}

func TestToChannel(t *testing.T) {
	c.Convey("Test ToChan of list source", t, func() {
		c.Convey("For emtpy list", func() {
			ds := &listSource{NewSlicer([]interface{}{})}
			out := ds.ToChan()
			rs := chanToSlice(out)
			c.So(rs, shouldSlicesResemble, []interface{}{})
		})
		c.Convey("For emtpy list", func() {
			ds := &listSource{NewSlicer([]interface{}{1, 2, 3, 4})}
			out := ds.ToChan()
			rs := chanToSlice(out)
			c.So(rs, shouldSlicesResemble, []interface{}{1, 2, 3, 4})
		})
	})

	expectedInts := make([]interface{}, count/2)
	for i := 0; i < count/2; i++ {
		expectedInts[i] = i * 2
	}
	c.Convey("Test ToChan of channel source", t, func() {
		c.Convey("For emtpy channel", func() {
			out, _, err := From([]int{}).Where(filterWithPanic).ToChan()
			c.So(err, c.ShouldBeNil)
			rs := chanToSlice(out)
			c.So(rs, shouldSlicesResemble, []interface{}{})
		})

		c.Convey("For channel", func() {
			out, _, err := From(tInts).Where(filterFunc).ToChan()
			c.So(err, c.ShouldBeNil)
			rs := chanToSlice(out)
			//ToChan cannot keep original order
			//c.So(rs, shouldSlicesResemble, expectedInts)
			c.So(len(rs), c.ShouldEqual, len(expectedInts))
		})

		c.Convey("For origin channel", func() {
			src := make(chan int)
			go func() {
				for i := 0; i < count; i++ {
					src <- i
				}
				close(src)
			}()
			out, _, err := From(src).ToChan()
			c.So(err, c.ShouldBeNil)
			rs := chanToSlice(out)
			c.So(rs, shouldSlicesResemble, tInts)
		})
	})

	c.Convey("Test error handling for ToChan", t, func() {
		c.Convey("no error appears from list source", func() {
			out, errChan, err := From(tInts).Where(filterFunc, parallelChunkSize).Select(projectFunc, parallelChunkSize).ToChan()
			c.So(err, c.ShouldBeNil)
			rs, stepErr := getChanResult(out, errChan)
			c.So(stepErr, c.ShouldBeNil)
			if len(rs) != count/2 {
				fmt.Println("list count error, ", rs, tInts)
			}
			c.So(len(rs), c.ShouldEqual, count/2)
		})

		c.Convey("When error appears in last chunk from list source", func() {
			out, errChan, err := From(tInts).Where(filterWithPanic, parallelChunkSize).Select(projectFunc, parallelChunkSize).ToChan()
			c.So(err, c.ShouldBeNil)
			_, stepErr := getChanResult(out, errChan)
			c.So(stepErr, c.ShouldNotBeNil)
		})

		c.Convey("no error appears from chan source", func() {
			out, errChan, err := From(getIntChan(tInts)).Where(filterFunc, parallelChunkSize).Select(projectFunc, parallelChunkSize).ToChan()
			c.So(err, c.ShouldBeNil)
			rs, stepErr := getChanResult(out, errChan)
			c.So(stepErr, c.ShouldBeNil)
			if len(rs) != count/2 {
				fmt.Println("chan count error, ", rs, tInts)
			}
			c.So(len(rs), c.ShouldEqual, count/2)
		})

		c.Convey("When error appears in last chunk from chan source", func() {
			out, errChan, err := From(getIntChan(tInts)).Where(filterWithPanic, parallelChunkSize).Select(projectFunc, parallelChunkSize).ToChan()
			c.So(err, c.ShouldBeNil)
			_, stepErr := getChanResult(out, errChan)
			c.So(stepErr, c.ShouldNotBeNil)
		})

	})

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
