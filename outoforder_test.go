package plinq

import (
	"errors"
	"fmt"
	c "github.com/smartystreets/goconvey/convey"
	"testing"
)

var (
	sequentialChunkSize     = 100
	parallelChunkSize   int = count / 7
)

//获取0到4所有的组合, 用于测试随机顺序下Skip/Take/ElementAt/FirstBy等操作是否正确
//[0,1,2,3,4], [0,1,2,4,3], [0,1,3,2,4]......
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
func getChunkSrcByOrder(indexs []int, ints []interface{}) chan *Chunk {
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
func TestSkipAndTakeWithOutOfOrder(t *testing.T) {
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
		//--------------------------------------------------------
		c.Convey("Test Skip for out of order", func() {
			c.Convey("Skip nothing", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).Skip(-1).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("Skip all", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).Skip(100).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("Skip 12", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).Skip(12).Results()
					c.So(err, c.ShouldBeNil)
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
					r, err := From(getChunkSrcByOrder(v, ints)).SkipWhile(func(v interface{}) bool {
						return v.(int) < -1
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("SkipWhile all", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).SkipWhile(func(v interface{}) bool {
						return v.(int) < 100
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("SkipWhile 12", func() {
				for _, v := range indexses {
					//fmt.Println("\nSkipWhile 12 for", v)
					r, err := From(getChunkSrcByOrder(v, ints)).SkipWhile(func(v interface{}) bool {
						return v.(int) < 12
					}).Results()
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, getSkipResult(12))
				}
			})
		})

		c.Convey("Test Take in channel", func() {
			c.Convey("Take nothing", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).Take(-1).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("Take all", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).Take(100).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("Take 12", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).Take(12).Results()
					c.So(err, c.ShouldBeNil)
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
					r, err := From(getChunkSrcByOrder(v, ints)).TakeWhile(func(v interface{}) bool {
						return v.(int) < -1
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, []interface{}{})
				}
			})
			c.Convey("TakeWhile all", func() {
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).TakeWhile(func(v interface{}) bool {
						return v.(int) < 100
					}).Results()
					//TODO: need test keep order
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, ints)
				}
			})
			c.Convey("TakeWhile 12", func() {
				//fmt.Println("\nTakeWhile 12")
				for _, v := range indexses {
					r, err := From(getChunkSrcByOrder(v, ints)).TakeWhile(func(v interface{}) bool {
						return v.(int) < 12
					}).Results()
					c.So(err, c.ShouldBeNil)
					c.So(r, shouldSlicesResemble, getTakeResult(12))
				}
			})
		})
		defaultChunkSize = DEFAULTCHUNKSIZE
	}
	c.Convey("Test Skip and Take Sequential", t, func() { test(sequentialChunkSize) })
	c.Convey("Test Skip and Take parallel", t, func() { test(parallelChunkSize) })
}

func TestElementAtWithOutOfOrder(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	indexses := getIndexses(countOfSkipTestData)

	c.Convey("Test ElementAt for out of order", t, func() {
		c.Convey("ElementAt nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getChunkSrcByOrder(v, ints)).ElementAt(-1)
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("ElementAt nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getChunkSrcByOrder(v, ints)).ElementAt(100)
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("If an error appears in before operation", func() {
			for _, v := range indexses {
				_, _, err := From(getChunkSrcByOrder(v, ints)).Select(projectWithPanic).ElementAt(100)
				c.So(err, c.ShouldNotBeNil)
			}
		})

		c.Convey("ElementAt 12", func() {
			for _, v := range indexses {
				//_ = indexses
				r, found, err := From(getChunkSrcByOrder(v, ints)).ElementAt(12)
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
	})
}

func TestFirstByWithOutOfOrder(t *testing.T) {
	ints := make([]interface{}, count)
	for i := 0; i < count; i++ {
		ints[i] = i
	}

	indexses := getIndexses(countOfSkipTestData)

	c.Convey("Test FirstBy for out of order", t, func() {
		c.Convey("FirstBy with panic an error", func() {
			for _, v := range indexses {
				_, found, err := From(getChunkSrcByOrder(v, ints)).FirstBy(func(v interface{}) bool {
					panic(errors.New("!error"))
				})
				c.So(err, c.ShouldNotBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("If an error appears in before operation", func() {
			for _, v := range indexses {
				_, _, err := From(getChunkSrcByOrder(v, ints)).Select(projectWithPanic).FirstBy(func(v interface{}) bool {
					return v.(int) == -1
				})
				c.So(err, c.ShouldNotBeNil)
			}
		})
		c.Convey("FirstBy nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getChunkSrcByOrder(v, ints)).FirstBy(func(v interface{}) bool {
					return v.(int) == -1
				})
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("FirstBy nothing", func() {
			for _, v := range indexses {
				_, found, err := From(getChunkSrcByOrder(v, ints)).FirstBy(func(v interface{}) bool {
					return v.(int) == count
				})
				c.So(err, c.ShouldBeNil)
				c.So(found, c.ShouldEqual, false)
			}
		})
		c.Convey("FirstBy 12", func() {
			for _, v := range indexses {
				r, found, err := From(getChunkSrcByOrder(v, ints)).FirstBy(func(v interface{}) bool {
					return v.(int) == 12
				})
				c.So(err, c.ShouldBeNil)
				c.So(r, c.ShouldEqual, 12)
				c.So(found, c.ShouldEqual, true)
			}
		})
	})

}
