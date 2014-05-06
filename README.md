go-plinq
========

PLINQ library for go, support the lazy evaluated and can use channel as source

## 主要特性

* 延迟查询

```go
src := make([]int, 100)
pSrc := &src

q := From(pSrc).Where(whereFunc).Select(selectFunc)

for i := count; i < count+10; i++ { src = append(src, i) }

rs, err := q1.Results()
```

* 并行查询

go-plinq默认采用并行查询，但也可以以非并行方式运行。在后面采用了go-linq的非并行模式对plinq的并行加速比进行了测试。从测试结果看，在4核CPU上，当N为1000时，plinq的加速比大约为2，当N达到10000或以上时，plinq的加速比稳定在3左右，如果加大运算的复杂度，加速比可以进一步提高到3.5左右。这里的加速比只是一个参考值，在并行算法中，加速比取决于很多因素，难以一概而论。

如果在小数据量下，并行的开销将大于收益，此时plinq将以非并行模式运行。这个阀值可以在运行时设置，默认为100。

* 除了支持slice，也支持channel作为数据源和输出

    * Channel作为查询数据源

    ```go
    type User struct {
	    id   int
	    name string
    }
    chUsers := make(chan *User)
    //send user to chUsers...

    rs, err := From(chUsers).Select(func(u    interface{})interface{} {
	    return u.name
    }).Results()
    ```

    * 返回Channel作为查询结果

    ```go
    chUsers := make(chan *User)
    //send user to chUsers....

    q := plinq.From(chUsers).Select(func(u    interface{}) interface{} {
	    return u.(User).name
    })
    if rsChan, errChan, err := q.ToChan(); err != nil {
	    fmt.Println("Return an error:", err)
    } else {
	    names := make([]interface{}, 0, 1)
	    for v := range rsChan {
	  	    names = append(names, v)
	    }

	    if e, ok := <-errChan; ok {
		    fmt.Println("Get an error in linq:", e)
	    } else {
		    fmt.Println("Return:", names)
	    }
    }
    ```

## 已经实现的linq查询运算符:
* 排序运算符：

OrderBy, Reverse

* Set运算符：

Distinct, Except, Intersect, Union

* 筛选运算符：

Where

* 投影运算符：

Select, SeleteMany

* 连接运算符：

Join, LeftJoin, GroupJoin, LeftGroupJoin

* 分组运算符：

GroupBy

* 串联运算符：

Concat

* 聚合运算符：

Aggregate, Average, Count, Max, Min, Sum

* 过滤操作符：

Skip, SkipWhile, Take, TakeWhile

* 元素运算符：

ElementAt, First, FirstBy, Last, LastBy

## 未完成的linq查询运算符:

* 量词/Quantifiers：

Contains, All, Any, AnyWith, SequenceEqual

* 生成器方法

Range, Repeat

* 元素运算符/Element Operators

, Single 

## 文档（未完成。。。）:

## 性能测试

这里采用了go-linq的非并行模式作为对比的依据，测试结果是采用go提供的benchmark统计得到的。

测试的CPU为Intel Atom Z3740D（4核 1.33GHZ）， 操作系统为Win8.1，时间单位是ms。

### 性能测试结果

N = 100
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>0.684</td><td>0.519</td><td>0.352</td><td>0.261</td><td>0.265</td><td>0.104</td><td>0.028</td><td>0.244</td><td>0.203</td>
  </tr>
  <tr>
    <td>go-linq</td><td>0.468</td><td>0.346</td><td>0.294</td><td>0.232</td><td>0.299</td><td>0.056</td><td>0.024</td><td>0.177</td><td>0.172</td>
  </tr>
</table>

N = 1000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>1.284</td><td>0.983</td><td>2.926</td><td>2.273</td><td>2.294</td><td>0.777</td><td>0.184</td><td>1.095</td><td>1.087</td>
  </tr>
  <tr>
    <td>go-linq</td><td>2.403</td><td>1.658</td><td>2.793</td><td>2.481</td><td>3.209</td><td>0.550</td><td>0.229</td><td>1.739</td><td>1.689</td>
  </tr>
</table>

N = 10000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>8.217</td><td>5.789</td><td>35.713</td><td>29.516</td><td>29.734</td><td>4.374</td><td>0.525</td><td>8.593</td><td>8.387</td>
  </tr>
  <tr>
    <td>go-linq</td><td>22.308</td><td>15.337</td><td>33.649</td><td>24.703</td><td>31.231</td><td>5.634</td><td>2.300</td><td>17.066</td><td>17.020</td>
  </tr>
</table>

N = 100000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>68.521</td><td>49.713</td><td>370.174</td><td>316.697</td><td>317.158</td><td>28.722</td><td>3.733</td><td>82.625</td><td>81.059</td>
  </tr>
  <tr>
    <td>go-linq</td><td>215.817</td><td>146.860</td><td>358.402</td><td>265.927</td><td>345.938</td><td>55.790</td><td>22.629</td><td>169.285</td><td>168.506</td>
  </tr>
</table>

N = 1000000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>698.026</td><td>495.929</td><td>3613.313</td><td>4074.990</td><td>4154.106</td><td>256.377</td><td>35.813</td><td>718.557</td><td>708.542</td>
  </tr>
  <tr>
    <td>go-linq</td><td>2214.319</td><td>1328.957</td><td>3700.441</td><td>2961.360</td><td>4201.261</td><td>506.144</td><td>210.309</td><td>1560.294</td><td>1558.292</td>
  </tr>
</table>

上面的测试结果中，Union、Intersect， Except的测试结果始终非常接近，这3个运算符基本都是串行的运算，OrderBy同样也是。

Join、Distinct没有列出对比结果，因为go-linq的实现采取了类似双重循环的算法，效率要低很多，所以无法对比并行和串行的差异。

并行算法的加速比取决于许多因素。从测试结果看，当N>=10000时，Select和Where运算的加速比基本稳定在3，但如果在select和where的函数中加入更耗时的操作，加速比将能达到3.5左右。

