go-plinq
========

PLINQ library for go, support the lazy evaluated and can use channel as source

## 主要特性

* 延迟查询

```go
pSrc := &src
q := From(pSrc).Where(whereFunc).Select(selectFunc)
for i := count; i < count+10; i++ { src = append(src, i) }
rs, err := q1.Results()
```

* 并行查询

go-plinq默认采用并行查询，但也可以通过设置数据块的大小以非并行方式运行。为了测量plinq的并行加速比，采用了go-linq的非并行模式作为对比的依据。从后面的测试结果看，当N为1000时，plinq的加速比大约为2，当N达到10000或以上时，plinq的加速比稳定在3左右。这里的加速比只是一个参考值，在并行算法中，加速比取决于很多因素，难以一概而论。

通过设置数据块的最小值，可以让plinq在小数据量（N<=100)的情况下退化为非并行模式运行，以防止并行带来的开销超过了并行的收益。

* 支持Channel作为查询数据源

```go
type User struct {
	id   int
	name string
}
chUsers := make(chan *User)
rs, err := From(chUsers).Select(func(u interface{})interface{} {
	return u.name
}).Results()
```

* 支持返回Channel作为查询结果

```go
users := make([]*User, 100)
rs, err := From(users).Select(func(u interface{})interface{} {
	return u.name
}).ToChan()
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

ElementAt, FirstBy 

## 未完成的linq查询运算符:

* 量词/Quantifiers：

Contains, All, Any, AnyWith, SequenceEqual

* 生成器方法

Range, Repeat

* 元素运算符/Element Operators

First, Last, LastBy, Single 

## 文档（未完成。。。）:

## 性能测试

这里采用了go-linq的非并行模式作为对比的依据，测试结果是采用go提供的benchmark统计得到的。

测试的CPU为Intel Atom Z3740D（4核 1.33GHZ），时间单位是ms。

### 性能测试结果

N = 100
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>0.249</td><td>0.190</td><td>0.347</td><td>0.255</td><td>0.266</td><td>0.101</td><td>0.029</td><td>0.250</td><td>0.203</td>
  </tr>
  <tr>
    <td>go-linq</td><td>0.224</td><td>0.166</td><td>0.293</td><td>0.233</td><td>0.297</td><td>0.056</td><td>0.024</td><td>0.186</td><td>0.172</td>
  </tr>
</table>

N = 1000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>1.279</td><td>1.012</td><td>2.930</td><td>2.920</td><td>2.941</td><td>0.766</td><td>0.184</td><td>1.094</td><td>1.121</td>
  </tr>
  <tr>
    <td>go-linq</td><td>2.322</td><td>1.635</td><td>2.747</td><td>2.500</td><td>3.230</td><td>0.548</td><td>0.231</td><td>1.733</td><td>1.665</td>
  </tr>
</table>

N = 10000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>8.217</td><td>5.883</td><td>36.133</td><td>29.606</td><td>29.444</td><td>4.202</td><td>0.524</td><td>8.472</td><td>8.633</td>
  </tr>
  <tr>
    <td>go-linq</td><td>22.568</td><td>15.513</td><td>33.193</td><td>24.343</td><td>30.973</td><td>5.758</td><td>2.339</td><td>17.538</td><td>17.119</td>
  </tr>
</table>

N = 100000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>69.763</td><td>50.194</td><td>377.585</td><td>323.476</td><td>322.451</td><td>29.643</td><td>3.746</td><td>83.282</td><td>81.700</td>
  </tr>
  <tr>
    <td>go-linq</td><td>218.864</td><td>147.164</td><td>359.931</td><td>267.010</td><td>344.306</td><td>55.085</td><td>22.721</td><td>169.383</td><td>167.977</td>
  </tr>
</table>

N = 1000000
<table>
  <tr>
    <th></th><th>Select</th><th>Where</th><th>Union</th><th>Except</th><th>Intersect</th><th>Reverse</th><th>Sum</th><th>SkipWhile</th><th>FirstBy</th>
  </tr>

  <tr>
    <td>go-plinq</td><td>692.519</td><td>572.842</td><td>3557.229</td><td>3747.599</td><td>3740.498</td><td>305.549</td><td>37.175</td><td>712.490</td><td>699.026</td>
  </tr>
  <tr>
    <td>go-linq</td><td>2230.280</td><td>1385.038</td><td>3274.814</td><td>2983.577</td><td>3950.806</td><td>456.671</td><td>210.697</td><td>1560.295</td><td>1550.277</td>
  </tr>
</table>

上面的测试结果中，Union、Intersect， Except的测试结果始终非常接近，这3个运算符基本都是串行的运算，OrderBy同样也是。

Join、Distinct没有列出对比结果，因为go-linq的实现采取了类似双重循环的算法，效率要低很多，所以无法对比并行和串行的差异。

并行算法的加速比取决于许多因素。从测试结果看，当N>=10000时，Select和Where运算的加速比基本稳定在3，但如果在select和where的函数中加入更耗时的操作，加速比将能达到3.5左右。

