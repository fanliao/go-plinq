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
* 支持Channel作为查询数据源
* 支持返回Channel作为查询结果（未完成。。。）


## 尚未实现的linq查询运算符:
* 过滤操作符：
Skip, SkipWhile, Take, TakeWhile

* 选择：
SeleteMany

* 量词/Quantifiers：
Contains
All, Any, AnyWith, 
SequenceEqual

* 生成器方法
Range， Repeat

* 元素运算符/Element Operators
ElementAt, First, FirstBy, Last, LastBy, Single, 

## 文档（未完成。。。）:

## 性能测试

性能测试说明：

Chunk Size是指plinq内部分割数据的数据块的大小，如果Chunk Size>=N，那将退化为串行的linq查询。

go-linq的并行模式太过简陋，所以在这里没有进行比较。绝大多数情况下，go-linq的并行模式比串行更慢。

### 性能测试结果

N = 100
<table>
  <tr>
    <th>Chunk Size</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>20 </td><td>128488 </td><td>178262  </td><td>742692   </td><td>383964    </td><td>831622  </td><td>587263  </td><td>602084  </td><td>76692    </td><td>117172</td>
  </tr>
  <tr>
    <td>100</td><td>42081  </td><td>137802  </td><td>1250438  </td><td>1069072   </td><td>2088570 </td><td>585460  </td><td>578650  </td><td>26158    </td><td>25207</td>
  </tr>
  <tr>
    <td>go-linq</td><td>41636  </td><td>139851  </td><td>None         </td><td>936898    </td><td>4019915 </td><td>228010  </td><td>168172  </td><td>24150    </td><td>22684</td>
  </tr>
</table>

N = 500
<table>
  <tr>
    <th>Chunk Size</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>180765 </td><td>389572  </td><td>3263799  </td><td>1687482   </td><td>2329424 </td><td>1635904 </td><td>1991927 </td><td>95840    </td><td>124983</td>
  </tr>
  <tr>
    <td>500</td><td>132494 </td><td>599481  </td><td>4106050  </td><td>4670868   </td><td>7226622 </td><td>2025977 </td><td>2166186 </td><td>42242    </td><td>77313</td>
  </tr>
  <tr>
    <td>go-linq</td><td>211006 </td><td>691347  </td><td>N/A</td><td>22673300  </td><td>92962510</td><td>1063429 </td><td>920535  </td><td>119849   </td><td>111789</td>
  </tr>
</table>

N = 1000
<table>
  <tr>
    <th>Chunk Size</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>263987 </td><td>587863  </td><td>2360470  </td><td>2048010   </td><td>4422503  </td><td>2833165</td><td>3569247 </td><td>114768   </td><td>159334</td>
  </tr>
  <tr>
    <td>500</td><td>223528 </td><td>742491  </td><td>6459510  </td><td>6090957   </td><td>11717234 </td><td>2976377</td><td>3663385 </td><td>65215    </td><td>151022</td>
  </tr>
  <tr>
    <td>go-linq</td><td>348171 </td><td>1335764 </td><td>N/A </td><td>91033790  </td><td>369944320</td><td>2176619</td><td>1883032 </td><td>233867   </td><td>220715</td>
  </tr>
</table>

N = 10000
<table>
  <tr>
    <th>Chunk Size</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>1228806</td><td>4118052 </td><td>19458598 </td><td>13039161  </td><td>39417932   </td><td>30144323</td><td>33959925</td><td>437343 </td><td>696423</td>
  </tr>
  <tr>
    <td>500</td><td>896318 </td><td>4156110 </td><td>19749033 </td><td>12358191  </td><td>37515182   </td><td>27840936</td><td>33399107</td><td>414909 </td><td>471292</td>
  </tr>
  <tr>
    <td>go-linq</td><td>3604853</td><td>13508824</td><td>N/A</td><td>9035282000</td><td>37626549200</td><td>27962687</td><td>18442864</td><td>2379541</td><td>2210405</td>
  </tr>
</table>

N = 100000
<table>
  <tr>
    <th>Chunk Size</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>9949631</td><td>39438016 </td><td>182067500 </td><td>119025020</td><td>382763000 </td><td>261184280</td><td>337719020</td><td>3112578 </td><td>6299257</td>
  </tr>
  <tr>
    <td>500</td><td>6361361 </td><td>37463910 </td><td>176259040</td><td>109611140</td><td>355923860 </td><td>228035690</td><td>327882800</td><td>2588805 </td><td>3791571</td>
  </tr>
  <tr>
    <td>go-linq</td><td>31383368</td><td>132549770</td><td>N/A</td><td>N/A</td><td>N/A    </td><td>289903640</td><td>201013840</td><td>22245279</td><td>21519150</td>
  </tr>
</table>

