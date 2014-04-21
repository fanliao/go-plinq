go-plinq
========

PLINQ library for go, support the lazy evaluated and can use channel as source


## Outstanding Linq operatoin:
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

## outstanding issue:
1. KeepOrder() cannot correctly run in all operation

## outstanding items:
1. performance testing
2. Doc

## Performance testing result

N = 100
<table>
  <tr>
    <th>The size of chunk</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
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

Count 100

   | where | select | groupby | distinct | join   | union  | except | reverse | aggregate
:- | :---: | -----: | ------: | -------: | -----: | -----: | -----: | ------: | ------: |
20 |128488 |178262  |742692   |383964    |831622  |587263  |602084  |76692    |117172
100|42081  |137802  |1250438  |1069072   |2088570 |585460  |578650  |26158    |25207
   |41636  |139851  |         |936898    |4019915 |228010  |168172  |24150    |22684

N = 500
<table>
  <tr>
    <th>The size of chunk</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>180765 </td><td>389572  </td><td>3263799  </td><td>1687482   </td><td>2329424 </td><td>1635904 </td><td>1991927 </td><td>95840    </td><td>124983</td>
  </tr>
  <tr>
    <td>500</td><td>132494 </td><td>599481  </td><td>4106050  </td><td>4670868   </td><td>7226622 </td><td>2025977 </td><td>2166186 </td><td>42242    </td><td>77313</td>
  </tr>
  <tr>
    <td>go-linq</td><td>211006 </td><td>691347  </td><td>         </td><td>22673300  </td><td>92962510</td><td>1063429 </td><td>920535  </td><td>119849   </td><td>111789</td>
  </tr>
</table>

Count 500
   | where | select | groupby | distinct | join   | union  | except | reverse | aggregate
---|-------|--------|---------|----------|--------|--------|--------|---------|----------
100|180765 |389572  |3263799  |1687482   |2329424 |1635904 |1991927 |95840    |124983
500|132494 |599481  |4106050  |4670868   |7226622 |2025977 |2166186 |42242    |77313
   |211006 |691347  |         |22673300  |92962510|1063429 |920535  |119849   |111789

N = 1000
<table>
  <tr>
    <th>The size of chunk</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>263987 </td><td>587863  </td><td>2360470  </td><td>2048010   </td><td>4422503  </td><td>2833165</td><td>3569247 </td><td>114768   </td><td>159334</td>
  </tr>
  <tr>
    <td>500</td><td>223528 </td><td>742491  </td><td>6459510  </td><td>6090957   </td><td>11717234 </td><td>2976377</td><td>3663385 </td><td>65215    </td><td>151022</td>
  </tr>
  <tr>
    <td>go-linq</td><td>348171 </td><td>1335764 </td><td>         </td><td>91033790  </td><td>369944320</td><td>2176619</td><td>1883032 </td><td>233867   </td><td>220715</td>
  </tr>
</table>

Count 1000
   | where | select | groupby | distinct | join    | union | except | reverse | aggregate
---|-------|--------|---------|----------|---------|-------|--------|---------|----------
100|263987 |587863  |2360470  |2048010   |4422503  |2833165|3569247 |114768   |159334
500|223528 |742491  |6459510  |6090957   |11717234 |2976377|3663385 |65215    |151022
   |348171 |1335764 |         |91033790  |369944320|2176619|1883032 |233867   |220715

N = 10000
<table>
  <tr>
    <th>The size of chunk</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>1228806</td><td>4118052 </td><td>19458598 </td><td>13039161  </td><td>39417932   </td><td>30144323</td><td>33959925</td><td>437343 </td><td>696423</td>
  </tr>
  <tr>
    <td>500</td><td>896318 </td><td>4156110 </td><td>19749033 </td><td>12358191  </td><td>37515182   </td><td>27840936</td><td>33399107</td><td>414909 </td><td>471292</td>
  </tr>
  <tr>
    <td>go-linq</td><td>3604853</td><td>13508824</td><td>         </td><td>9035282000</td><td>37626549200</td><td>27962687</td><td>18442864</td><td>2379541</td><td>2210405</td>
  </tr>
</table>


Count 10000
   | where | select | groupby | distinct | join      | union | except |reverse | aggregate
---|-------|--------|---------|----------|-----------|-------|--------|--------|---------
100|1228806|4118052 |19458598 |13039161  |39417932   |30144323|33959925|437343 |696423
500|896318 |4156110 |19749033 |12358191  |37515182   |27840936|33399107|414909 |471292
   |3604853|13508824|         |9035282000|37626549200|27962687|18442864|2379541|2210405

N = 100000
<table>
  <tr>
    <th>The size of chunk</th><th> where </th><th> select </th><th> groupby </th><th> distinct </th><th> join   </th><th> union  </th><th> except </th><th> reverse </th><th> aggregate</th>
  </tr>
  <tr>
    <td>100</td><td>9949631</td><td>39438016 </td><td>182067500 </td><td>119025020</td><td>382763000 </td><td>261184280</td><td>337719020</td><td>3112578 </td><td>6299257</td>
  </tr>
  <tr>
    <td>500</td><td>6361361 </td><td>37463910 </td><td>176259040</td><td>109611140</td><td>355923860 </td><td>228035690</td><td>327882800</td><td>2588805 </td><td>3791571</td>
  </tr>
  <tr>
    <td>go-linq</td><td>31383368</td><td>132549770</td><td>None</td><td>       None </td>None<td>None    </td><td>289903640</td><td>201013840</td><td>22245279</td><td>21519150</td>
  </tr>
</table>


Count 100000
   | where | select | groupby | distinct | join      | union | except |reverse | aggregate
---|-------|--------|---------|----------|-----------|-------|--------|--------|---------
100|9949631|39438016 |182067500 |119025020|382763000 |261184280|337719020|3112578 |6299257
500|6361361 |37463910 |176259040|109611140|355923860 |228035690|327882800|2588805 |3791571
   |31383368|132549770|         |        |          |289903640|201013840|22245279|21519150
