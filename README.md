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

Count 500
   | where | select | groupby | distinct | join   | union  | except | reverse | aggregate
---|-------|--------|---------|----------|--------|--------|--------|---------|----------
100|154026 |339498  |3894728  |2263331   |2410543 |1582325 |1961883 |95840    |108058
500|109160 |578851  |4027921  |4632809   |7226622 |1966892 |2166186 |42242    |77313
   |211006 |691347  |         |22673300  |92962510|1063429 |920535  |119849   |111789

Count 1000
   | where | select | groupby | distinct | join    | union | except | reverse | aggregate
---|-------|--------|---------|----------|---------|-------|--------|---------|----------
100|205201 |543198  |6016839  |3028458   |4528656  |2833165|3569247 |114768   |125284
500|210610 |740288  |4898204  |6287241   |6038874  |2983386|3611309 |61470    |132595
   |348171 |1335764 |         |91033790  |369944320|2176619|1883032 |233867   |220715

Count 10000
   | where | select | groupby | distinct | join      | union | except |reverse | aggregate
---|-------|--------|---------|----------|-----------|-------|--------|--------|---------
100|713649 |3981852 |22392942 |15102187  |41080408   |30084219|33028547|413407 |414208
500|1171225|7422909 |26158432 |15823269  |48451182   |29762603|33298952|408200 |416211
   |4252249|17045065|         |9473936800|37937184500|27410449|18442864|2379541|2210405