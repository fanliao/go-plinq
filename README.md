go-plinq
========

PLINQ library for go, support the lazy evaluated and can use channel as source

## 主要特性

* 延迟查询

* 并行查询

* 除了支持slice，也支持channel作为数据源和输出

## 已经实现的linq查询运算符

* 排序运算符：OrderBy, Reverse

* 集合运算符：Distinct, Except, Intersect, Union

* 筛选运算符：Where

* 投影运算符：Select, SeleteMany

* 连接运算符：Join, LeftJoin, GroupJoin, LeftGroupJoin

* 分组运算符：GroupBy

* 串联运算符：Concat

* 聚合运算符：Aggregate, Average, Count, Max, Min, Sum

* 过滤操作符：Skip, SkipWhile, Take, TakeWhile

* 元素运算符：ElementAt, First, FirstBy, Last, LastBy

## 文档（完善中）

* [GoDoc at godoc.org](http://godoc.org/github.com/fanliao/go-plinq)

Wiki:

1. [Install & Import](https://github.com/fanliao/go-plinq/wiki/Install-&-Import)
2. [Quickstart](https://github.com/fanliao/go-plinq/wiki/Quickstart)
3. [Table of Query Functions](https://github.com/fanliao/go-plinq/wiki/Query-Functions)
4. Customize Parallel Mechanism (outstanding)
5. [Performance] (https://github.com/fanliao/go-plinq/wiki/Performance)
6. Remarks & Notes (outstanding)
7. FAQ (outstanding)

## LICENSE

go-plinq is licensed under the MIT Licence,
(http://www.apache.org/licenses/LICENSE-2.0.html).



