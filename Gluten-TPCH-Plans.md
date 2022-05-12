# Gluten TPCH Plans

##  目录
* [Q1: 价格统计报告查询](#q1: 价格统计报告查询 ✓)
* [Q2: 最小代价供货商查询](#q2: 最小代价供货商查询)
* [Q3: 运送优先级查询](#Q3: 发货优先级查询 ✓)
* [Q4: 订单优先级查询](#q4: 订单优先级查询)
* [Q5: 某地区供货商为公司带来的收入查询](#q5: 某地区供货商为公司带来的收入查询 ✓)
* [Q6: 预测收入变化查询](#q6: 预测收入变化查询 ✓)
* [Q7: 货运盈利情况查询](#q7: 货运盈利情况查询)
* [Q8: 国家市场份额查询](#q8: 国家市场份额查询)
* [Q9: 产品类型利润估量查询](#q9: 产品类型利润估量查询  ✓)
* [Q10: 货运存在问题的查询](#q10: 货运存在问题的查询 ✓)
* [Q11: 库存价值查询](#q11: 库存价值查询)
* [Q12: 货运模式和订单优先级查询](#q12: 货运模式和订单优先级查询)
* [Q13: 消费者订单数量查询](#Q13: 消费者订单数量查询 ✓)
* [Q14: 促销效果查询](#Q14: 促销效果查询 ✓)
* [Q15: 头等供货商查询](#q15: 头等供货商查询)
* [Q16: 零件供货商关系查询](#q16: 零件供货商关系查询)
* [Q17: 小订单收入查询](#q17: 小订单收入查询)
* [Q18: 大订单顾客查询](#q18: 大订单顾客查询)
* [Q19: 折扣收入查询](#Q19: 折扣收入查询 ✓)
* [Q20: 供货商竞争力查询](#q20: 供货商竞争力查询)
* [Q21: 不能按时交货的供货商查询](#q21: 不能按时交货的供货商查询)
* [Q22: 全球销售机会查询](#q22: 全球销售机会查询)

## Q1: 价格统计报告查询 ✓
* Q1 语句是查询 lineItems 的一个定价总结报告。在单个表 lineitem 上查询某个时间段内，对**已付款**、**已运送**的等各类商品进行统计，包括业务量的计费、发货、折扣、税、平均价格等信息。
* Q1 语句的特点是：带有分组、排序、聚集操作并存的单表查询操作。这个查询会导致表上的数据有 95% 到 97% 行被读取到。
````mysql
select
	l_returnflag,  #返回标志
	l_linestatus,
	sum(l_quantity) as sum_qty, #总的数量
	sum(l_extendedprice) as sum_base_price, #聚集函数操作
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order #每个分组所包含的行数
from
	lineitem
where
	l_shipdate <= date_sub('1998-12-01', interval 90 day) #时间段是随机生成的
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
````

````
== Parsed Logical Plan ==
'Sort ['l_returnflag ASC NULLS FIRST, 'l_linestatus ASC NULLS FIRST], true
+- 'Aggregate ['l_returnflag, 'l_linestatus], ['l_returnflag, 'l_linestatus, 'sum('l_quantity) AS sum_qty#0, 'sum('l_extendedprice) AS sum_base_price#1, 'sum(('l_extendedprice * (1 - 'l_discount))) AS sum_disc_price#2, 'sum((('l_extendedprice * (1 - 'l_discount)) * (1 + 'l_tax))) AS sum_charge#3, 'avg('l_quantity) AS avg_qty#4, 'avg('l_extendedprice) AS avg_price#5, 'avg('l_discount) AS avg_disc#6, 'count(1) AS count_order#7]
   +- 'Filter ('l_shipdate <= (10471 - 1 days))
      +- 'UnresolvedRelation [lineitem], [], false

== Analyzed Logical Plan ==
l_returnflag: string, l_linestatus: string, sum_qty: double, sum_base_price: double, sum_disc_price: double, sum_charge: double, avg_qty: double, avg_price: double, avg_disc: double, count_order: bigint
Project [l_returnflag#17, l_linestatus#18, sum_qty#0, sum_base_price#1, sum_disc_price#2, sum_charge#3, avg_qty#4, avg_price#5, avg_disc#6, count_order#7L]
+- Sort [l_returnflag#17 ASC NULLS FIRST, l_linestatus#18 ASC NULLS FIRST], true
   +- Aggregate [l_returnflag#17, l_linestatus#18], [l_returnflag#17, l_linestatus#18, sum(l_quantity#13) AS sum_qty#0, sum(l_extendedprice#14) AS sum_base_price#1, sum((l_extendedprice#14 * (cast(1 as double) - l_discount#15))) AS sum_disc_price#2, sum(((l_extendedprice#14 * (cast(1 as double) - l_discount#15)) * (cast(1 as double) + l_tax#16))) AS sum_charge#3, avg(l_quantity#13) AS avg_qty#4, avg(l_extendedprice#14) AS avg_price#5, avg(l_discount#15) AS avg_disc#6, count(1) AS count_order#7L]
      +- Filter (l_shipdate#19 <= 10471 - 1 days)
         +- SubqueryAlias spark_catalog.default.lineitem
            +- Relation[l_orderkey#9L,l_partkey#10L,l_suppkey#11L,l_linenumber#12L,l_quantity#13,l_extendedprice#14,l_discount#15,l_tax#16,l_returnflag#17,l_linestatus#18,l_shipdate#19,l_commitdate#20,l_receiptdate#21,l_shipinstruct#22,l_shipmode#23,l_comment#24] parquet

== Optimized Logical Plan ==
Sort [l_returnflag#17 ASC NULLS FIRST, l_linestatus#18 ASC NULLS FIRST], true
+- Aggregate [l_returnflag#17, l_linestatus#18], [l_returnflag#17, l_linestatus#18, sum(l_quantity#13) AS sum_qty#0, sum(l_extendedprice#14) AS sum_base_price#1, sum((l_extendedprice#14 * (1.0 - l_discount#15))) AS sum_disc_price#2, sum(((l_extendedprice#14 * (1.0 - l_discount#15)) * (1.0 + l_tax#16))) AS sum_charge#3, avg(l_quantity#13) AS avg_qty#4, avg(l_extendedprice#14) AS avg_price#5, avg(l_discount#15) AS avg_disc#6, count(1) AS count_order#7L]
   +- Project [l_quantity#13, l_extendedprice#14, l_discount#15, l_tax#16, l_returnflag#17, l_linestatus#18]
      +- Filter (isnotnull(l_shipdate#19) AND (l_shipdate#19 <= 10470))
         +- Relation[l_orderkey#9L,l_partkey#10L,l_suppkey#11L,l_linenumber#12L,l_quantity#13,l_extendedprice#14,l_discount#15,l_tax#16,l_returnflag#17,l_linestatus#18,l_shipdate#19,l_commitdate#20,l_receiptdate#21,l_shipinstruct#22,l_shipmode#23,l_comment#24] parquet

== Physical Plan ==
*(1) Sort [l_returnflag#17 ASC NULLS FIRST, l_linestatus#18 ASC NULLS FIRST], true, 0
+- *(1) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(l_returnflag#17 ASC NULLS FIRST, l_linestatus#18 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#50], [id=#50], [OUTPUT] List(l_returnflag:StringType, l_linestatus:StringType, sum_qty:DoubleType, sum_base_price:DoubleType, sum_disc_price:DoubleType, sum_charge:DoubleType, avg_qty:DoubleType, avg_price:DoubleType, avg_disc:DoubleType, count_order:LongType), [OUTPUT] List(l_returnflag:StringType, l_linestatus:StringType, sum_qty:DoubleType, sum_base_price:DoubleType, sum_disc_price:DoubleType, sum_charge:DoubleType, avg_qty:DoubleType, avg_price:DoubleType, avg_disc:DoubleType, count_order:LongType)
         +- *(2) HashAggregateTransformer(keys=[l_returnflag#17, l_linestatus#18], functions=[sum(l_quantity#13), sum(l_extendedprice#14), sum((l_extendedprice#14 * (1.0 - l_discount#15))), sum(((l_extendedprice#14 * (1.0 - l_discount#15)) * (1.0 + l_tax#16))), avg(l_quantity#13), avg(l_extendedprice#14), avg(l_discount#15), count(1)], output=[l_returnflag#17, l_linestatus#18, sum_qty#0, sum_base_price#1, sum_disc_price#2, sum_charge#3, avg_qty#4, avg_price#5, avg_disc#6, count_order#7L])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(l_returnflag#17, l_linestatus#18, 1), ENSURE_REQUIREMENTS, [id=#45], [id=#45], [OUTPUT] ArrayBuffer(l_returnflag:StringType, l_linestatus:StringType, sum:DoubleType, sum:DoubleType, sum:DoubleType, sum:DoubleType, sum:DoubleType, count:LongType, sum:DoubleType, count:LongType, sum:DoubleType, count:LongType, count:LongType), [OUTPUT] ArrayBuffer(l_returnflag:StringType, l_linestatus:StringType, sum:DoubleType, sum:DoubleType, sum:DoubleType, sum:DoubleType, sum:DoubleType, count:LongType, sum:DoubleType, count:LongType, sum:DoubleType, count:LongType, count:LongType)
                  +- *(1) HashAggregateTransformer(keys=[l_returnflag#17, l_linestatus#18], functions=[partial_sum(l_quantity#13), partial_sum(l_extendedprice#14), partial_sum((l_extendedprice#14 * (1.0 - l_discount#15))), partial_sum(((l_extendedprice#14 * (1.0 - l_discount#15)) * (1.0 + l_tax#16))), partial_avg(l_quantity#13), partial_avg(l_extendedprice#14), partial_avg(l_discount#15), partial_count(1)], output=[l_returnflag#17, l_linestatus#18, sum#71, sum#72, sum#73, sum#74, sum#75, count#76L, sum#77, count#78L, sum#79, count#80L, count#81L])
                     +- *(1) ProjectExecTransformer [l_quantity#13, l_extendedprice#14, l_discount#15, l_tax#16, l_returnflag#17, l_linestatus#18]
                        +- *(1) FilterExecTransformer (isnotnull(l_shipdate#19) AND (l_shipdate#19 <= 10470))
                           +- *(1) FileScan parquet default.lineitem[l_quantity#13,l_extendedprice#14,l_discount#15,l_tax#16,l_returnflag#17,l_linestatus#18,l_shipdate#19] Batched: true, DataFilters: [isnotnull(l_shipdate#19), (l_shipdate#19 <= 10470)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), LessThanOrEqual(l_shipdate,1998-09-01)], ReadSchema: struct<l_quantity:double,l_extendedprice:double,l_discount:double,l_tax:double,l_returnflag:strin...

````

[返回目录](#目录)

## Q2: 最小代价供货商查询
* Q2 语句查询获得最小代价的供货商。得到给定的区域内，对于指定的零件（某一类型和大小的零件），哪个供应者能以最低的价格供应它，就可以选择哪个供应者来订货。
* Q2 语句的特点是：带有排序、聚集操作、子查询并存的多表查询操作。查询语句没有从语法上限制返回多少条元组，但是 TPC-H 标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。
* 有一个 scalar sub-query
````mysql
select
	s_acctbal, # 帐户余额
	s_name, # 名字
	n_name, # 国家
	p_partkey, # 零件的号码
	p_mfgr, # 生产者
	s_address, # 供应者的地址
	s_phone, # 电话号码
	s_comment # 备注信息
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15 # 指定大小，在区间[1, 50]内随机选择
	and p_type like '%BRASS' # 指定类型，在TPC-H标准指定的范围内随机选择
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE' # 指定地区，在TPC-H标准指定的范围内随机选择
	and ps_supplycost = (
		select
			min(ps_supplycost) # 聚集函数
		from   # 与父查询的表有重叠
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
````

````
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['s_acctbal DESC NULLS LAST, 'n_name ASC NULLS FIRST, 's_name ASC NULLS FIRST, 'p_partkey ASC NULLS FIRST], true
      +- 'Project ['s_acctbal, 's_name, 'n_name, 'p_partkey, 'p_mfgr, 's_address, 's_phone, 's_comment]
         +- 'Filter (((('p_partkey = 'ps_partkey) AND ('s_suppkey = 'ps_suppkey)) AND (('p_size = 15) AND 'p_type LIKE %BRASS)) AND ((('s_nationkey = 'n_nationkey) AND ('n_regionkey = 'r_regionkey)) AND (('r_name = EUROPE) AND ('ps_supplycost = scalar-subquery#82 []))))
            :  +- 'Project [unresolvedalias('min('ps_supplycost), None)]
            :     +- 'Filter (((('p_partkey = 'ps_partkey) AND ('s_suppkey = 'ps_suppkey)) AND ('s_nationkey = 'n_nationkey)) AND (('n_regionkey = 'r_regionkey) AND ('r_name = EUROPE)))
            :        +- 'Join Inner
            :           :- 'Join Inner
            :           :  :- 'Join Inner
            :           :  :  :- 'UnresolvedRelation [partsupp], [], false
            :           :  :  +- 'UnresolvedRelation [supplier], [], false
            :           :  +- 'UnresolvedRelation [nation], [], false
            :           +- 'UnresolvedRelation [region], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'UnresolvedRelation [part], [], false
               :  :  :  +- 'UnresolvedRelation [supplier], [], false
               :  :  +- 'UnresolvedRelation [partsupp], [], false
               :  +- 'UnresolvedRelation [nation], [], false
               +- 'UnresolvedRelation [region], [], false

== Analyzed Logical Plan ==
s_acctbal: double, s_name: string, n_name: string, p_partkey: bigint, p_mfgr: string, s_address: string, s_phone: string, s_comment: string
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_acctbal#97 DESC NULLS LAST, n_name#105 ASC NULLS FIRST, s_name#93 ASC NULLS FIRST, p_partkey#83L ASC NULLS FIRST], true
      +- Project [s_acctbal#97, s_name#93, n_name#105, p_partkey#83L, p_mfgr#85, s_address#94, s_phone#96, s_comment#98]
         +- Filter ((((p_partkey#83L = ps_partkey#99L) AND (s_suppkey#92L = ps_suppkey#100L)) AND ((p_size#88L = cast(15 as bigint)) AND p_type#87 LIKE %BRASS)) AND (((s_nationkey#95L = n_nationkey#104L) AND (n_regionkey#106L = r_regionkey#108L)) AND ((r_name#109 = EUROPE) AND (ps_supplycost#102 = scalar-subquery#82 [p_partkey#83L]))))
            :  +- Aggregate [min(ps_supplycost#114) AS min(ps_supplycost)#150]
            :     +- Filter ((((outer(p_partkey#83L) = ps_partkey#111L) AND (s_suppkey#116L = ps_suppkey#112L)) AND (s_nationkey#119L = n_nationkey#123L)) AND ((n_regionkey#125L = r_regionkey#127L) AND (r_name#128 = EUROPE)))
            :        +- Join Inner
            :           :- Join Inner
            :           :  :- Join Inner
            :           :  :  :- SubqueryAlias spark_catalog.default.partsupp
            :           :  :  :  +- Relation[ps_partkey#111L,ps_suppkey#112L,ps_availqty#113L,ps_supplycost#114,ps_comment#115] parquet
            :           :  :  +- SubqueryAlias spark_catalog.default.supplier
            :           :  :     +- Relation[s_suppkey#116L,s_name#117,s_address#118,s_nationkey#119L,s_phone#120,s_acctbal#121,s_comment#122] parquet
            :           :  +- SubqueryAlias spark_catalog.default.nation
            :           :     +- Relation[n_nationkey#123L,n_name#124,n_regionkey#125L,n_comment#126] parquet
            :           +- SubqueryAlias spark_catalog.default.region
            :              +- Relation[r_regionkey#127L,r_name#128,r_comment#129] parquet
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- Join Inner
               :  :  :  :- SubqueryAlias spark_catalog.default.part
               :  :  :  :  +- Relation[p_partkey#83L,p_name#84,p_mfgr#85,p_brand#86,p_type#87,p_size#88L,p_container#89,p_retailprice#90,p_comment#91] parquet
               :  :  :  +- SubqueryAlias spark_catalog.default.supplier
               :  :  :     +- Relation[s_suppkey#92L,s_name#93,s_address#94,s_nationkey#95L,s_phone#96,s_acctbal#97,s_comment#98] parquet
               :  :  +- SubqueryAlias spark_catalog.default.partsupp
               :  :     +- Relation[ps_partkey#99L,ps_suppkey#100L,ps_availqty#101L,ps_supplycost#102,ps_comment#103] parquet
               :  +- SubqueryAlias spark_catalog.default.nation
               :     +- Relation[n_nationkey#104L,n_name#105,n_regionkey#106L,n_comment#107] parquet
               +- SubqueryAlias spark_catalog.default.region
                  +- Relation[r_regionkey#108L,r_name#109,r_comment#110] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [s_acctbal#97 DESC NULLS LAST, n_name#105 ASC NULLS FIRST, s_name#93 ASC NULLS FIRST, p_partkey#83L ASC NULLS FIRST], true
      +- Project [s_acctbal#97, s_name#93, n_name#105, p_partkey#83L, p_mfgr#85, s_address#94, s_phone#96, s_comment#98]
         +- Join Inner, (n_regionkey#106L = r_regionkey#108L)
            :- Project [p_partkey#83L, p_mfgr#85, s_name#93, s_address#94, s_phone#96, s_acctbal#97, s_comment#98, n_name#105, n_regionkey#106L]
            :  +- Join Inner, (s_nationkey#95L = n_nationkey#104L)
            :     :- Project [p_partkey#83L, p_mfgr#85, s_name#93, s_address#94, s_nationkey#95L, s_phone#96, s_acctbal#97, s_comment#98]
            :     :  +- Join Inner, (s_suppkey#92L = ps_suppkey#100L)
            :     :     :- Project [p_partkey#83L, p_mfgr#85, ps_suppkey#100L]
            :     :     :  +- Join Inner, ((knownfloatingpointnormalized(normalizenanandzero(ps_supplycost#102)) = knownfloatingpointnormalized(normalizenanandzero(min(ps_supplycost)#150))) AND (p_partkey#83L = ps_partkey#111L))
            :     :     :     :- Project [p_partkey#83L, p_mfgr#85, ps_suppkey#100L, ps_supplycost#102]
            :     :     :     :  +- Join Inner, (p_partkey#83L = ps_partkey#99L)
            :     :     :     :     :- Project [p_partkey#83L, p_mfgr#85]
            :     :     :     :     :  +- Filter ((((isnotnull(p_size#88L) AND isnotnull(p_type#87)) AND (p_size#88L = 15)) AND EndsWith(p_type#87, BRASS)) AND isnotnull(p_partkey#83L))
            :     :     :     :     :     +- Relation[p_partkey#83L,p_name#84,p_mfgr#85,p_brand#86,p_type#87,p_size#88L,p_container#89,p_retailprice#90,p_comment#91] parquet
            :     :     :     :     +- Project [ps_partkey#99L, ps_suppkey#100L, ps_supplycost#102]
            :     :     :     :        +- Filter ((isnotnull(ps_partkey#99L) AND isnotnull(ps_supplycost#102)) AND isnotnull(ps_suppkey#100L))
            :     :     :     :           +- Relation[ps_partkey#99L,ps_suppkey#100L,ps_availqty#101L,ps_supplycost#102,ps_comment#103] parquet
            :     :     :     +- Filter isnotnull(min(ps_supplycost)#150)
            :     :     :        +- Aggregate [ps_partkey#111L], [min(ps_supplycost#114) AS min(ps_supplycost)#150, ps_partkey#111L]
            :     :     :           +- Project [ps_partkey#111L, ps_supplycost#114]
            :     :     :              +- Join Inner, (n_regionkey#125L = r_regionkey#127L)
            :     :     :                 :- Project [ps_partkey#111L, ps_supplycost#114, n_regionkey#125L]
            :     :     :                 :  +- Join Inner, (s_nationkey#119L = n_nationkey#123L)
            :     :     :                 :     :- Project [ps_partkey#111L, ps_supplycost#114, s_nationkey#119L]
            :     :     :                 :     :  +- Join Inner, (s_suppkey#116L = ps_suppkey#112L)
            :     :     :                 :     :     :- Project [ps_partkey#111L, ps_suppkey#112L, ps_supplycost#114]
            :     :     :                 :     :     :  +- Filter (isnotnull(ps_suppkey#112L) AND isnotnull(ps_partkey#111L))
            :     :     :                 :     :     :     +- Relation[ps_partkey#111L,ps_suppkey#112L,ps_availqty#113L,ps_supplycost#114,ps_comment#115] parquet
            :     :     :                 :     :     +- Project [s_suppkey#116L, s_nationkey#119L]
            :     :     :                 :     :        +- Filter (isnotnull(s_suppkey#116L) AND isnotnull(s_nationkey#119L))
            :     :     :                 :     :           +- Relation[s_suppkey#116L,s_name#117,s_address#118,s_nationkey#119L,s_phone#120,s_acctbal#121,s_comment#122] parquet
            :     :     :                 :     +- Project [n_nationkey#123L, n_regionkey#125L]
            :     :     :                 :        +- Filter (isnotnull(n_nationkey#123L) AND isnotnull(n_regionkey#125L))
            :     :     :                 :           +- Relation[n_nationkey#123L,n_name#124,n_regionkey#125L,n_comment#126] parquet
            :     :     :                 +- Project [r_regionkey#127L]
            :     :     :                    +- Filter ((isnotnull(r_name#128) AND (r_name#128 = EUROPE)) AND isnotnull(r_regionkey#127L))
            :     :     :                       +- Relation[r_regionkey#127L,r_name#128,r_comment#129] parquet
            :     :     +- Filter (isnotnull(s_suppkey#92L) AND isnotnull(s_nationkey#95L))
            :     :        +- Relation[s_suppkey#92L,s_name#93,s_address#94,s_nationkey#95L,s_phone#96,s_acctbal#97,s_comment#98] parquet
            :     +- Project [n_nationkey#104L, n_name#105, n_regionkey#106L]
            :        +- Filter (isnotnull(n_nationkey#104L) AND isnotnull(n_regionkey#106L))
            :           +- Relation[n_nationkey#104L,n_name#105,n_regionkey#106L,n_comment#107] parquet
            +- Project [r_regionkey#108L]
               +- Filter ((isnotnull(r_name#109) AND (r_name#109 = EUROPE)) AND isnotnull(r_regionkey#108L))
                  +- Relation[r_regionkey#108L,r_name#109,r_comment#110] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_acctbal#97 DESC NULLS LAST,n_name#105 ASC NULLS FIRST,s_name#93 ASC NULLS FIRST,p_partkey#83L ASC NULLS FIRST], output=[s_acctbal#97,s_name#93,n_name#105,p_partkey#83L,p_mfgr#85,s_address#94,s_phone#96,s_comment#98])
+- *(4) ColumnarToRow
   +- *(19) ProjectExecTransformer [s_acctbal#97, s_name#93, n_name#105, p_partkey#83L, p_mfgr#85, s_address#94, s_phone#96, s_comment#98]
      +- *(19) ShuffledHashJoinExecTransformer [n_regionkey#106L], [r_regionkey#108L], Inner, BuildRight
         :- CoalesceBatches
         :  +- ColumnarExchange hashpartitioning(n_regionkey#106L, 1), ENSURE_REQUIREMENTS, [id=#622], [id=#622], [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, s_name:StringType, s_address:StringType, s_phone:StringType, s_acctbal:DoubleType, s_comment:StringType, n_name:StringType, n_regionkey:LongType), [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, s_name:StringType, s_address:StringType, s_phone:StringType, s_acctbal:DoubleType, s_comment:StringType, n_name:StringType, n_regionkey:LongType)
         :     +- *(17) ProjectExecTransformer [p_partkey#83L, p_mfgr#85, s_name#93, s_address#94, s_phone#96, s_acctbal#97, s_comment#98, n_name#105, n_regionkey#106L]
         :        +- *(17) ShuffledHashJoinExecTransformer [s_nationkey#95L], [n_nationkey#104L], Inner, BuildRight
         :           :- CoalesceBatches
         :           :  +- ColumnarExchange hashpartitioning(s_nationkey#95L, 1), ENSURE_REQUIREMENTS, [id=#616], [id=#616], [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, s_name:StringType, s_address:StringType, s_nationkey:LongType, s_phone:StringType, s_acctbal:DoubleType, s_comment:StringType), [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, s_name:StringType, s_address:StringType, s_nationkey:LongType, s_phone:StringType, s_acctbal:DoubleType, s_comment:StringType)
         :           :     +- *(15) ProjectExecTransformer [p_partkey#83L, p_mfgr#85, s_name#93, s_address#94, s_nationkey#95L, s_phone#96, s_acctbal#97, s_comment#98]
         :           :        +- *(15) ShuffledHashJoinExecTransformer [ps_suppkey#100L], [s_suppkey#92L], Inner, BuildRight
         :           :           :- CoalesceBatches
         :           :           :  +- ColumnarExchange hashpartitioning(ps_suppkey#100L, 1), ENSURE_REQUIREMENTS, [id=#610], [id=#610], [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, ps_suppkey:LongType), [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, ps_suppkey:LongType)
         :           :           :     +- *(13) ProjectExecTransformer [p_partkey#83L, p_mfgr#85, ps_suppkey#100L]
         :           :           :        +- *(13) ShuffledHashJoinExecTransformer [knownfloatingpointnormalized(normalizenanandzero(ps_supplycost#102)), p_partkey#83L], [knownfloatingpointnormalized(normalizenanandzero(min(ps_supplycost)#150)), ps_partkey#111L], Inner, BuildLeft
         :           :           :           :- CoalesceBatches
         :           :           :           :  +- ColumnarExchange hashpartitioning(knownfloatingpointnormalized(normalizenanandzero(ps_supplycost#102)), p_partkey#83L, 1), ENSURE_REQUIREMENTS, [id=#586], [id=#586], [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, ps_suppkey:LongType, ps_supplycost:DoubleType), [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType, ps_suppkey:LongType, ps_supplycost:DoubleType)
         :           :           :           :     +- *(4) ProjectExecTransformer [p_partkey#83L, p_mfgr#85, ps_suppkey#100L, ps_supplycost#102]
         :           :           :           :        +- *(4) ShuffledHashJoinExecTransformer [p_partkey#83L], [ps_partkey#99L], Inner, BuildLeft
         :           :           :           :           :- CoalesceBatches
         :           :           :           :           :  +- ColumnarExchange hashpartitioning(p_partkey#83L, 1), ENSURE_REQUIREMENTS, [id=#580], [id=#580], [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType), [OUTPUT] List(p_partkey:LongType, p_mfgr:StringType)
         :           :           :           :           :     +- *(2) ProjectExecTransformer [p_partkey#83L, p_mfgr#85]
         :           :           :           :           :        +- RowToColumnar
         :           :           :           :           :           +- *(1) Filter ((((isnotnull(p_size#88L) AND isnotnull(p_type#87)) AND (p_size#88L = 15)) AND EndsWith(p_type#87, BRASS)) AND isnotnull(p_partkey#83L))
         :           :           :           :           :              +- *(1) ColumnarToRow
         :           :           :           :           :                 +- *(1) FileScan parquet default.part[p_partkey#83L,p_mfgr#85,p_type#87,p_size#88L] Batched: true, DataFilters: [isnotnull(p_size#88L), isnotnull(p_type#87), (p_size#88L = 15), EndsWith(p_type#87, BRASS), isno..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_size), IsNotNull(p_type), EqualTo(p_size,15), StringEndsWith(p_type,BRASS), IsNotNul..., ReadSchema: struct<p_partkey:bigint,p_mfgr:string,p_type:string,p_size:bigint>
         :           :           :           :           +- CoalesceBatches
         :           :           :           :              +- ColumnarExchange hashpartitioning(ps_partkey#99L, 1), ENSURE_REQUIREMENTS, [id=#483], [id=#483], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_supplycost:DoubleType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_supplycost:DoubleType)
         :           :           :           :                 +- *(3) FilterExecTransformer ((isnotnull(ps_partkey#99L) AND isnotnull(ps_supplycost#102)) AND isnotnull(ps_suppkey#100L))
         :           :           :           :                    +- *(3) FileScan parquet default.partsupp[ps_partkey#99L,ps_suppkey#100L,ps_supplycost#102] Batched: true, DataFilters: [isnotnull(ps_partkey#99L), isnotnull(ps_supplycost#102), isnotnull(ps_suppkey#100L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_partkey), IsNotNull(ps_supplycost), IsNotNull(ps_suppkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_supplycost:double>
         :           :           :           +- CoalesceBatches
         :           :           :              +- ColumnarExchange hashpartitioning(knownfloatingpointnormalized(normalizenanandzero(min(ps_supplycost)#150)), ps_partkey#111L, 1), ENSURE_REQUIREMENTS, [id=#604], [id=#604], [OUTPUT] List(min(ps_supplycost):DoubleType, ps_partkey:LongType), [OUTPUT] List(min(ps_supplycost):DoubleType, ps_partkey:LongType)
         :           :           :                 +- *(12) FilterExecTransformer isnotnull(min(ps_supplycost)#150)
         :           :           :                    +- RowToColumnar
         :           :           :                       +- *(3) HashAggregate(keys=[ps_partkey#111L], functions=[min(ps_supplycost#114)], output=[min(ps_supplycost)#150, ps_partkey#111L])
         :           :           :                          +- *(3) ColumnarToRow
         :           :           :                             +- CoalesceBatches
         :           :           :                                +- ColumnarExchange hashpartitioning(ps_partkey#111L, 1), ENSURE_REQUIREMENTS, [id=#594], [id=#594], [OUTPUT] List(ps_partkey:LongType, min:DoubleType), [OUTPUT] List(ps_partkey:LongType, min:DoubleType)
         :           :           :                                   +- RowToColumnar
         :           :           :                                      +- *(2) HashAggregate(keys=[ps_partkey#111L], functions=[partial_min(ps_supplycost#114)], output=[ps_partkey#111L, min#169])
         :           :           :                                         +- *(2) ColumnarToRow
         :           :           :                                            +- *(11) ProjectExecTransformer [ps_partkey#111L, ps_supplycost#114]
         :           :           :                                               +- *(11) ShuffledHashJoinExecTransformer [n_regionkey#125L], [r_regionkey#127L], Inner, BuildRight
         :           :           :                                                  :- CoalesceBatches
         :           :           :                                                  :  +- ColumnarExchange hashpartitioning(n_regionkey#125L, 1), ENSURE_REQUIREMENTS, [id=#513], [id=#513], [OUTPUT] List(ps_partkey:LongType, ps_supplycost:DoubleType, n_regionkey:LongType), [OUTPUT] List(ps_partkey:LongType, ps_supplycost:DoubleType, n_regionkey:LongType)
         :           :           :                                                  :     +- *(9) ProjectExecTransformer [ps_partkey#111L, ps_supplycost#114, n_regionkey#125L]
         :           :           :                                                  :        +- *(9) ShuffledHashJoinExecTransformer [s_nationkey#119L], [n_nationkey#123L], Inner, BuildRight
         :           :           :                                                  :           :- CoalesceBatches
         :           :           :                                                  :           :  +- ColumnarExchange hashpartitioning(s_nationkey#119L, 1), ENSURE_REQUIREMENTS, [id=#503], [id=#503], [OUTPUT] List(ps_partkey:LongType, ps_supplycost:DoubleType, s_nationkey:LongType), [OUTPUT] List(ps_partkey:LongType, ps_supplycost:DoubleType, s_nationkey:LongType)
         :           :           :                                                  :           :     +- *(7) ProjectExecTransformer [ps_partkey#111L, ps_supplycost#114, s_nationkey#119L]
         :           :           :                                                  :           :        +- *(7) ShuffledHashJoinExecTransformer [ps_suppkey#112L], [s_suppkey#116L], Inner, BuildRight
         :           :           :                                                  :           :           :- CoalesceBatches
         :           :           :                                                  :           :           :  +- ColumnarExchange hashpartitioning(ps_suppkey#112L, 1), ENSURE_REQUIREMENTS, [id=#493], [id=#493], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_supplycost:DoubleType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_supplycost:DoubleType)
         :           :           :                                                  :           :           :     +- *(5) FilterExecTransformer (isnotnull(ps_suppkey#112L) AND isnotnull(ps_partkey#111L))
         :           :           :                                                  :           :           :        +- *(5) FileScan parquet default.partsupp[ps_partkey#111L,ps_suppkey#112L,ps_supplycost#114] Batched: true, DataFilters: [isnotnull(ps_suppkey#112L), isnotnull(ps_partkey#111L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey), IsNotNull(ps_partkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_supplycost:double>
         :           :           :                                                  :           :           +- CoalesceBatches
         :           :           :                                                  :           :              +- ColumnarExchange hashpartitioning(s_suppkey#116L, 1), ENSURE_REQUIREMENTS, [id=#497], [id=#497], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
         :           :           :                                                  :           :                 +- *(6) FilterExecTransformer (isnotnull(s_suppkey#116L) AND isnotnull(s_nationkey#119L))
         :           :           :                                                  :           :                    +- *(6) FileScan parquet default.supplier[s_suppkey#116L,s_nationkey#119L] Batched: true, DataFilters: [isnotnull(s_suppkey#116L), isnotnull(s_nationkey#119L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
         :           :           :                                                  :           +- CoalesceBatches
         :           :           :                                                  :              +- ColumnarExchange hashpartitioning(n_nationkey#123L, 1), ENSURE_REQUIREMENTS, [id=#507], [id=#507], [OUTPUT] List(n_nationkey:LongType, n_regionkey:LongType), [OUTPUT] List(n_nationkey:LongType, n_regionkey:LongType)
         :           :           :                                                  :                 +- *(8) FilterExecTransformer (isnotnull(n_nationkey#123L) AND isnotnull(n_regionkey#125L))
         :           :           :                                                  :                    +- *(8) FileScan parquet default.nation[n_nationkey#123L,n_regionkey#125L] Batched: true, DataFilters: [isnotnull(n_nationkey#123L), isnotnull(n_regionkey#125L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey), IsNotNull(n_regionkey)], ReadSchema: struct<n_nationkey:bigint,n_regionkey:bigint>
         :           :           :                                                  +- CoalesceBatches
         :           :           :                                                     +- ColumnarExchange hashpartitioning(r_regionkey#127L, 1), ENSURE_REQUIREMENTS, [id=#517], [id=#517], [OUTPUT] List(r_regionkey:LongType), [OUTPUT] List(r_regionkey:LongType)
         :           :           :                                                        +- *(10) ProjectExecTransformer [r_regionkey#127L]
         :           :           :                                                           +- *(10) FilterExecTransformer ((isnotnull(r_name#128) AND (r_name#128 = EUROPE)) AND isnotnull(r_regionkey#127L))
         :           :           :                                                              +- *(10) FileScan parquet default.region[r_regionkey#127L,r_name#128] Batched: true, DataFilters: [isnotnull(r_name#128), (r_name#128 = EUROPE), isnotnull(r_regionkey#127L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,EUROPE), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>
         :           :           +- CoalesceBatches
         :           :              +- ColumnarExchange hashpartitioning(s_suppkey#92L, 1), ENSURE_REQUIREMENTS, [id=#544], [id=#544], [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_nationkey:LongType, s_phone:StringType, s_acctbal:DoubleType, s_comment:StringType), [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_nationkey:LongType, s_phone:StringType, s_acctbal:DoubleType, s_comment:StringType)
         :           :                 +- *(14) FilterExecTransformer (isnotnull(s_suppkey#92L) AND isnotnull(s_nationkey#95L))
         :           :                    +- *(14) FileScan parquet default.supplier[s_suppkey#92L,s_name#93,s_address#94,s_nationkey#95L,s_phone#96,s_acctbal#97,s_comment#98] Batched: true, DataFilters: [isnotnull(s_suppkey#92L), isnotnull(s_nationkey#95L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_name:string,s_address:string,s_nationkey:bigint,s_phone:string,s_acctba...
         :           +- CoalesceBatches
         :              +- ColumnarExchange hashpartitioning(n_nationkey#104L, 1), ENSURE_REQUIREMENTS, [id=#554], [id=#554], [OUTPUT] List(n_nationkey:LongType, n_name:StringType, n_regionkey:LongType), [OUTPUT] List(n_nationkey:LongType, n_name:StringType, n_regionkey:LongType)
         :                 +- *(16) FilterExecTransformer (isnotnull(n_nationkey#104L) AND isnotnull(n_regionkey#106L))
         :                    +- *(16) FileScan parquet default.nation[n_nationkey#104L,n_name#105,n_regionkey#106L] Batched: true, DataFilters: [isnotnull(n_nationkey#104L), isnotnull(n_regionkey#106L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey), IsNotNull(n_regionkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string,n_regionkey:bigint>
         +- CoalesceBatches
            +- ReusedExchange [r_regionkey#108L], ColumnarExchange hashpartitioning(r_regionkey#127L, 1), ENSURE_REQUIREMENTS, [id=#517], [id=#517], [OUTPUT] List(r_regionkey:LongType)

````

[返回目录](#目录)

## Q3: 发货优先级查询 ✓
* Q3 语句查询**尚未发货，且潜在收入前 10 的订单**。该查询返回**发送优先级**和**潜在收入**（定义为：`l_extendedprice * (1-l_discount)`），即在指定日期前还没有发货的订单中**潜在收入最大的订单**，按订单的潜在收入降序排列。==如果未发货订单超过 10 个，则仅列出收入最大的 10 个订单==。
* Q3语句的特点是：带有分组、排序、聚集操作并存的三表查询操作。查询语句没有从语法上限制返回多少条元组，但是 TPC-H 标准规定，查询结果只返回前10行（通常依赖于应用程序实现）。
````mysql
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue, # 潜在的收入，聚集操作
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING' # 在TPC-H标准指定的范围内随机选择
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15' # 指定日期段，在在[1995-03-01, 1995-03-31]中随机选择
	and l_shipdate > '1995-03-15'  # 指定日期段，在在[1995-03-01, 1995-03-31]中随机选择
group by
	l_orderkey, # 订单标识
	o_orderdate,  # 订单日期
	o_shippriority # 运输优先级
order by
	revenue desc, # 降序排序，把潜在最大收入列在前面
	o_orderdate
limit 10;
````

````
== Parsed Logical Plan ==
'GlobalLimit 10
+- 'LocalLimit 10
   +- 'Sort ['revenue DESC NULLS LAST, 'o_orderdate ASC NULLS FIRST], true
      +- 'Aggregate ['l_orderkey, 'o_orderdate, 'o_shippriority], ['l_orderkey, 'sum(('l_extendedprice * (1 - 'l_discount))) AS revenue#170, 'o_orderdate, 'o_shippriority]
         +- 'Filter (((('c_mktsegment = BUILDING) AND ('c_custkey = 'o_custkey)) AND ('l_orderkey = 'o_orderkey)) AND (('o_orderdate < 9204) AND ('l_shipdate > 9204)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [customer], [], false
               :  +- 'UnresolvedRelation [orders], [], false
               +- 'UnresolvedRelation [lineitem], [], false

== Analyzed Logical Plan ==
l_orderkey: bigint, revenue: double, o_orderdate: date, o_shippriority: bigint
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#170 DESC NULLS LAST, o_orderdate#183 ASC NULLS FIRST], true
      +- Aggregate [l_orderkey#188L, o_orderdate#183, o_shippriority#186L], [l_orderkey#188L, sum((l_extendedprice#193 * (cast(1 as double) - l_discount#194))) AS revenue#170, o_orderdate#183, o_shippriority#186L]
         +- Filter ((((c_mktsegment#177 = BUILDING) AND (c_custkey#171L = o_custkey#180L)) AND (l_orderkey#188L = o_orderkey#179L)) AND ((o_orderdate#183 < 9204) AND (l_shipdate#198 > 9204)))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias spark_catalog.default.customer
               :  :  +- Relation[c_custkey#171L,c_name#172,c_address#173,c_nationkey#174L,c_phone#175,c_acctbal#176,c_mktsegment#177,c_comment#178] parquet
               :  +- SubqueryAlias spark_catalog.default.orders
               :     +- Relation[o_orderkey#179L,o_custkey#180L,o_orderstatus#181,o_totalprice#182,o_orderdate#183,o_orderpriority#184,o_clerk#185,o_shippriority#186L,o_comment#187] parquet
               +- SubqueryAlias spark_catalog.default.lineitem
                  +- Relation[l_orderkey#188L,l_partkey#189L,l_suppkey#190L,l_linenumber#191L,l_quantity#192,l_extendedprice#193,l_discount#194,l_tax#195,l_returnflag#196,l_linestatus#197,l_shipdate#198,l_commitdate#199,l_receiptdate#200,l_shipinstruct#201,l_shipmode#202,l_comment#203] parquet

== Optimized Logical Plan ==
GlobalLimit 10
+- LocalLimit 10
   +- Sort [revenue#170 DESC NULLS LAST, o_orderdate#183 ASC NULLS FIRST], true
      +- Aggregate [l_orderkey#188L, o_orderdate#183, o_shippriority#186L], [l_orderkey#188L, sum((l_extendedprice#193 * (1.0 - l_discount#194))) AS revenue#170, o_orderdate#183, o_shippriority#186L]
         +- Project [o_orderdate#183, o_shippriority#186L, l_orderkey#188L, l_extendedprice#193, l_discount#194]
            +- Join Inner, (l_orderkey#188L = o_orderkey#179L)
               :- Project [o_orderkey#179L, o_orderdate#183, o_shippriority#186L]
               :  +- Join Inner, (c_custkey#171L = o_custkey#180L)
               :     :- Project [c_custkey#171L]
               :     :  +- Filter ((isnotnull(c_mktsegment#177) AND (c_mktsegment#177 = BUILDING)) AND isnotnull(c_custkey#171L))
               :     :     +- Relation[c_custkey#171L,c_name#172,c_address#173,c_nationkey#174L,c_phone#175,c_acctbal#176,c_mktsegment#177,c_comment#178] parquet
               :     +- Project [o_orderkey#179L, o_custkey#180L, o_orderdate#183, o_shippriority#186L]
               :        +- Filter (((isnotnull(o_orderdate#183) AND (o_orderdate#183 < 9204)) AND isnotnull(o_custkey#180L)) AND isnotnull(o_orderkey#179L))
               :           +- Relation[o_orderkey#179L,o_custkey#180L,o_orderstatus#181,o_totalprice#182,o_orderdate#183,o_orderpriority#184,o_clerk#185,o_shippriority#186L,o_comment#187] parquet
               +- Project [l_orderkey#188L, l_extendedprice#193, l_discount#194]
                  +- Filter ((isnotnull(l_shipdate#198) AND (l_shipdate#198 > 9204)) AND isnotnull(l_orderkey#188L))
                     +- Relation[l_orderkey#188L,l_partkey#189L,l_suppkey#190L,l_linenumber#191L,l_quantity#192,l_extendedprice#193,l_discount#194,l_tax#195,l_returnflag#196,l_linestatus#197,l_shipdate#198,l_commitdate#199,l_receiptdate#200,l_shipinstruct#201,l_shipmode#202,l_comment#203] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=10, orderBy=[revenue#170 DESC NULLS LAST,o_orderdate#183 ASC NULLS FIRST], output=[l_orderkey#188L,revenue#170,o_orderdate#183,o_shippriority#186L])
+- *(1) ColumnarToRow
   +- *(5) HashAggregateTransformer(keys=[l_orderkey#188L, o_orderdate#183, o_shippriority#186L], functions=[sum((l_extendedprice#193 * (1.0 - l_discount#194)))], output=[l_orderkey#188L, revenue#170, o_orderdate#183, o_shippriority#186L])
      +- *(5) HashAggregateTransformer(keys=[l_orderkey#188L, o_orderdate#183, o_shippriority#186L], functions=[partial_sum((l_extendedprice#193 * (1.0 - l_discount#194)))], output=[l_orderkey#188L, o_orderdate#183, o_shippriority#186L, sum#229])
         +- *(5) ProjectExecTransformer [o_orderdate#183, o_shippriority#186L, l_orderkey#188L, l_extendedprice#193, l_discount#194]
            +- *(5) ShuffledHashJoinExecTransformer [o_orderkey#179L], [l_orderkey#188L], Inner, BuildRight
               :- CoalesceBatches
               :  +- ColumnarExchange hashpartitioning(o_orderkey#179L, 1), ENSURE_REQUIREMENTS, [id=#782], [id=#782], [OUTPUT] List(o_orderkey:LongType, o_orderdate:DateType, o_shippriority:LongType), [OUTPUT] List(o_orderkey:LongType, o_orderdate:DateType, o_shippriority:LongType)
               :     +- *(3) ProjectExecTransformer [o_orderkey#179L, o_orderdate#183, o_shippriority#186L]
               :        +- *(3) ShuffledHashJoinExecTransformer [c_custkey#171L], [o_custkey#180L], Inner, BuildLeft
               :           :- CoalesceBatches
               :           :  +- ColumnarExchange hashpartitioning(c_custkey#171L, 1), ENSURE_REQUIREMENTS, [id=#772], [id=#772], [OUTPUT] List(c_custkey:LongType), [OUTPUT] List(c_custkey:LongType)
               :           :     +- *(1) ProjectExecTransformer [c_custkey#171L]
               :           :        +- *(1) FilterExecTransformer ((isnotnull(c_mktsegment#177) AND (c_mktsegment#177 = BUILDING)) AND isnotnull(c_custkey#171L))
               :           :           +- *(1) FileScan parquet default.customer[c_custkey#171L,c_mktsegment#177] Batched: true, DataFilters: [isnotnull(c_mktsegment#177), (c_mktsegment#177 = BUILDING), isnotnull(c_custkey#171L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_mktsegment), EqualTo(c_mktsegment,BUILDING), IsNotNull(c_custkey)], ReadSchema: struct<c_custkey:bigint,c_mktsegment:string>
               :           +- CoalesceBatches
               :              +- ColumnarExchange hashpartitioning(o_custkey#180L, 1), ENSURE_REQUIREMENTS, [id=#776], [id=#776], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_orderdate:DateType, o_shippriority:LongType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_orderdate:DateType, o_shippriority:LongType)
               :                 +- *(2) FilterExecTransformer (((isnotnull(o_orderdate#183) AND (o_orderdate#183 < 9204)) AND isnotnull(o_custkey#180L)) AND isnotnull(o_orderkey#179L))
               :                    +- *(2) FileScan parquet default.orders[o_orderkey#179L,o_custkey#180L,o_orderdate#183,o_shippriority#186L] Batched: true, DataFilters: [isnotnull(o_orderdate#183), (o_orderdate#183 < 9204), isnotnull(o_custkey#180L), isnotnull(o_ord..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), LessThan(o_orderdate,1995-03-15), IsNotNull(o_custkey), IsNotNull(o_orde..., ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:date,o_shippriority:bigint>
               +- CoalesceBatches
                  +- ColumnarExchange hashpartitioning(l_orderkey#188L, 1), ENSURE_REQUIREMENTS, [id=#786], [id=#786], [OUTPUT] List(l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                     +- *(4) ProjectExecTransformer [l_orderkey#188L, l_extendedprice#193, l_discount#194]
                        +- *(4) FilterExecTransformer ((isnotnull(l_shipdate#198) AND (l_shipdate#198 > 9204)) AND isnotnull(l_orderkey#188L))
                           +- *(4) FileScan parquet default.lineitem[l_orderkey#188L,l_extendedprice#193,l_discount#194,l_shipdate#198] Batched: true, DataFilters: [isnotnull(l_shipdate#198), (l_shipdate#198 > 9204), isnotnull(l_orderkey#188L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThan(l_shipdate,1995-03-15), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_extendedprice:double,l_discount:double,l_shipdate:date>

````

[返回目录](#目录)

## Q4: 订单优先级查询
* Q4 语句确定订单优先级系统的工作情况并评估客户满意度。
* **业务意义**：统计每个季度内，<u>至少有一项客户的收货日期晚于其承诺日期的</u>订单。根据**订单优先级**分组计数，并按订单优先级升序排序。
* Q4语句的特点是：带有分组、排序、聚集操作、子查询并存的单表查询操作。**子查询是相关子查询**。

````mysql
select
	o_orderpriority,        # 订单优先级
	count(*) as order_count # 订单优先级计数
from
	orders
where
	o_orderdate >= '1993-07-01'
	 # 指定订单的时间段: 三个月，DATE 是在 1993 年 1 月和 1997 年 10 月之间随机选择的某个月的第一天
	and o_orderdate < date_add('1993-07-01', interval '3' month)
	and exists (
		select *
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority # 按订单优先级分组
order by
	o_orderpriority; # 按订单优先级排序
````

````
== Parsed Logical Plan ==
'Sort ['o_orderpriority ASC NULLS FIRST], true
+- 'Aggregate ['o_orderpriority], ['o_orderpriority, 'count(1) AS order_count#231]
   +- 'Filter ((('o_orderdate >= 8582) AND ('o_orderdate < (8582 + 3 months))) AND exists#230 [])
      :  +- 'Project [*]
      :     +- 'Filter (('l_orderkey = 'o_orderkey) AND ('l_commitdate < 'l_receiptdate))
      :        +- 'UnresolvedRelation [lineitem], [], false
      +- 'UnresolvedRelation [orders], [], false

== Analyzed Logical Plan ==
o_orderpriority: string, order_count: bigint
Sort [o_orderpriority#238 ASC NULLS FIRST], true
+- Aggregate [o_orderpriority#238], [o_orderpriority#238, count(1) AS order_count#231L]
   +- Filter (((o_orderdate#237 >= 8582) AND (o_orderdate#237 < 8582 + 3 months)) AND exists#230 [o_orderkey#233L])
      :  +- Project [l_orderkey#242L, l_partkey#243L, l_suppkey#244L, l_linenumber#245L, l_quantity#246, l_extendedprice#247, l_discount#248, l_tax#249, l_returnflag#250, l_linestatus#251, l_shipdate#252, l_commitdate#253, l_receiptdate#254, l_shipinstruct#255, l_shipmode#256, l_comment#257]
      :     +- Filter ((l_orderkey#242L = outer(o_orderkey#233L)) AND (l_commitdate#253 < l_receiptdate#254))
      :        +- SubqueryAlias spark_catalog.default.lineitem
      :           +- Relation[l_orderkey#242L,l_partkey#243L,l_suppkey#244L,l_linenumber#245L,l_quantity#246,l_extendedprice#247,l_discount#248,l_tax#249,l_returnflag#250,l_linestatus#251,l_shipdate#252,l_commitdate#253,l_receiptdate#254,l_shipinstruct#255,l_shipmode#256,l_comment#257] parquet
      +- SubqueryAlias spark_catalog.default.orders
         +- Relation[o_orderkey#233L,o_custkey#234L,o_orderstatus#235,o_totalprice#236,o_orderdate#237,o_orderpriority#238,o_clerk#239,o_shippriority#240L,o_comment#241] parquet

== Optimized Logical Plan ==
Sort [o_orderpriority#238 ASC NULLS FIRST], true
+- Aggregate [o_orderpriority#238], [o_orderpriority#238, count(1) AS order_count#231L]
   +- Project [o_orderpriority#238]
      +- Join LeftSemi, (l_orderkey#242L = o_orderkey#233L)
         :- Project [o_orderkey#233L, o_orderpriority#238]
         :  +- Filter ((isnotnull(o_orderdate#237) AND (o_orderdate#237 >= 8582)) AND (o_orderdate#237 < 8674))
         :     +- Relation[o_orderkey#233L,o_custkey#234L,o_orderstatus#235,o_totalprice#236,o_orderdate#237,o_orderpriority#238,o_clerk#239,o_shippriority#240L,o_comment#241] parquet
         +- Project [l_orderkey#242L]
            +- Filter ((isnotnull(l_commitdate#253) AND isnotnull(l_receiptdate#254)) AND (l_commitdate#253 < l_receiptdate#254))
               +- Relation[l_orderkey#242L,l_partkey#243L,l_suppkey#244L,l_linenumber#245L,l_quantity#246,l_extendedprice#247,l_discount#248,l_tax#249,l_returnflag#250,l_linestatus#251,l_shipdate#252,l_commitdate#253,l_receiptdate#254,l_shipinstruct#255,l_shipmode#256,l_comment#257] parquet

== Physical Plan ==
*(1) Sort [o_orderpriority#238 ASC NULLS FIRST], true, 0
+- *(1) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(o_orderpriority#238 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#900], [id=#900], [OUTPUT] List(o_orderpriority:StringType, order_count:LongType), [OUTPUT] List(o_orderpriority:StringType, order_count:LongType)
         +- *(4) HashAggregateTransformer(keys=[o_orderpriority#238], functions=[count(1)], output=[o_orderpriority#238, order_count#231L])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(o_orderpriority#238, 1), ENSURE_REQUIREMENTS, [id=#895], [id=#895], [OUTPUT] ArrayBuffer(o_orderpriority:StringType, count:LongType), [OUTPUT] ArrayBuffer(o_orderpriority:StringType, count:LongType)
                  +- *(3) HashAggregateTransformer(keys=[o_orderpriority#238], functions=[partial_count(1)], output=[o_orderpriority#238, count#261L])
                     +- *(3) ProjectExecTransformer [o_orderpriority#238]
                        +- *(3) ShuffledHashJoinExecTransformer [o_orderkey#233L], [l_orderkey#242L], LeftSemi, BuildRight
                           :- CoalesceBatches
                           :  +- ColumnarExchange hashpartitioning(o_orderkey#233L, 1), ENSURE_REQUIREMENTS, [id=#884], [id=#884], [OUTPUT] List(o_orderkey:LongType, o_orderpriority:StringType), [OUTPUT] List(o_orderkey:LongType, o_orderpriority:StringType)
                           :     +- *(1) ProjectExecTransformer [o_orderkey#233L, o_orderpriority#238]
                           :        +- *(1) FilterExecTransformer ((isnotnull(o_orderdate#237) AND (o_orderdate#237 >= 8582)) AND (o_orderdate#237 < 8674))
                           :           +- *(1) FileScan parquet default.orders[o_orderkey#233L,o_orderdate#237,o_orderpriority#238] Batched: true, DataFilters: [isnotnull(o_orderdate#237), (o_orderdate#237 >= 8582), (o_orderdate#237 < 8674)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), GreaterThanOrEqual(o_orderdate,1993-07-01), LessThan(o_orderdate,1993-10..., ReadSchema: struct<o_orderkey:bigint,o_orderdate:date,o_orderpriority:string>
                           +- CoalesceBatches
                              +- ColumnarExchange hashpartitioning(l_orderkey#242L, 1), ENSURE_REQUIREMENTS, [id=#888], [id=#888], [OUTPUT] List(l_orderkey:LongType), [OUTPUT] List(l_orderkey:LongType)
                                 +- *(2) ProjectExecTransformer [l_orderkey#242L]
                                    +- *(2) FilterExecTransformer ((isnotnull(l_commitdate#253) AND isnotnull(l_receiptdate#254)) AND (l_commitdate#253 < l_receiptdate#254))
                                       +- *(2) FileScan parquet default.lineitem[l_orderkey#242L,l_commitdate#253,l_receiptdate#254] Batched: true, DataFilters: [isnotnull(l_commitdate#253), isnotnull(l_receiptdate#254), (l_commitdate#253 < l_receiptdate#254)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_commitdate), IsNotNull(l_receiptdate)], ReadSchema: struct<l_orderkey:bigint,l_commitdate:date,l_receiptdate:date>

````

[返回目录](#目录)

## Q5: 某地区供货商为公司带来的收入查询 ✓
* Q5语句查询统计通过**某个地区零件供货商**而获得的收入（定义为：`sum(l_extendedprice * (1 -l_discount))`）。可用于决定在给定的区域是否需要建立一个当地分配中心。
* Q5语句的特点是：带有分组、排序、聚集操作、子查询并存的多表连接查询操作。
````mysql
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA' # 指定地区，在TPC-H标准指定的范围内随机选择
	and o_orderdate >= '1994-01-01' # DATE是从1993年到1997年中随机选择的一年的1月1日
	and o_orderdate < date_add('1994-01-01', interval '1' year)
group by
	n_name # 按名字分组
order by
	revenue desc; # 按收入降序排序，注意分组和排序子句不同
````

````
== Parsed Logical Plan ==
'Sort ['revenue DESC NULLS LAST], true
+- 'Aggregate ['n_name], ['n_name, 'sum(('l_extendedprice * (1 - 'l_discount))) AS revenue#262]
   +- 'Filter ((((('c_custkey = 'o_custkey) AND ('l_orderkey = 'o_orderkey)) AND ('l_suppkey = 's_suppkey)) AND (('c_nationkey = 's_nationkey) AND ('s_nationkey = 'n_nationkey))) AND ((('n_regionkey = 'r_regionkey) AND ('r_name = ASIA)) AND (('o_orderdate >= 8766) AND ('o_orderdate < (8766 + 1 years)))))
      +- 'Join Inner
         :- 'Join Inner
         :  :- 'Join Inner
         :  :  :- 'Join Inner
         :  :  :  :- 'Join Inner
         :  :  :  :  :- 'UnresolvedRelation [customer], [], false
         :  :  :  :  +- 'UnresolvedRelation [orders], [], false
         :  :  :  +- 'UnresolvedRelation [lineitem], [], false
         :  :  +- 'UnresolvedRelation [supplier], [], false
         :  +- 'UnresolvedRelation [nation], [], false
         +- 'UnresolvedRelation [region], [], false

== Analyzed Logical Plan ==
n_name: string, revenue: double
Sort [revenue#262 DESC NULLS LAST], true
+- Aggregate [n_name#304], [n_name#304, sum((l_extendedprice#285 * (cast(1 as double) - l_discount#286))) AS revenue#262]
   +- Filter (((((c_custkey#263L = o_custkey#272L) AND (l_orderkey#280L = o_orderkey#271L)) AND (l_suppkey#282L = s_suppkey#296L)) AND ((c_nationkey#266L = s_nationkey#299L) AND (s_nationkey#299L = n_nationkey#303L))) AND (((n_regionkey#305L = r_regionkey#307L) AND (r_name#308 = ASIA)) AND ((o_orderdate#275 >= 8766) AND (o_orderdate#275 < 8766 + 1 years))))
      +- Join Inner
         :- Join Inner
         :  :- Join Inner
         :  :  :- Join Inner
         :  :  :  :- Join Inner
         :  :  :  :  :- SubqueryAlias spark_catalog.default.customer
         :  :  :  :  :  +- Relation[c_custkey#263L,c_name#264,c_address#265,c_nationkey#266L,c_phone#267,c_acctbal#268,c_mktsegment#269,c_comment#270] parquet
         :  :  :  :  +- SubqueryAlias spark_catalog.default.orders
         :  :  :  :     +- Relation[o_orderkey#271L,o_custkey#272L,o_orderstatus#273,o_totalprice#274,o_orderdate#275,o_orderpriority#276,o_clerk#277,o_shippriority#278L,o_comment#279] parquet
         :  :  :  +- SubqueryAlias spark_catalog.default.lineitem
         :  :  :     +- Relation[l_orderkey#280L,l_partkey#281L,l_suppkey#282L,l_linenumber#283L,l_quantity#284,l_extendedprice#285,l_discount#286,l_tax#287,l_returnflag#288,l_linestatus#289,l_shipdate#290,l_commitdate#291,l_receiptdate#292,l_shipinstruct#293,l_shipmode#294,l_comment#295] parquet
         :  :  +- SubqueryAlias spark_catalog.default.supplier
         :  :     +- Relation[s_suppkey#296L,s_name#297,s_address#298,s_nationkey#299L,s_phone#300,s_acctbal#301,s_comment#302] parquet
         :  +- SubqueryAlias spark_catalog.default.nation
         :     +- Relation[n_nationkey#303L,n_name#304,n_regionkey#305L,n_comment#306] parquet
         +- SubqueryAlias spark_catalog.default.region
            +- Relation[r_regionkey#307L,r_name#308,r_comment#309] parquet

== Optimized Logical Plan ==
Sort [revenue#262 DESC NULLS LAST], true
+- Aggregate [n_name#304], [n_name#304, sum((l_extendedprice#285 * (1.0 - l_discount#286))) AS revenue#262]
   +- Project [l_extendedprice#285, l_discount#286, n_name#304]
      +- Join Inner, (n_regionkey#305L = r_regionkey#307L)
         :- Project [l_extendedprice#285, l_discount#286, n_name#304, n_regionkey#305L]
         :  +- Join Inner, (s_nationkey#299L = n_nationkey#303L)
         :     :- Project [l_extendedprice#285, l_discount#286, s_nationkey#299L]
         :     :  +- Join Inner, ((l_suppkey#282L = s_suppkey#296L) AND (c_nationkey#266L = s_nationkey#299L))
         :     :     :- Project [c_nationkey#266L, l_suppkey#282L, l_extendedprice#285, l_discount#286]
         :     :     :  +- Join Inner, (l_orderkey#280L = o_orderkey#271L)
         :     :     :     :- Project [c_nationkey#266L, o_orderkey#271L]
         :     :     :     :  +- Join Inner, (c_custkey#263L = o_custkey#272L)
         :     :     :     :     :- Project [c_custkey#263L, c_nationkey#266L]
         :     :     :     :     :  +- Filter (isnotnull(c_custkey#263L) AND isnotnull(c_nationkey#266L))
         :     :     :     :     :     +- Relation[c_custkey#263L,c_name#264,c_address#265,c_nationkey#266L,c_phone#267,c_acctbal#268,c_mktsegment#269,c_comment#270] parquet
         :     :     :     :     +- Project [o_orderkey#271L, o_custkey#272L]
         :     :     :     :        +- Filter ((((isnotnull(o_orderdate#275) AND (o_orderdate#275 >= 8766)) AND (o_orderdate#275 < 9131)) AND isnotnull(o_custkey#272L)) AND isnotnull(o_orderkey#271L))
         :     :     :     :           +- Relation[o_orderkey#271L,o_custkey#272L,o_orderstatus#273,o_totalprice#274,o_orderdate#275,o_orderpriority#276,o_clerk#277,o_shippriority#278L,o_comment#279] parquet
         :     :     :     +- Project [l_orderkey#280L, l_suppkey#282L, l_extendedprice#285, l_discount#286]
         :     :     :        +- Filter (isnotnull(l_orderkey#280L) AND isnotnull(l_suppkey#282L))
         :     :     :           +- Relation[l_orderkey#280L,l_partkey#281L,l_suppkey#282L,l_linenumber#283L,l_quantity#284,l_extendedprice#285,l_discount#286,l_tax#287,l_returnflag#288,l_linestatus#289,l_shipdate#290,l_commitdate#291,l_receiptdate#292,l_shipinstruct#293,l_shipmode#294,l_comment#295] parquet
         :     :     +- Project [s_suppkey#296L, s_nationkey#299L]
         :     :        +- Filter (isnotnull(s_suppkey#296L) AND isnotnull(s_nationkey#299L))
         :     :           +- Relation[s_suppkey#296L,s_name#297,s_address#298,s_nationkey#299L,s_phone#300,s_acctbal#301,s_comment#302] parquet
         :     +- Project [n_nationkey#303L, n_name#304, n_regionkey#305L]
         :        +- Filter (isnotnull(n_nationkey#303L) AND isnotnull(n_regionkey#305L))
         :           +- Relation[n_nationkey#303L,n_name#304,n_regionkey#305L,n_comment#306] parquet
         +- Project [r_regionkey#307L]
            +- Filter ((isnotnull(r_name#308) AND (r_name#308 = ASIA)) AND isnotnull(r_regionkey#307L))
               +- Relation[r_regionkey#307L,r_name#308,r_comment#309] parquet

== Physical Plan ==
*(1) Sort [revenue#262 DESC NULLS LAST], true, 0
+- *(1) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(revenue#262 DESC NULLS LAST, 1), ENSURE_REQUIREMENTS, [id=#1205], [id=#1205], [OUTPUT] List(n_name:StringType, revenue:DoubleType), [OUTPUT] List(n_name:StringType, revenue:DoubleType)
         +- *(12) HashAggregateTransformer(keys=[n_name#304], functions=[sum((l_extendedprice#285 * (1.0 - l_discount#286)))], output=[n_name#304, revenue#262])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(n_name#304, 1), ENSURE_REQUIREMENTS, [id=#1200], [id=#1200], [OUTPUT] ArrayBuffer(n_name:StringType, sum:DoubleType), [OUTPUT] ArrayBuffer(n_name:StringType, sum:DoubleType)
                  +- *(11) HashAggregateTransformer(keys=[n_name#304], functions=[partial_sum((l_extendedprice#285 * (1.0 - l_discount#286)))], output=[n_name#304, sum#315])
                     +- *(11) ProjectExecTransformer [l_extendedprice#285, l_discount#286, n_name#304]
                        +- *(11) ShuffledHashJoinExecTransformer [n_regionkey#305L], [r_regionkey#307L], Inner, BuildRight
                           :- CoalesceBatches
                           :  +- ColumnarExchange hashpartitioning(n_regionkey#305L, 1), ENSURE_REQUIREMENTS, [id=#1189], [id=#1189], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, n_name:StringType, n_regionkey:LongType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, n_name:StringType, n_regionkey:LongType)
                           :     +- *(9) ProjectExecTransformer [l_extendedprice#285, l_discount#286, n_name#304, n_regionkey#305L]
                           :        +- *(9) ShuffledHashJoinExecTransformer [s_nationkey#299L], [n_nationkey#303L], Inner, BuildRight
                           :           :- CoalesceBatches
                           :           :  +- ColumnarExchange hashpartitioning(s_nationkey#299L, 1), ENSURE_REQUIREMENTS, [id=#1179], [id=#1179], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType)
                           :           :     +- *(7) ProjectExecTransformer [l_extendedprice#285, l_discount#286, s_nationkey#299L]
                           :           :        +- *(7) ShuffledHashJoinExecTransformer [l_suppkey#282L, c_nationkey#266L], [s_suppkey#296L, s_nationkey#299L], Inner, BuildRight
                           :           :           :- CoalesceBatches
                           :           :           :  +- ColumnarExchange hashpartitioning(l_suppkey#282L, c_nationkey#266L, 1), ENSURE_REQUIREMENTS, [id=#1169], [id=#1169], [OUTPUT] List(c_nationkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(c_nationkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                           :           :           :     +- *(5) ProjectExecTransformer [c_nationkey#266L, l_suppkey#282L, l_extendedprice#285, l_discount#286]
                           :           :           :        +- *(5) ShuffledHashJoinExecTransformer [o_orderkey#271L], [l_orderkey#280L], Inner, BuildRight
                           :           :           :           :- CoalesceBatches
                           :           :           :           :  +- ColumnarExchange hashpartitioning(o_orderkey#271L, 1), ENSURE_REQUIREMENTS, [id=#1159], [id=#1159], [OUTPUT] List(c_nationkey:LongType, o_orderkey:LongType), [OUTPUT] List(c_nationkey:LongType, o_orderkey:LongType)
                           :           :           :           :     +- *(3) ProjectExecTransformer [c_nationkey#266L, o_orderkey#271L]
                           :           :           :           :        +- *(3) ShuffledHashJoinExecTransformer [c_custkey#263L], [o_custkey#272L], Inner, BuildLeft
                           :           :           :           :           :- CoalesceBatches
                           :           :           :           :           :  +- ColumnarExchange hashpartitioning(c_custkey#263L, 1), ENSURE_REQUIREMENTS, [id=#1149], [id=#1149], [OUTPUT] List(c_custkey:LongType, c_nationkey:LongType), [OUTPUT] List(c_custkey:LongType, c_nationkey:LongType)
                           :           :           :           :           :     +- *(1) FilterExecTransformer (isnotnull(c_custkey#263L) AND isnotnull(c_nationkey#266L))
                           :           :           :           :           :        +- *(1) FileScan parquet default.customer[c_custkey#263L,c_nationkey#266L] Batched: true, DataFilters: [isnotnull(c_custkey#263L), isnotnull(c_nationkey#266L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_custkey), IsNotNull(c_nationkey)], ReadSchema: struct<c_custkey:bigint,c_nationkey:bigint>
                           :           :           :           :           +- CoalesceBatches
                           :           :           :           :              +- ColumnarExchange hashpartitioning(o_custkey#272L, 1), ENSURE_REQUIREMENTS, [id=#1153], [id=#1153], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType)
                           :           :           :           :                 +- *(2) ProjectExecTransformer [o_orderkey#271L, o_custkey#272L]
                           :           :           :           :                    +- *(2) FilterExecTransformer ((((isnotnull(o_orderdate#275) AND (o_orderdate#275 >= 8766)) AND (o_orderdate#275 < 9131)) AND isnotnull(o_custkey#272L)) AND isnotnull(o_orderkey#271L))
                           :           :           :           :                       +- *(2) FileScan parquet default.orders[o_orderkey#271L,o_custkey#272L,o_orderdate#275] Batched: true, DataFilters: [isnotnull(o_orderdate#275), (o_orderdate#275 >= 8766), (o_orderdate#275 < 9131), isnotnull(o_cus..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), GreaterThanOrEqual(o_orderdate,1994-01-01), LessThan(o_orderdate,1995-01..., ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:date>
                           :           :           :           +- CoalesceBatches
                           :           :           :              +- ColumnarExchange hashpartitioning(l_orderkey#280L, 1), ENSURE_REQUIREMENTS, [id=#1163], [id=#1163], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                           :           :           :                 +- *(4) FilterExecTransformer (isnotnull(l_orderkey#280L) AND isnotnull(l_suppkey#282L))
                           :           :           :                    +- *(4) FileScan parquet default.lineitem[l_orderkey#280L,l_suppkey#282L,l_extendedprice#285,l_discount#286] Batched: true, DataFilters: [isnotnull(l_orderkey#280L), isnotnull(l_suppkey#282L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_orderkey), IsNotNull(l_suppkey)], ReadSchema: struct<l_orderkey:bigint,l_suppkey:bigint,l_extendedprice:double,l_discount:double>
                           :           :           +- CoalesceBatches
                           :           :              +- ColumnarExchange hashpartitioning(s_suppkey#296L, s_nationkey#299L, 1), ENSURE_REQUIREMENTS, [id=#1173], [id=#1173], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
                           :           :                 +- *(6) FilterExecTransformer (isnotnull(s_suppkey#296L) AND isnotnull(s_nationkey#299L))
                           :           :                    +- *(6) FileScan parquet default.supplier[s_suppkey#296L,s_nationkey#299L] Batched: true, DataFilters: [isnotnull(s_suppkey#296L), isnotnull(s_nationkey#299L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
                           :           +- CoalesceBatches
                           :              +- ColumnarExchange hashpartitioning(n_nationkey#303L, 1), ENSURE_REQUIREMENTS, [id=#1183], [id=#1183], [OUTPUT] List(n_nationkey:LongType, n_name:StringType, n_regionkey:LongType), [OUTPUT] List(n_nationkey:LongType, n_name:StringType, n_regionkey:LongType)
                           :                 +- *(8) FilterExecTransformer (isnotnull(n_nationkey#303L) AND isnotnull(n_regionkey#305L))
                           :                    +- *(8) FileScan parquet default.nation[n_nationkey#303L,n_name#304,n_regionkey#305L] Batched: true, DataFilters: [isnotnull(n_nationkey#303L), isnotnull(n_regionkey#305L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey), IsNotNull(n_regionkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string,n_regionkey:bigint>
                           +- CoalesceBatches
                              +- ColumnarExchange hashpartitioning(r_regionkey#307L, 1), ENSURE_REQUIREMENTS, [id=#1193], [id=#1193], [OUTPUT] List(r_regionkey:LongType), [OUTPUT] List(r_regionkey:LongType)
                                 +- *(10) ProjectExecTransformer [r_regionkey#307L]
                                    +- *(10) FilterExecTransformer ((isnotnull(r_name#308) AND (r_name#308 = ASIA)) AND isnotnull(r_regionkey#307L))
                                       +- *(10) FileScan parquet default.region[r_regionkey#307L,r_name#308] Batched: true, DataFilters: [isnotnull(r_name#308), (r_name#308 = ASIA), isnotnull(r_regionkey#307L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,ASIA), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>

````

[返回目录](#目录)

## Q6: 预测收入变化查询 ✓
* Q6 语句量化在<u>给定年度</u>，<u>给定百分比范围</u>，取消某些公司范围内的折扣**可增加多少收入额**。这是典型的 **what-if** 判断，用来寻找增加收入的途径。
* **业务意义**：该查询考虑给定年份中所有已发货、且折扣介于 `[DISCOUNT] - 0.01` 和 `[DISCOUNT] + 0.01` 之间的订单项，如果对数量少于 `[quantity]` 的订单项消除这些折扣，**总收入将增加多少金额**。请注意，对于**折扣**（`l_discount`）和**数量**（`l_discount`）在限定范围内的**所有订单项**，潜在收入增加等于 `sum(l_extendedprice * l_discount))`。
* Q6语句的特点是：带有聚集操作的单表查询操作。查询语句使用了BETWEEN-AND操作符，有的数据库可以对BETWEEN-AND进行优化。
````mysql
select
	sum(l_extendedprice * l_discount) as revenue # 潜在的收入增加量
from
	lineitem
where
	l_shipdate >= '1994-01-01' # DATE是从[1993, 1997]中随机选择的一年的1月1日
	and l_shipdate < date_add('1994-01-01', interval '1' year) # 一年内
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24; # QUANTITY在区间[24, 25]中随机选择
````

````
== Parsed Logical Plan ==
'Project ['sum(('l_extendedprice * 'l_discount)) AS revenue#316]
+- 'Filter ((('l_shipdate >= 8766) AND ('l_shipdate < (8766 + 1 years))) AND ((('l_discount >= (0.06 - 0.01)) AND ('l_discount <= (0.06 + 0.01))) AND ('l_quantity < 24)))
   +- 'UnresolvedRelation [lineitem], [], false

== Analyzed Logical Plan ==
revenue: double
Aggregate [sum((l_extendedprice#322 * l_discount#323)) AS revenue#316]
+- Filter (((l_shipdate#327 >= 8766) AND (l_shipdate#327 < 8766 + 1 years)) AND (((l_discount#323 >= cast(CheckOverflow((promote_precision(cast(0.06 as decimal(3,2))) - promote_precision(cast(0.01 as decimal(3,2)))), DecimalType(3,2), true) as double)) AND (l_discount#323 <= cast(CheckOverflow((promote_precision(cast(0.06 as decimal(3,2))) + promote_precision(cast(0.01 as decimal(3,2)))), DecimalType(3,2), true) as double))) AND (l_quantity#321 < cast(24 as double))))
   +- SubqueryAlias spark_catalog.default.lineitem
      +- Relation[l_orderkey#317L,l_partkey#318L,l_suppkey#319L,l_linenumber#320L,l_quantity#321,l_extendedprice#322,l_discount#323,l_tax#324,l_returnflag#325,l_linestatus#326,l_shipdate#327,l_commitdate#328,l_receiptdate#329,l_shipinstruct#330,l_shipmode#331,l_comment#332] parquet

== Optimized Logical Plan ==
Aggregate [sum((l_extendedprice#322 * l_discount#323)) AS revenue#316]
+- Project [l_extendedprice#322, l_discount#323]
   +- Filter (((((((isnotnull(l_shipdate#327) AND isnotnull(l_discount#323)) AND isnotnull(l_quantity#321)) AND (l_shipdate#327 >= 8766)) AND (l_shipdate#327 < 9131)) AND (l_discount#323 >= 0.05)) AND (l_discount#323 <= 0.07)) AND (l_quantity#321 < 24.0))
      +- Relation[l_orderkey#317L,l_partkey#318L,l_suppkey#319L,l_linenumber#320L,l_quantity#321,l_extendedprice#322,l_discount#323,l_tax#324,l_returnflag#325,l_linestatus#326,l_shipdate#327,l_commitdate#328,l_receiptdate#329,l_shipinstruct#330,l_shipmode#331,l_comment#332] parquet

== Physical Plan ==
*(1) ColumnarToRow
+- *(2) HashAggregateTransformer(keys=[], functions=[sum((l_extendedprice#322 * l_discount#323))], output=[revenue#316])
   +- CoalesceBatches
      +- ColumnarExchange SinglePartition, ENSURE_REQUIREMENTS, [id=#1244], [id=#1244], [OUTPUT] List(sum:DoubleType), [OUTPUT] List(sum:DoubleType)
         +- *(1) HashAggregateTransformer(keys=[], functions=[partial_sum((l_extendedprice#322 * l_discount#323))], output=[sum#336])
            +- *(1) ProjectExecTransformer [l_extendedprice#322, l_discount#323]
               +- *(1) FilterExecTransformer (((((((isnotnull(l_shipdate#327) AND isnotnull(l_discount#323)) AND isnotnull(l_quantity#321)) AND (l_shipdate#327 >= 8766)) AND (l_shipdate#327 < 9131)) AND (l_discount#323 >= 0.05)) AND (l_discount#323 <= 0.07)) AND (l_quantity#321 < 24.0))
                  +- *(1) FileScan parquet default.lineitem[l_quantity#321,l_extendedprice#322,l_discount#323,l_shipdate#327] Batched: true, DataFilters: [isnotnull(l_shipdate#327), isnotnull(l_discount#323), isnotnull(l_quantity#321), (l_shipdate#327..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), IsNotNull(l_discount), IsNotNull(l_quantity), GreaterThanOrEqual(l_shipda..., ReadSchema: struct<l_quantity:double,l_extendedprice:double,l_discount:double,l_shipdate:date>

````

[返回目录](#目录)

## Q7: 货运盈利情况查询
* Q7 语句是查询从**供货商国家**与**销售商品的国家**之间通过销售获利情况的查询。此查询确定在两国之间货运商品的量用以帮助重新谈判货运合同。
* Q7语句的特点是：带有分组、排序、聚集、子查询操作并存的多表查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询。
````mysql
select
	supp_nation, # 供货商国家
	cust_nation, # 顾客国家
	l_year,
	sum(volume) as revenue # 年度、年度的货运收入
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and ( # NATION2 和 NATION1 的值不同，表示查询的是跨国的货运情况
				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
			)
			and l_shipdate between '1995-01-01' and '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
````

````
== Parsed Logical Plan ==
'Sort ['supp_nation ASC NULLS FIRST, 'cust_nation ASC NULLS FIRST, 'l_year ASC NULLS FIRST], true
+- 'Aggregate ['supp_nation, 'cust_nation, 'l_year], ['supp_nation, 'cust_nation, 'l_year, 'sum('volume) AS revenue#341]
   +- 'SubqueryAlias shipping
      +- 'Project ['n1.n_name AS supp_nation#337, 'n2.n_name AS cust_nation#338, 'extract(year, 'l_shipdate) AS l_year#339, ('l_extendedprice * (1 - 'l_discount)) AS volume#340]
         +- 'Filter (((('s_suppkey = 'l_suppkey) AND ('o_orderkey = 'l_orderkey)) AND (('c_custkey = 'o_custkey) AND ('s_nationkey = 'n1.n_nationkey))) AND ((('c_nationkey = 'n2.n_nationkey) AND ((('n1.n_name = FRANCE) AND ('n2.n_name = GERMANY)) OR (('n1.n_name = GERMANY) AND ('n2.n_name = FRANCE)))) AND (('l_shipdate >= 9131) AND ('l_shipdate <= 9861))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'UnresolvedRelation [supplier], [], false
               :  :  :  :  +- 'UnresolvedRelation [lineitem], [], false
               :  :  :  +- 'UnresolvedRelation [orders], [], false
               :  :  +- 'UnresolvedRelation [customer], [], false
               :  +- 'SubqueryAlias n1
               :     +- 'UnresolvedRelation [nation], [], false
               +- 'SubqueryAlias n2
                  +- 'UnresolvedRelation [nation], [], false

== Analyzed Logical Plan ==
supp_nation: string, cust_nation: string, l_year: int, revenue: double
Project [supp_nation#337, cust_nation#338, l_year#339, revenue#341]
+- Sort [supp_nation#337 ASC NULLS FIRST, cust_nation#338 ASC NULLS FIRST, l_year#339 ASC NULLS FIRST], true
   +- Aggregate [supp_nation#337, cust_nation#338, l_year#339], [supp_nation#337, cust_nation#338, l_year#339, sum(volume#340) AS revenue#341]
      +- SubqueryAlias shipping
         +- Project [n_name#383 AS supp_nation#337, n_name#387 AS cust_nation#338, extract(year, l_shipdate#359) AS l_year#339, (l_extendedprice#354 * (cast(1 as double) - l_discount#355)) AS volume#340]
            +- Filter ((((s_suppkey#342L = l_suppkey#351L) AND (o_orderkey#365L = l_orderkey#349L)) AND ((c_custkey#374L = o_custkey#366L) AND (s_nationkey#345L = n_nationkey#382L))) AND (((c_nationkey#377L = n_nationkey#386L) AND (((n_name#383 = FRANCE) AND (n_name#387 = GERMANY)) OR ((n_name#383 = GERMANY) AND (n_name#387 = FRANCE)))) AND ((l_shipdate#359 >= 9131) AND (l_shipdate#359 <= 9861))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- SubqueryAlias spark_catalog.default.supplier
                  :  :  :  :  :  +- Relation[s_suppkey#342L,s_name#343,s_address#344,s_nationkey#345L,s_phone#346,s_acctbal#347,s_comment#348] parquet
                  :  :  :  :  +- SubqueryAlias spark_catalog.default.lineitem
                  :  :  :  :     +- Relation[l_orderkey#349L,l_partkey#350L,l_suppkey#351L,l_linenumber#352L,l_quantity#353,l_extendedprice#354,l_discount#355,l_tax#356,l_returnflag#357,l_linestatus#358,l_shipdate#359,l_commitdate#360,l_receiptdate#361,l_shipinstruct#362,l_shipmode#363,l_comment#364] parquet
                  :  :  :  +- SubqueryAlias spark_catalog.default.orders
                  :  :  :     +- Relation[o_orderkey#365L,o_custkey#366L,o_orderstatus#367,o_totalprice#368,o_orderdate#369,o_orderpriority#370,o_clerk#371,o_shippriority#372L,o_comment#373] parquet
                  :  :  +- SubqueryAlias spark_catalog.default.customer
                  :  :     +- Relation[c_custkey#374L,c_name#375,c_address#376,c_nationkey#377L,c_phone#378,c_acctbal#379,c_mktsegment#380,c_comment#381] parquet
                  :  +- SubqueryAlias n1
                  :     +- SubqueryAlias spark_catalog.default.nation
                  :        +- Relation[n_nationkey#382L,n_name#383,n_regionkey#384L,n_comment#385] parquet
                  +- SubqueryAlias n2
                     +- SubqueryAlias spark_catalog.default.nation
                        +- Relation[n_nationkey#386L,n_name#387,n_regionkey#388L,n_comment#389] parquet

== Optimized Logical Plan ==
Sort [supp_nation#337 ASC NULLS FIRST, cust_nation#338 ASC NULLS FIRST, l_year#339 ASC NULLS FIRST], true
+- Aggregate [supp_nation#337, cust_nation#338, l_year#339], [supp_nation#337, cust_nation#338, l_year#339, sum(volume#340) AS revenue#341]
   +- Project [n_name#383 AS supp_nation#337, n_name#387 AS cust_nation#338, year(l_shipdate#359) AS l_year#339, (l_extendedprice#354 * (1.0 - l_discount#355)) AS volume#340]
      +- Join Inner, ((c_nationkey#377L = n_nationkey#386L) AND (((n_name#383 = FRANCE) AND (n_name#387 = GERMANY)) OR ((n_name#383 = GERMANY) AND (n_name#387 = FRANCE))))
         :- Project [l_extendedprice#354, l_discount#355, l_shipdate#359, c_nationkey#377L, n_name#383]
         :  +- Join Inner, (s_nationkey#345L = n_nationkey#382L)
         :     :- Project [s_nationkey#345L, l_extendedprice#354, l_discount#355, l_shipdate#359, c_nationkey#377L]
         :     :  +- Join Inner, (c_custkey#374L = o_custkey#366L)
         :     :     :- Project [s_nationkey#345L, l_extendedprice#354, l_discount#355, l_shipdate#359, o_custkey#366L]
         :     :     :  +- Join Inner, (o_orderkey#365L = l_orderkey#349L)
         :     :     :     :- Project [s_nationkey#345L, l_orderkey#349L, l_extendedprice#354, l_discount#355, l_shipdate#359]
         :     :     :     :  +- Join Inner, (s_suppkey#342L = l_suppkey#351L)
         :     :     :     :     :- Project [s_suppkey#342L, s_nationkey#345L]
         :     :     :     :     :  +- Filter (isnotnull(s_suppkey#342L) AND isnotnull(s_nationkey#345L))
         :     :     :     :     :     +- Relation[s_suppkey#342L,s_name#343,s_address#344,s_nationkey#345L,s_phone#346,s_acctbal#347,s_comment#348] parquet
         :     :     :     :     +- Project [l_orderkey#349L, l_suppkey#351L, l_extendedprice#354, l_discount#355, l_shipdate#359]
         :     :     :     :        +- Filter ((((isnotnull(l_shipdate#359) AND (l_shipdate#359 >= 9131)) AND (l_shipdate#359 <= 9861)) AND isnotnull(l_suppkey#351L)) AND isnotnull(l_orderkey#349L))
         :     :     :     :           +- Relation[l_orderkey#349L,l_partkey#350L,l_suppkey#351L,l_linenumber#352L,l_quantity#353,l_extendedprice#354,l_discount#355,l_tax#356,l_returnflag#357,l_linestatus#358,l_shipdate#359,l_commitdate#360,l_receiptdate#361,l_shipinstruct#362,l_shipmode#363,l_comment#364] parquet
         :     :     :     +- Project [o_orderkey#365L, o_custkey#366L]
         :     :     :        +- Filter (isnotnull(o_orderkey#365L) AND isnotnull(o_custkey#366L))
         :     :     :           +- Relation[o_orderkey#365L,o_custkey#366L,o_orderstatus#367,o_totalprice#368,o_orderdate#369,o_orderpriority#370,o_clerk#371,o_shippriority#372L,o_comment#373] parquet
         :     :     +- Project [c_custkey#374L, c_nationkey#377L]
         :     :        +- Filter (isnotnull(c_custkey#374L) AND isnotnull(c_nationkey#377L))
         :     :           +- Relation[c_custkey#374L,c_name#375,c_address#376,c_nationkey#377L,c_phone#378,c_acctbal#379,c_mktsegment#380,c_comment#381] parquet
         :     +- Project [n_nationkey#382L, n_name#383]
         :        +- Filter (isnotnull(n_nationkey#382L) AND ((n_name#383 = FRANCE) OR (n_name#383 = GERMANY)))
         :           +- Relation[n_nationkey#382L,n_name#383,n_regionkey#384L,n_comment#385] parquet
         +- Project [n_nationkey#386L, n_name#387]
            +- Filter (isnotnull(n_nationkey#386L) AND ((n_name#387 = GERMANY) OR (n_name#387 = FRANCE)))
               +- Relation[n_nationkey#386L,n_name#387,n_regionkey#388L,n_comment#389] parquet

== Physical Plan ==
*(2) Sort [supp_nation#337 ASC NULLS FIRST, cust_nation#338 ASC NULLS FIRST, l_year#339 ASC NULLS FIRST], true, 0
+- *(2) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(supp_nation#337 ASC NULLS FIRST, cust_nation#338 ASC NULLS FIRST, l_year#339 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#1613], [id=#1613], [OUTPUT] List(supp_nation:StringType, cust_nation:StringType, l_year:IntegerType, revenue:DoubleType), [OUTPUT] List(supp_nation:StringType, cust_nation:StringType, l_year:IntegerType, revenue:DoubleType)
         +- *(12) HashAggregateTransformer(keys=[supp_nation#337, cust_nation#338, l_year#339], functions=[sum(volume#340)], output=[supp_nation#337, cust_nation#338, l_year#339, revenue#341])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(supp_nation#337, cust_nation#338, l_year#339, 1), ENSURE_REQUIREMENTS, [id=#1608], [id=#1608], [OUTPUT] ArrayBuffer(supp_nation:StringType, cust_nation:StringType, l_year:IntegerType, sum:DoubleType), [OUTPUT] ArrayBuffer(supp_nation:StringType, cust_nation:StringType, l_year:IntegerType, sum:DoubleType)
                  +- RowToColumnar
                     +- *(1) HashAggregate(keys=[supp_nation#337, cust_nation#338, l_year#339], functions=[partial_sum(volume#340)], output=[supp_nation#337, cust_nation#338, l_year#339, sum#399])
                        +- *(1) Project [n_name#383 AS supp_nation#337, n_name#387 AS cust_nation#338, year(l_shipdate#359) AS l_year#339, (l_extendedprice#354 * (1.0 - l_discount#355)) AS volume#340]
                           +- *(1) ColumnarToRow
                              +- *(11) ShuffledHashJoinExecTransformer [c_nationkey#377L], [n_nationkey#386L], Inner, BuildRight, (((n_name#383 = FRANCE) AND (n_name#387 = GERMANY)) OR ((n_name#383 = GERMANY) AND (n_name#387 = FRANCE)))
                                 :- CoalesceBatches
                                 :  +- ColumnarExchange hashpartitioning(c_nationkey#377L, 1), ENSURE_REQUIREMENTS, [id=#1546], [id=#1546], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType, c_nationkey:LongType, n_name:StringType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType, c_nationkey:LongType, n_name:StringType)
                                 :     +- *(9) ProjectExecTransformer [l_extendedprice#354, l_discount#355, l_shipdate#359, c_nationkey#377L, n_name#383]
                                 :        +- *(9) ShuffledHashJoinExecTransformer [s_nationkey#345L], [n_nationkey#382L], Inner, BuildRight
                                 :           :- CoalesceBatches
                                 :           :  +- ColumnarExchange hashpartitioning(s_nationkey#345L, 1), ENSURE_REQUIREMENTS, [id=#1536], [id=#1536], [OUTPUT] List(s_nationkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType, c_nationkey:LongType), [OUTPUT] List(s_nationkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType, c_nationkey:LongType)
                                 :           :     +- *(7) ProjectExecTransformer [s_nationkey#345L, l_extendedprice#354, l_discount#355, l_shipdate#359, c_nationkey#377L]
                                 :           :        +- *(7) ShuffledHashJoinExecTransformer [o_custkey#366L], [c_custkey#374L], Inner, BuildRight
                                 :           :           :- CoalesceBatches
                                 :           :           :  +- ColumnarExchange hashpartitioning(o_custkey#366L, 1), ENSURE_REQUIREMENTS, [id=#1526], [id=#1526], [OUTPUT] List(s_nationkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType, o_custkey:LongType), [OUTPUT] List(s_nationkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType, o_custkey:LongType)
                                 :           :           :     +- *(5) ProjectExecTransformer [s_nationkey#345L, l_extendedprice#354, l_discount#355, l_shipdate#359, o_custkey#366L]
                                 :           :           :        +- *(5) ShuffledHashJoinExecTransformer [l_orderkey#349L], [o_orderkey#365L], Inner, BuildRight
                                 :           :           :           :- CoalesceBatches
                                 :           :           :           :  +- ColumnarExchange hashpartitioning(l_orderkey#349L, 1), ENSURE_REQUIREMENTS, [id=#1516], [id=#1516], [OUTPUT] List(s_nationkey:LongType, l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType), [OUTPUT] List(s_nationkey:LongType, l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType)
                                 :           :           :           :     +- *(3) ProjectExecTransformer [s_nationkey#345L, l_orderkey#349L, l_extendedprice#354, l_discount#355, l_shipdate#359]
                                 :           :           :           :        +- *(3) ShuffledHashJoinExecTransformer [s_suppkey#342L], [l_suppkey#351L], Inner, BuildLeft
                                 :           :           :           :           :- CoalesceBatches
                                 :           :           :           :           :  +- ColumnarExchange hashpartitioning(s_suppkey#342L, 1), ENSURE_REQUIREMENTS, [id=#1506], [id=#1506], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
                                 :           :           :           :           :     +- *(1) FilterExecTransformer (isnotnull(s_suppkey#342L) AND isnotnull(s_nationkey#345L))
                                 :           :           :           :           :        +- *(1) FileScan parquet default.supplier[s_suppkey#342L,s_nationkey#345L] Batched: true, DataFilters: [isnotnull(s_suppkey#342L), isnotnull(s_nationkey#345L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
                                 :           :           :           :           +- CoalesceBatches
                                 :           :           :           :              +- ColumnarExchange hashpartitioning(l_suppkey#351L, 1), ENSURE_REQUIREMENTS, [id=#1510], [id=#1510], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, l_shipdate:DateType)
                                 :           :           :           :                 +- *(2) FilterExecTransformer ((((isnotnull(l_shipdate#359) AND (l_shipdate#359 >= 9131)) AND (l_shipdate#359 <= 9861)) AND isnotnull(l_suppkey#351L)) AND isnotnull(l_orderkey#349L))
                                 :           :           :           :                    +- *(2) FileScan parquet default.lineitem[l_orderkey#349L,l_suppkey#351L,l_extendedprice#354,l_discount#355,l_shipdate#359] Batched: true, DataFilters: [isnotnull(l_shipdate#359), (l_shipdate#359 >= 9131), (l_shipdate#359 <= 9861), isnotnull(l_suppk..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1995-01-01), LessThanOrEqual(l_shipdate,199..., ReadSchema: struct<l_orderkey:bigint,l_suppkey:bigint,l_extendedprice:double,l_discount:double,l_shipdate:date>
                                 :           :           :           +- CoalesceBatches
                                 :           :           :              +- ColumnarExchange hashpartitioning(o_orderkey#365L, 1), ENSURE_REQUIREMENTS, [id=#1520], [id=#1520], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType)
                                 :           :           :                 +- *(4) FilterExecTransformer (isnotnull(o_orderkey#365L) AND isnotnull(o_custkey#366L))
                                 :           :           :                    +- *(4) FileScan parquet default.orders[o_orderkey#365L,o_custkey#366L] Batched: true, DataFilters: [isnotnull(o_orderkey#365L), isnotnull(o_custkey#366L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderkey), IsNotNull(o_custkey)], ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint>
                                 :           :           +- CoalesceBatches
                                 :           :              +- ColumnarExchange hashpartitioning(c_custkey#374L, 1), ENSURE_REQUIREMENTS, [id=#1530], [id=#1530], [OUTPUT] List(c_custkey:LongType, c_nationkey:LongType), [OUTPUT] List(c_custkey:LongType, c_nationkey:LongType)
                                 :           :                 +- *(6) FilterExecTransformer (isnotnull(c_custkey#374L) AND isnotnull(c_nationkey#377L))
                                 :           :                    +- *(6) FileScan parquet default.customer[c_custkey#374L,c_nationkey#377L] Batched: true, DataFilters: [isnotnull(c_custkey#374L), isnotnull(c_nationkey#377L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_custkey), IsNotNull(c_nationkey)], ReadSchema: struct<c_custkey:bigint,c_nationkey:bigint>
                                 :           +- CoalesceBatches
                                 :              +- ColumnarExchange hashpartitioning(n_nationkey#382L, 1), ENSURE_REQUIREMENTS, [id=#1540], [id=#1540], [OUTPUT] List(n_nationkey:LongType, n_name:StringType), [OUTPUT] List(n_nationkey:LongType, n_name:StringType)
                                 :                 +- *(8) FilterExecTransformer (isnotnull(n_nationkey#382L) AND ((n_name#383 = FRANCE) OR (n_name#383 = GERMANY)))
                                 :                    +- *(8) FileScan parquet default.nation[n_nationkey#382L,n_name#383] Batched: true, DataFilters: [isnotnull(n_nationkey#382L), ((n_name#383 = FRANCE) OR (n_name#383 = GERMANY))], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey), Or(EqualTo(n_name,FRANCE),EqualTo(n_name,GERMANY))], ReadSchema: struct<n_nationkey:bigint,n_name:string>
                                 +- CoalesceBatches
                                    +- ReusedExchange [n_nationkey#386L, n_name#387], ColumnarExchange hashpartitioning(n_nationkey#382L, 1), ENSURE_REQUIREMENTS, [id=#1540], [id=#1540], [OUTPUT] List(n_nationkey:LongType, n_name:StringType)

````

[返回目录](#目录)

## Q8: 国家市场份额查询
* Q8 语句是查询在过去的两年中一个给定零件类型在某国某地区市场份额的变化情况。
* **业务意义**：某国在某一地区的市场份额是**指该地区某一特定类型产品的收入中由该国供应商提供的部分**，特定种类的产品收入定义为 `sum(l_extendedprice * (1-l_discount))`，下面的查询给出 1995 年和 1996 年订单中某国某种产品的份额。
* Q8 语句的特点是：带有分组、排序、聚集、子查询操作并存的查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询，但子查询自身是多表连接的查询。
* TPC-H 标准定义了 Q8 语句等价的变形 SQL，与上述查询语句格式上基本相同，主要是目标列使用了不同的表达方式，在此不再赘述。

````mysql
select
	o_year, # 年份
	sum(case
		when nation = 'BRAZIL' then volume     # 指定国家，在TPC-H标准指定的范围内随机选择
		else 0 end) / sum(volume) as mkt_share # 市场份额：特定种类的产品收入的百分比；
from
	(
		select
			extract(year from o_orderdate) as o_year, # 分解出年份
			l_extendedprice * (1 - l_discount) as volume, # 特定种类的产品收入
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA' # 指定地区，在TPC-H标准指定的范围内随机选择
			and s_nationkey = n2.n_nationkey
			and o_orderdate between '1995-01-01' and '1996-12-31' # 只查95、96年的情况
			and p_type = 'ECONOMY ANODIZED STEEL' # 指定零件类型，在TPC-H标准指定的范围内随机选择
	) as all_nations
group by
	o_year # 按年分组
order by
	o_year; # 按年排序
````

````
== Parsed Logical Plan ==
'Sort ['o_year ASC NULLS FIRST], true
+- 'Aggregate ['o_year], ['o_year, ('sum(CASE WHEN ('nation = BRAZIL) THEN 'volume ELSE 0 END) / 'sum('volume)) AS mkt_share#403]
   +- 'SubqueryAlias all_nations
      +- 'Project ['extract(year, 'o_orderdate) AS o_year#400, ('l_extendedprice * (1 - 'l_discount)) AS volume#401, 'n2.n_name AS nation#402]
         +- 'Filter ((((('p_partkey = 'l_partkey) AND ('s_suppkey = 'l_suppkey)) AND ('l_orderkey = 'o_orderkey)) AND (('o_custkey = 'c_custkey) AND ('c_nationkey = 'n1.n_nationkey))) AND (((('n1.n_regionkey = 'r_regionkey) AND ('r_name = AMERICA)) AND ('s_nationkey = 'n2.n_nationkey)) AND ((('o_orderdate >= 9131) AND ('o_orderdate <= 9861)) AND ('p_type = ECONOMY ANODIZED STEEL))))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :- 'Join Inner
               :  :  :  :  :  :  :- 'UnresolvedRelation [part], [], false
               :  :  :  :  :  :  +- 'UnresolvedRelation [supplier], [], false
               :  :  :  :  :  +- 'UnresolvedRelation [lineitem], [], false
               :  :  :  :  +- 'UnresolvedRelation [orders], [], false
               :  :  :  +- 'UnresolvedRelation [customer], [], false
               :  :  +- 'SubqueryAlias n1
               :  :     +- 'UnresolvedRelation [nation], [], false
               :  +- 'SubqueryAlias n2
               :     +- 'UnresolvedRelation [nation], [], false
               +- 'UnresolvedRelation [region], [], false

== Analyzed Logical Plan ==
o_year: int, mkt_share: double
Project [o_year#400, mkt_share#403]
+- Sort [o_year#400 ASC NULLS FIRST], true
   +- Aggregate [o_year#400], [o_year#400, (sum(CASE WHEN (nation#402 = BRAZIL) THEN volume#401 ELSE cast(0 as double) END) / sum(volume#401)) AS mkt_share#403]
      +- SubqueryAlias all_nations
         +- Project [extract(year, o_orderdate#440) AS o_year#400, (l_extendedprice#425 * (cast(1 as double) - l_discount#426)) AS volume#401, n_name#458 AS nation#402]
            +- Filter (((((p_partkey#404L = l_partkey#421L) AND (s_suppkey#413L = l_suppkey#422L)) AND (l_orderkey#420L = o_orderkey#436L)) AND ((o_custkey#437L = c_custkey#445L) AND (c_nationkey#448L = n_nationkey#453L))) AND ((((n_regionkey#455L = r_regionkey#461L) AND (r_name#462 = AMERICA)) AND (s_nationkey#416L = n_nationkey#457L)) AND (((o_orderdate#440 >= 9131) AND (o_orderdate#440 <= 9861)) AND (p_type#408 = ECONOMY ANODIZED STEEL))))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- Join Inner
                  :  :  :  :  :  :- Join Inner
                  :  :  :  :  :  :  :- SubqueryAlias spark_catalog.default.part
                  :  :  :  :  :  :  :  +- Relation[p_partkey#404L,p_name#405,p_mfgr#406,p_brand#407,p_type#408,p_size#409L,p_container#410,p_retailprice#411,p_comment#412] parquet
                  :  :  :  :  :  :  +- SubqueryAlias spark_catalog.default.supplier
                  :  :  :  :  :  :     +- Relation[s_suppkey#413L,s_name#414,s_address#415,s_nationkey#416L,s_phone#417,s_acctbal#418,s_comment#419] parquet
                  :  :  :  :  :  +- SubqueryAlias spark_catalog.default.lineitem
                  :  :  :  :  :     +- Relation[l_orderkey#420L,l_partkey#421L,l_suppkey#422L,l_linenumber#423L,l_quantity#424,l_extendedprice#425,l_discount#426,l_tax#427,l_returnflag#428,l_linestatus#429,l_shipdate#430,l_commitdate#431,l_receiptdate#432,l_shipinstruct#433,l_shipmode#434,l_comment#435] parquet
                  :  :  :  :  +- SubqueryAlias spark_catalog.default.orders
                  :  :  :  :     +- Relation[o_orderkey#436L,o_custkey#437L,o_orderstatus#438,o_totalprice#439,o_orderdate#440,o_orderpriority#441,o_clerk#442,o_shippriority#443L,o_comment#444] parquet
                  :  :  :  +- SubqueryAlias spark_catalog.default.customer
                  :  :  :     +- Relation[c_custkey#445L,c_name#446,c_address#447,c_nationkey#448L,c_phone#449,c_acctbal#450,c_mktsegment#451,c_comment#452] parquet
                  :  :  +- SubqueryAlias n1
                  :  :     +- SubqueryAlias spark_catalog.default.nation
                  :  :        +- Relation[n_nationkey#453L,n_name#454,n_regionkey#455L,n_comment#456] parquet
                  :  +- SubqueryAlias n2
                  :     +- SubqueryAlias spark_catalog.default.nation
                  :        +- Relation[n_nationkey#457L,n_name#458,n_regionkey#459L,n_comment#460] parquet
                  +- SubqueryAlias spark_catalog.default.region
                     +- Relation[r_regionkey#461L,r_name#462,r_comment#463] parquet

== Optimized Logical Plan ==
Sort [o_year#400 ASC NULLS FIRST], true
+- Aggregate [o_year#400], [o_year#400, (sum(CASE WHEN (nation#402 = BRAZIL) THEN volume#401 ELSE 0.0 END) / sum(volume#401)) AS mkt_share#403]
   +- Project [year(o_orderdate#440) AS o_year#400, (l_extendedprice#425 * (1.0 - l_discount#426)) AS volume#401, n_name#458 AS nation#402]
      +- Join Inner, (n_regionkey#455L = r_regionkey#461L)
         :- Project [l_extendedprice#425, l_discount#426, o_orderdate#440, n_regionkey#455L, n_name#458]
         :  +- Join Inner, (s_nationkey#416L = n_nationkey#457L)
         :     :- Project [l_extendedprice#425, l_discount#426, s_nationkey#416L, o_orderdate#440, n_regionkey#455L]
         :     :  +- Join Inner, (c_nationkey#448L = n_nationkey#453L)
         :     :     :- Project [l_extendedprice#425, l_discount#426, s_nationkey#416L, o_orderdate#440, c_nationkey#448L]
         :     :     :  +- Join Inner, (o_custkey#437L = c_custkey#445L)
         :     :     :     :- Project [l_extendedprice#425, l_discount#426, s_nationkey#416L, o_custkey#437L, o_orderdate#440]
         :     :     :     :  +- Join Inner, (l_orderkey#420L = o_orderkey#436L)
         :     :     :     :     :- Project [l_orderkey#420L, l_extendedprice#425, l_discount#426, s_nationkey#416L]
         :     :     :     :     :  +- Join Inner, (s_suppkey#413L = l_suppkey#422L)
         :     :     :     :     :     :- Project [l_orderkey#420L, l_suppkey#422L, l_extendedprice#425, l_discount#426]
         :     :     :     :     :     :  +- Join Inner, (p_partkey#404L = l_partkey#421L)
         :     :     :     :     :     :     :- Project [p_partkey#404L]
         :     :     :     :     :     :     :  +- Filter ((isnotnull(p_type#408) AND (p_type#408 = ECONOMY ANODIZED STEEL)) AND isnotnull(p_partkey#404L))
         :     :     :     :     :     :     :     +- Relation[p_partkey#404L,p_name#405,p_mfgr#406,p_brand#407,p_type#408,p_size#409L,p_container#410,p_retailprice#411,p_comment#412] parquet
         :     :     :     :     :     :     +- Project [l_orderkey#420L, l_partkey#421L, l_suppkey#422L, l_extendedprice#425, l_discount#426]
         :     :     :     :     :     :        +- Filter ((isnotnull(l_partkey#421L) AND isnotnull(l_suppkey#422L)) AND isnotnull(l_orderkey#420L))
         :     :     :     :     :     :           +- Relation[l_orderkey#420L,l_partkey#421L,l_suppkey#422L,l_linenumber#423L,l_quantity#424,l_extendedprice#425,l_discount#426,l_tax#427,l_returnflag#428,l_linestatus#429,l_shipdate#430,l_commitdate#431,l_receiptdate#432,l_shipinstruct#433,l_shipmode#434,l_comment#435] parquet
         :     :     :     :     :     +- Project [s_suppkey#413L, s_nationkey#416L]
         :     :     :     :     :        +- Filter (isnotnull(s_suppkey#413L) AND isnotnull(s_nationkey#416L))
         :     :     :     :     :           +- Relation[s_suppkey#413L,s_name#414,s_address#415,s_nationkey#416L,s_phone#417,s_acctbal#418,s_comment#419] parquet
         :     :     :     :     +- Project [o_orderkey#436L, o_custkey#437L, o_orderdate#440]
         :     :     :     :        +- Filter ((((isnotnull(o_orderdate#440) AND (o_orderdate#440 >= 9131)) AND (o_orderdate#440 <= 9861)) AND isnotnull(o_orderkey#436L)) AND isnotnull(o_custkey#437L))
         :     :     :     :           +- Relation[o_orderkey#436L,o_custkey#437L,o_orderstatus#438,o_totalprice#439,o_orderdate#440,o_orderpriority#441,o_clerk#442,o_shippriority#443L,o_comment#444] parquet
         :     :     :     +- Project [c_custkey#445L, c_nationkey#448L]
         :     :     :        +- Filter (isnotnull(c_custkey#445L) AND isnotnull(c_nationkey#448L))
         :     :     :           +- Relation[c_custkey#445L,c_name#446,c_address#447,c_nationkey#448L,c_phone#449,c_acctbal#450,c_mktsegment#451,c_comment#452] parquet
         :     :     +- Project [n_nationkey#453L, n_regionkey#455L]
         :     :        +- Filter (isnotnull(n_nationkey#453L) AND isnotnull(n_regionkey#455L))
         :     :           +- Relation[n_nationkey#453L,n_name#454,n_regionkey#455L,n_comment#456] parquet
         :     +- Project [n_nationkey#457L, n_name#458]
         :        +- Filter isnotnull(n_nationkey#457L)
         :           +- Relation[n_nationkey#457L,n_name#458,n_regionkey#459L,n_comment#460] parquet
         +- Project [r_regionkey#461L]
            +- Filter ((isnotnull(r_name#462) AND (r_name#462 = AMERICA)) AND isnotnull(r_regionkey#461L))
               +- Relation[r_regionkey#461L,r_name#462,r_comment#463] parquet

== Physical Plan ==
*(2) Sort [o_year#400 ASC NULLS FIRST], true, 0
+- *(2) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(o_year#400 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#2045], [id=#2045], [OUTPUT] List(o_year:IntegerType, mkt_share:DoubleType), [OUTPUT] List(o_year:IntegerType, mkt_share:DoubleType)
         +- *(16) HashAggregateTransformer(keys=[o_year#400], functions=[sum(CASE WHEN (nation#402 = BRAZIL) THEN volume#401 ELSE 0.0 END), sum(volume#401)], output=[o_year#400, mkt_share#403])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(o_year#400, 1), ENSURE_REQUIREMENTS, [id=#2040], [id=#2040], [OUTPUT] ArrayBuffer(o_year:IntegerType, sum:DoubleType, sum:DoubleType), [OUTPUT] ArrayBuffer(o_year:IntegerType, sum:DoubleType, sum:DoubleType)
                  +- RowToColumnar
                     +- *(1) HashAggregate(keys=[o_year#400], functions=[partial_sum(CASE WHEN (nation#402 = BRAZIL) THEN volume#401 ELSE 0.0 END), partial_sum(volume#401)], output=[o_year#400, sum#471, sum#472])
                        +- *(1) Project [year(o_orderdate#440) AS o_year#400, (l_extendedprice#425 * (1.0 - l_discount#426)) AS volume#401, n_name#458 AS nation#402]
                           +- *(1) ColumnarToRow
                              +- *(15) ShuffledHashJoinExecTransformer [n_regionkey#455L], [r_regionkey#461L], Inner, BuildRight
                                 :- CoalesceBatches
                                 :  +- ColumnarExchange hashpartitioning(n_regionkey#455L, 1), ENSURE_REQUIREMENTS, [id=#2012], [id=#2012], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, o_orderdate:DateType, n_regionkey:LongType, n_name:StringType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, o_orderdate:DateType, n_regionkey:LongType, n_name:StringType)
                                 :     +- *(13) ProjectExecTransformer [l_extendedprice#425, l_discount#426, o_orderdate#440, n_regionkey#455L, n_name#458]
                                 :        +- *(13) ShuffledHashJoinExecTransformer [s_nationkey#416L], [n_nationkey#457L], Inner, BuildRight
                                 :           :- CoalesceBatches
                                 :           :  +- ColumnarExchange hashpartitioning(s_nationkey#416L, 1), ENSURE_REQUIREMENTS, [id=#2002], [id=#2002], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, o_orderdate:DateType, n_regionkey:LongType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, o_orderdate:DateType, n_regionkey:LongType)
                                 :           :     +- *(11) ProjectExecTransformer [l_extendedprice#425, l_discount#426, s_nationkey#416L, o_orderdate#440, n_regionkey#455L]
                                 :           :        +- *(11) ShuffledHashJoinExecTransformer [c_nationkey#448L], [n_nationkey#453L], Inner, BuildRight
                                 :           :           :- CoalesceBatches
                                 :           :           :  +- ColumnarExchange hashpartitioning(c_nationkey#448L, 1), ENSURE_REQUIREMENTS, [id=#1992], [id=#1992], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, o_orderdate:DateType, c_nationkey:LongType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, o_orderdate:DateType, c_nationkey:LongType)
                                 :           :           :     +- *(9) ProjectExecTransformer [l_extendedprice#425, l_discount#426, s_nationkey#416L, o_orderdate#440, c_nationkey#448L]
                                 :           :           :        +- *(9) ShuffledHashJoinExecTransformer [o_custkey#437L], [c_custkey#445L], Inner, BuildRight
                                 :           :           :           :- CoalesceBatches
                                 :           :           :           :  +- ColumnarExchange hashpartitioning(o_custkey#437L, 1), ENSURE_REQUIREMENTS, [id=#1982], [id=#1982], [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, o_custkey:LongType, o_orderdate:DateType), [OUTPUT] List(l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, o_custkey:LongType, o_orderdate:DateType)
                                 :           :           :           :     +- *(7) ProjectExecTransformer [l_extendedprice#425, l_discount#426, s_nationkey#416L, o_custkey#437L, o_orderdate#440]
                                 :           :           :           :        +- *(7) ShuffledHashJoinExecTransformer [l_orderkey#420L], [o_orderkey#436L], Inner, BuildRight
                                 :           :           :           :           :- CoalesceBatches
                                 :           :           :           :           :  +- ColumnarExchange hashpartitioning(l_orderkey#420L, 1), ENSURE_REQUIREMENTS, [id=#1972], [id=#1972], [OUTPUT] List(l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType), [OUTPUT] List(l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType)
                                 :           :           :           :           :     +- *(5) ProjectExecTransformer [l_orderkey#420L, l_extendedprice#425, l_discount#426, s_nationkey#416L]
                                 :           :           :           :           :        +- *(5) ShuffledHashJoinExecTransformer [l_suppkey#422L], [s_suppkey#413L], Inner, BuildRight
                                 :           :           :           :           :           :- CoalesceBatches
                                 :           :           :           :           :           :  +- ColumnarExchange hashpartitioning(l_suppkey#422L, 1), ENSURE_REQUIREMENTS, [id=#1962], [id=#1962], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                                 :           :           :           :           :           :     +- *(3) ProjectExecTransformer [l_orderkey#420L, l_suppkey#422L, l_extendedprice#425, l_discount#426]
                                 :           :           :           :           :           :        +- *(3) ShuffledHashJoinExecTransformer [p_partkey#404L], [l_partkey#421L], Inner, BuildLeft
                                 :           :           :           :           :           :           :- CoalesceBatches
                                 :           :           :           :           :           :           :  +- ColumnarExchange hashpartitioning(p_partkey#404L, 1), ENSURE_REQUIREMENTS, [id=#1952], [id=#1952], [OUTPUT] List(p_partkey:LongType), [OUTPUT] List(p_partkey:LongType)
                                 :           :           :           :           :           :           :     +- *(1) ProjectExecTransformer [p_partkey#404L]
                                 :           :           :           :           :           :           :        +- *(1) FilterExecTransformer ((isnotnull(p_type#408) AND (p_type#408 = ECONOMY ANODIZED STEEL)) AND isnotnull(p_partkey#404L))
                                 :           :           :           :           :           :           :           +- *(1) FileScan parquet default.part[p_partkey#404L,p_type#408] Batched: true, DataFilters: [isnotnull(p_type#408), (p_type#408 = ECONOMY ANODIZED STEEL), isnotnull(p_partkey#404L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_type), EqualTo(p_type,ECONOMY ANODIZED STEEL), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_type:string>
                                 :           :           :           :           :           :           +- CoalesceBatches
                                 :           :           :           :           :           :              +- ColumnarExchange hashpartitioning(l_partkey#421L, 1), ENSURE_REQUIREMENTS, [id=#1956], [id=#1956], [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                                 :           :           :           :           :           :                 +- *(2) FilterExecTransformer ((isnotnull(l_partkey#421L) AND isnotnull(l_suppkey#422L)) AND isnotnull(l_orderkey#420L))
                                 :           :           :           :           :           :                    +- *(2) FileScan parquet default.lineitem[l_orderkey#420L,l_partkey#421L,l_suppkey#422L,l_extendedprice#425,l_discount#426] Batched: true, DataFilters: [isnotnull(l_partkey#421L), isnotnull(l_suppkey#422L), isnotnull(l_orderkey#420L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_partkey), IsNotNull(l_suppkey), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_partkey:bigint,l_suppkey:bigint,l_extendedprice:double,l_discount:double>
                                 :           :           :           :           :           +- CoalesceBatches
                                 :           :           :           :           :              +- ColumnarExchange hashpartitioning(s_suppkey#413L, 1), ENSURE_REQUIREMENTS, [id=#1966], [id=#1966], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
                                 :           :           :           :           :                 +- *(4) FilterExecTransformer (isnotnull(s_suppkey#413L) AND isnotnull(s_nationkey#416L))
                                 :           :           :           :           :                    +- *(4) FileScan parquet default.supplier[s_suppkey#413L,s_nationkey#416L] Batched: true, DataFilters: [isnotnull(s_suppkey#413L), isnotnull(s_nationkey#416L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
                                 :           :           :           :           +- CoalesceBatches
                                 :           :           :           :              +- ColumnarExchange hashpartitioning(o_orderkey#436L, 1), ENSURE_REQUIREMENTS, [id=#1976], [id=#1976], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_orderdate:DateType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_orderdate:DateType)
                                 :           :           :           :                 +- *(6) FilterExecTransformer ((((isnotnull(o_orderdate#440) AND (o_orderdate#440 >= 9131)) AND (o_orderdate#440 <= 9861)) AND isnotnull(o_orderkey#436L)) AND isnotnull(o_custkey#437L))
                                 :           :           :           :                    +- *(6) FileScan parquet default.orders[o_orderkey#436L,o_custkey#437L,o_orderdate#440] Batched: true, DataFilters: [isnotnull(o_orderdate#440), (o_orderdate#440 >= 9131), (o_orderdate#440 <= 9861), isnotnull(o_or..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), GreaterThanOrEqual(o_orderdate,1995-01-01), LessThanOrEqual(o_orderdate,..., ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:date>
                                 :           :           :           +- CoalesceBatches
                                 :           :           :              +- ColumnarExchange hashpartitioning(c_custkey#445L, 1), ENSURE_REQUIREMENTS, [id=#1986], [id=#1986], [OUTPUT] List(c_custkey:LongType, c_nationkey:LongType), [OUTPUT] List(c_custkey:LongType, c_nationkey:LongType)
                                 :           :           :                 +- *(8) FilterExecTransformer (isnotnull(c_custkey#445L) AND isnotnull(c_nationkey#448L))
                                 :           :           :                    +- *(8) FileScan parquet default.customer[c_custkey#445L,c_nationkey#448L] Batched: true, DataFilters: [isnotnull(c_custkey#445L), isnotnull(c_nationkey#448L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_custkey), IsNotNull(c_nationkey)], ReadSchema: struct<c_custkey:bigint,c_nationkey:bigint>
                                 :           :           +- CoalesceBatches
                                 :           :              +- ColumnarExchange hashpartitioning(n_nationkey#453L, 1), ENSURE_REQUIREMENTS, [id=#1996], [id=#1996], [OUTPUT] List(n_nationkey:LongType, n_regionkey:LongType), [OUTPUT] List(n_nationkey:LongType, n_regionkey:LongType)
                                 :           :                 +- *(10) FilterExecTransformer (isnotnull(n_nationkey#453L) AND isnotnull(n_regionkey#455L))
                                 :           :                    +- *(10) FileScan parquet default.nation[n_nationkey#453L,n_regionkey#455L] Batched: true, DataFilters: [isnotnull(n_nationkey#453L), isnotnull(n_regionkey#455L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey), IsNotNull(n_regionkey)], ReadSchema: struct<n_nationkey:bigint,n_regionkey:bigint>
                                 :           +- CoalesceBatches
                                 :              +- ColumnarExchange hashpartitioning(n_nationkey#457L, 1), ENSURE_REQUIREMENTS, [id=#2006], [id=#2006], [OUTPUT] List(n_nationkey:LongType, n_name:StringType), [OUTPUT] List(n_nationkey:LongType, n_name:StringType)
                                 :                 +- *(12) FilterExecTransformer isnotnull(n_nationkey#457L)
                                 :                    +- *(12) FileScan parquet default.nation[n_nationkey#457L,n_name#458] Batched: true, DataFilters: [isnotnull(n_nationkey#457L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>
                                 +- CoalesceBatches
                                    +- ColumnarExchange hashpartitioning(r_regionkey#461L, 1), ENSURE_REQUIREMENTS, [id=#2016], [id=#2016], [OUTPUT] List(r_regionkey:LongType), [OUTPUT] List(r_regionkey:LongType)
                                       +- *(14) ProjectExecTransformer [r_regionkey#461L]
                                          +- *(14) FilterExecTransformer ((isnotnull(r_name#462) AND (r_name#462 = AMERICA)) AND isnotnull(r_regionkey#461L))
                                             +- *(14) FileScan parquet default.region[r_regionkey#461L,r_name#462] Batched: true, DataFilters: [isnotnull(r_name#462), (r_name#462 = AMERICA), isnotnull(r_regionkey#461L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/region], PartitionFilters: [], PushedFilters: [IsNotNull(r_name), EqualTo(r_name,AMERICA), IsNotNull(r_regionkey)], ReadSchema: struct<r_regionkey:bigint,r_name:string>

````

[返回目录](#目录)

## Q9: 产品类型利润估量查询  ✓
* Q9 语句是查询每个国家每一年所有被定购的零件在一年中的总利润。
* Q9 语句的特点是：带有分组、排序、聚集、子查询操作并存的查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询，但子查询自身是多表连接的查询。子查询使用 LIKE 操作符，有的查询优化器不支持对 LIKE 操作符进行优化。
````mysql
select
	nation,
	o_year,
	sum(amount) as sum_profit # 每个国家每一年所有被定购的零件在一年中的总利润
from(
	select
	  n_name as nation, # 国家
		extract(year from o_orderdate) as o_year, # 取出年份
		l_extendedprice * (1-l_discount) - ps_supplycost * l_quantity as amount # 利润
	from
		part,
		supplier,
		lineitem,
		partsupp,
		orders,
		nation
	where
		s_suppkey = l_suppkey
		and ps_suppkey = l_suppkey
		and ps_partkey = l_partkey
		and p_partkey = l_partkey
		and o_orderkey = l_orderkey
		and s_nationkey = n_nationkey
		and p_name like '%green%' # LIKE操作，查询优化器可能进行优化
) as profit
group by
	nation, # 国家
	o_year  # 年份
order by
	nation,
	o_year desc;
````

````
== Parsed Logical Plan ==
'Sort ['nation ASC NULLS FIRST, 'o_year DESC NULLS LAST], true
+- 'Aggregate ['nation, 'o_year], ['nation, 'o_year, 'sum('amount) AS sum_profit#476]
   +- 'SubqueryAlias profit
      +- 'Project ['n_name AS nation#473, 'extract(year, 'o_orderdate) AS o_year#474, (('l_extendedprice * (1 - 'l_discount)) - ('ps_supplycost * 'l_quantity)) AS amount#475]
         +- 'Filter (((('s_suppkey = 'l_suppkey) AND ('ps_suppkey = 'l_suppkey)) AND (('ps_partkey = 'l_partkey) AND ('p_partkey = 'l_partkey))) AND ((('o_orderkey = 'l_orderkey) AND ('s_nationkey = 'n_nationkey)) AND 'p_name LIKE %green%))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'Join Inner
               :  :  :  :- 'Join Inner
               :  :  :  :  :- 'UnresolvedRelation [part], [], false
               :  :  :  :  +- 'UnresolvedRelation [supplier], [], false
               :  :  :  +- 'UnresolvedRelation [lineitem], [], false
               :  :  +- 'UnresolvedRelation [partsupp], [], false
               :  +- 'UnresolvedRelation [orders], [], false
               +- 'UnresolvedRelation [nation], [], false

== Analyzed Logical Plan ==
nation: string, o_year: int, sum_profit: double
Project [nation#473, o_year#474, sum_profit#476]
+- Sort [nation#473 ASC NULLS FIRST, o_year#474 DESC NULLS LAST], true
   +- Aggregate [nation#473, o_year#474], [nation#473, o_year#474, sum(amount#475) AS sum_profit#476]
      +- SubqueryAlias profit
         +- Project [n_name#524 AS nation#473, extract(year, o_orderdate#518) AS o_year#474, ((l_extendedprice#498 * (cast(1 as double) - l_discount#499)) - (ps_supplycost#512 * l_quantity#497)) AS amount#475]
            +- Filter ((((s_suppkey#486L = l_suppkey#495L) AND (ps_suppkey#510L = l_suppkey#495L)) AND ((ps_partkey#509L = l_partkey#494L) AND (p_partkey#477L = l_partkey#494L))) AND (((o_orderkey#514L = l_orderkey#493L) AND (s_nationkey#489L = n_nationkey#523L)) AND p_name#478 LIKE %green%))
               +- Join Inner
                  :- Join Inner
                  :  :- Join Inner
                  :  :  :- Join Inner
                  :  :  :  :- Join Inner
                  :  :  :  :  :- SubqueryAlias spark_catalog.default.part
                  :  :  :  :  :  +- Relation[p_partkey#477L,p_name#478,p_mfgr#479,p_brand#480,p_type#481,p_size#482L,p_container#483,p_retailprice#484,p_comment#485] parquet
                  :  :  :  :  +- SubqueryAlias spark_catalog.default.supplier
                  :  :  :  :     +- Relation[s_suppkey#486L,s_name#487,s_address#488,s_nationkey#489L,s_phone#490,s_acctbal#491,s_comment#492] parquet
                  :  :  :  +- SubqueryAlias spark_catalog.default.lineitem
                  :  :  :     +- Relation[l_orderkey#493L,l_partkey#494L,l_suppkey#495L,l_linenumber#496L,l_quantity#497,l_extendedprice#498,l_discount#499,l_tax#500,l_returnflag#501,l_linestatus#502,l_shipdate#503,l_commitdate#504,l_receiptdate#505,l_shipinstruct#506,l_shipmode#507,l_comment#508] parquet
                  :  :  +- SubqueryAlias spark_catalog.default.partsupp
                  :  :     +- Relation[ps_partkey#509L,ps_suppkey#510L,ps_availqty#511L,ps_supplycost#512,ps_comment#513] parquet
                  :  +- SubqueryAlias spark_catalog.default.orders
                  :     +- Relation[o_orderkey#514L,o_custkey#515L,o_orderstatus#516,o_totalprice#517,o_orderdate#518,o_orderpriority#519,o_clerk#520,o_shippriority#521L,o_comment#522] parquet
                  +- SubqueryAlias spark_catalog.default.nation
                     +- Relation[n_nationkey#523L,n_name#524,n_regionkey#525L,n_comment#526] parquet

== Optimized Logical Plan ==
Sort [nation#473 ASC NULLS FIRST, o_year#474 DESC NULLS LAST], true
+- Aggregate [nation#473, o_year#474], [nation#473, o_year#474, sum(amount#475) AS sum_profit#476]
   +- Project [n_name#524 AS nation#473, year(o_orderdate#518) AS o_year#474, ((l_extendedprice#498 * (1.0 - l_discount#499)) - (ps_supplycost#512 * l_quantity#497)) AS amount#475]
      +- Join Inner, (s_nationkey#489L = n_nationkey#523L)
         :- Project [l_quantity#497, l_extendedprice#498, l_discount#499, s_nationkey#489L, ps_supplycost#512, o_orderdate#518]
         :  +- Join Inner, (o_orderkey#514L = l_orderkey#493L)
         :     :- Project [l_orderkey#493L, l_quantity#497, l_extendedprice#498, l_discount#499, s_nationkey#489L, ps_supplycost#512]
         :     :  +- Join Inner, ((ps_suppkey#510L = l_suppkey#495L) AND (ps_partkey#509L = l_partkey#494L))
         :     :     :- Project [l_orderkey#493L, l_partkey#494L, l_suppkey#495L, l_quantity#497, l_extendedprice#498, l_discount#499, s_nationkey#489L]
         :     :     :  +- Join Inner, (s_suppkey#486L = l_suppkey#495L)
         :     :     :     :- Project [l_orderkey#493L, l_partkey#494L, l_suppkey#495L, l_quantity#497, l_extendedprice#498, l_discount#499]
         :     :     :     :  +- Join Inner, (p_partkey#477L = l_partkey#494L)
         :     :     :     :     :- Project [p_partkey#477L]
         :     :     :     :     :  +- Filter ((isnotnull(p_name#478) AND Contains(p_name#478, green)) AND isnotnull(p_partkey#477L))
         :     :     :     :     :     +- Relation[p_partkey#477L,p_name#478,p_mfgr#479,p_brand#480,p_type#481,p_size#482L,p_container#483,p_retailprice#484,p_comment#485] parquet
         :     :     :     :     +- Project [l_orderkey#493L, l_partkey#494L, l_suppkey#495L, l_quantity#497, l_extendedprice#498, l_discount#499]
         :     :     :     :        +- Filter ((isnotnull(l_partkey#494L) AND isnotnull(l_suppkey#495L)) AND isnotnull(l_orderkey#493L))
         :     :     :     :           +- Relation[l_orderkey#493L,l_partkey#494L,l_suppkey#495L,l_linenumber#496L,l_quantity#497,l_extendedprice#498,l_discount#499,l_tax#500,l_returnflag#501,l_linestatus#502,l_shipdate#503,l_commitdate#504,l_receiptdate#505,l_shipinstruct#506,l_shipmode#507,l_comment#508] parquet
         :     :     :     +- Project [s_suppkey#486L, s_nationkey#489L]
         :     :     :        +- Filter (isnotnull(s_suppkey#486L) AND isnotnull(s_nationkey#489L))
         :     :     :           +- Relation[s_suppkey#486L,s_name#487,s_address#488,s_nationkey#489L,s_phone#490,s_acctbal#491,s_comment#492] parquet
         :     :     +- Project [ps_partkey#509L, ps_suppkey#510L, ps_supplycost#512]
         :     :        +- Filter (isnotnull(ps_suppkey#510L) AND isnotnull(ps_partkey#509L))
         :     :           +- Relation[ps_partkey#509L,ps_suppkey#510L,ps_availqty#511L,ps_supplycost#512,ps_comment#513] parquet
         :     +- Project [o_orderkey#514L, o_orderdate#518]
         :        +- Filter isnotnull(o_orderkey#514L)
         :           +- Relation[o_orderkey#514L,o_custkey#515L,o_orderstatus#516,o_totalprice#517,o_orderdate#518,o_orderpriority#519,o_clerk#520,o_shippriority#521L,o_comment#522] parquet
         +- Project [n_nationkey#523L, n_name#524]
            +- Filter isnotnull(n_nationkey#523L)
               +- Relation[n_nationkey#523L,n_name#524,n_regionkey#525L,n_comment#526] parquet

== Physical Plan ==
*(3) Sort [nation#473 ASC NULLS FIRST, o_year#474 DESC NULLS LAST], true, 0
+- *(3) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(nation#473 ASC NULLS FIRST, o_year#474 DESC NULLS LAST, 1), ENSURE_REQUIREMENTS, [id=#2462], [id=#2462], [OUTPUT] List(nation:StringType, o_year:IntegerType, sum_profit:DoubleType), [OUTPUT] List(nation:StringType, o_year:IntegerType, sum_profit:DoubleType)
         +- *(13) HashAggregateTransformer(keys=[nation#473, o_year#474], functions=[sum(amount#475)], output=[nation#473, o_year#474, sum_profit#476])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(nation#473, o_year#474, 1), ENSURE_REQUIREMENTS, [id=#2457], [id=#2457], [OUTPUT] ArrayBuffer(nation:StringType, o_year:IntegerType, sum:DoubleType), [OUTPUT] ArrayBuffer(nation:StringType, o_year:IntegerType, sum:DoubleType)
                  +- RowToColumnar
                     +- *(2) HashAggregate(keys=[nation#473, o_year#474], functions=[partial_sum(amount#475)], output=[nation#473, o_year#474, sum#534])
                        +- *(2) Project [n_name#524 AS nation#473, year(o_orderdate#518) AS o_year#474, ((l_extendedprice#498 * (1.0 - l_discount#499)) - (ps_supplycost#512 * l_quantity#497)) AS amount#475]
                           +- *(2) ColumnarToRow
                              +- *(12) ShuffledHashJoinExecTransformer [s_nationkey#489L], [n_nationkey#523L], Inner, BuildRight
                                 :- CoalesceBatches
                                 :  +- ColumnarExchange hashpartitioning(s_nationkey#489L, 1), ENSURE_REQUIREMENTS, [id=#2446], [id=#2446], [OUTPUT] List(l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, ps_supplycost:DoubleType, o_orderdate:DateType), [OUTPUT] List(l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, ps_supplycost:DoubleType, o_orderdate:DateType)
                                 :     +- *(10) ProjectExecTransformer [l_quantity#497, l_extendedprice#498, l_discount#499, s_nationkey#489L, ps_supplycost#512, o_orderdate#518]
                                 :        +- *(10) ShuffledHashJoinExecTransformer [l_orderkey#493L], [o_orderkey#514L], Inner, BuildRight
                                 :           :- CoalesceBatches
                                 :           :  +- ColumnarExchange hashpartitioning(l_orderkey#493L, 1), ENSURE_REQUIREMENTS, [id=#2440], [id=#2440], [OUTPUT] List(l_orderkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, ps_supplycost:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType, ps_supplycost:DoubleType)
                                 :           :     +- *(8) ProjectExecTransformer [l_orderkey#493L, l_quantity#497, l_extendedprice#498, l_discount#499, s_nationkey#489L, ps_supplycost#512]
                                 :           :        +- *(8) ShuffledHashJoinExecTransformer [l_suppkey#495L, l_partkey#494L], [ps_suppkey#510L, ps_partkey#509L], Inner, BuildRight
                                 :           :           :- CoalesceBatches
                                 :           :           :  +- ColumnarExchange hashpartitioning(l_suppkey#495L, l_partkey#494L, 1), ENSURE_REQUIREMENTS, [id=#2434], [id=#2434], [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType), [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType, s_nationkey:LongType)
                                 :           :           :     +- *(6) ProjectExecTransformer [l_orderkey#493L, l_partkey#494L, l_suppkey#495L, l_quantity#497, l_extendedprice#498, l_discount#499, s_nationkey#489L]
                                 :           :           :        +- *(6) ShuffledHashJoinExecTransformer [l_suppkey#495L], [s_suppkey#486L], Inner, BuildRight
                                 :           :           :           :- CoalesceBatches
                                 :           :           :           :  +- ColumnarExchange hashpartitioning(l_suppkey#495L, 1), ENSURE_REQUIREMENTS, [id=#2428], [id=#2428], [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType)
                                 :           :           :           :     +- *(4) ProjectExecTransformer [l_orderkey#493L, l_partkey#494L, l_suppkey#495L, l_quantity#497, l_extendedprice#498, l_discount#499]
                                 :           :           :           :        +- *(4) ShuffledHashJoinExecTransformer [p_partkey#477L], [l_partkey#494L], Inner, BuildLeft
                                 :           :           :           :           :- CoalesceBatches
                                 :           :           :           :           :  +- ColumnarExchange hashpartitioning(p_partkey#477L, 1), ENSURE_REQUIREMENTS, [id=#2422], [id=#2422], [OUTPUT] List(p_partkey:LongType), [OUTPUT] List(p_partkey:LongType)
                                 :           :           :           :           :     +- *(2) ProjectExecTransformer [p_partkey#477L]
                                 :           :           :           :           :        +- RowToColumnar
                                 :           :           :           :           :           +- *(1) Filter ((isnotnull(p_name#478) AND Contains(p_name#478, green)) AND isnotnull(p_partkey#477L))
                                 :           :           :           :           :              +- *(1) ColumnarToRow
                                 :           :           :           :           :                 +- *(1) FileScan parquet default.part[p_partkey#477L,p_name#478] Batched: true, DataFilters: [isnotnull(p_name#478), Contains(p_name#478, green), isnotnull(p_partkey#477L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_name), StringContains(p_name,green), IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_name:string>
                                 :           :           :           :           +- CoalesceBatches
                                 :           :           :           :              +- ColumnarExchange hashpartitioning(l_partkey#494L, 1), ENSURE_REQUIREMENTS, [id=#2356], [id=#2356], [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType)
                                 :           :           :           :                 +- *(3) FilterExecTransformer ((isnotnull(l_partkey#494L) AND isnotnull(l_suppkey#495L)) AND isnotnull(l_orderkey#493L))
                                 :           :           :           :                    +- *(3) FileScan parquet default.lineitem[l_orderkey#493L,l_partkey#494L,l_suppkey#495L,l_quantity#497,l_extendedprice#498,l_discount#499] Batched: true, DataFilters: [isnotnull(l_partkey#494L), isnotnull(l_suppkey#495L), isnotnull(l_orderkey#493L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_partkey), IsNotNull(l_suppkey), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_partkey:bigint,l_suppkey:bigint,l_quantity:double,l_extendedprice:doub...
                                 :           :           :           +- CoalesceBatches
                                 :           :           :              +- ColumnarExchange hashpartitioning(s_suppkey#486L, 1), ENSURE_REQUIREMENTS, [id=#2366], [id=#2366], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
                                 :           :           :                 +- *(5) FilterExecTransformer (isnotnull(s_suppkey#486L) AND isnotnull(s_nationkey#489L))
                                 :           :           :                    +- *(5) FileScan parquet default.supplier[s_suppkey#486L,s_nationkey#489L] Batched: true, DataFilters: [isnotnull(s_suppkey#486L), isnotnull(s_nationkey#489L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
                                 :           :           +- CoalesceBatches
                                 :           :              +- ColumnarExchange hashpartitioning(ps_suppkey#510L, ps_partkey#509L, 1), ENSURE_REQUIREMENTS, [id=#2376], [id=#2376], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_supplycost:DoubleType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_supplycost:DoubleType)
                                 :           :                 +- *(7) FilterExecTransformer (isnotnull(ps_suppkey#510L) AND isnotnull(ps_partkey#509L))
                                 :           :                    +- *(7) FileScan parquet default.partsupp[ps_partkey#509L,ps_suppkey#510L,ps_supplycost#512] Batched: true, DataFilters: [isnotnull(ps_suppkey#510L), isnotnull(ps_partkey#509L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey), IsNotNull(ps_partkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_supplycost:double>
                                 :           +- CoalesceBatches
                                 :              +- ColumnarExchange hashpartitioning(o_orderkey#514L, 1), ENSURE_REQUIREMENTS, [id=#2386], [id=#2386], [OUTPUT] List(o_orderkey:LongType, o_orderdate:DateType), [OUTPUT] List(o_orderkey:LongType, o_orderdate:DateType)
                                 :                 +- *(9) FilterExecTransformer isnotnull(o_orderkey#514L)
                                 :                    +- *(9) FileScan parquet default.orders[o_orderkey#514L,o_orderdate#518] Batched: true, DataFilters: [isnotnull(o_orderkey#514L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_orderdate:date>
                                 +- CoalesceBatches
                                    +- ColumnarExchange hashpartitioning(n_nationkey#523L, 1), ENSURE_REQUIREMENTS, [id=#2396], [id=#2396], [OUTPUT] List(n_nationkey:LongType, n_name:StringType), [OUTPUT] List(n_nationkey:LongType, n_name:StringType)
                                       +- *(11) FilterExecTransformer isnotnull(n_nationkey#523L)
                                          +- *(11) FileScan parquet default.nation[n_nationkey#523L,n_name#524] Batched: true, DataFilters: [isnotnull(n_nationkey#523L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>

````

[返回目录](#目录)

## Q10: 货运存在问题的查询 ✓
* Q10 语句是查询每个国家在某时刻起的**三个月内**货运存在问题的客户和造成的损失。
* Q10语句的特点是：带有分组、排序、聚集操作并存的多表连接查询操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前10行（通常依赖于应用程序实现）。
````mysql
select
	c_custkey, # 客户信息
	c_name,    # 客户信息
	sum(l_extendedprice * (1 - l_discount)) as revenue, # 收入损失
	c_acctbal,
	n_name,   # 国家
	c_address,# 地址
	c_phone,  # 电话
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= '1993-10-01' # DATE 每月 1 号
	and o_orderdate < date_add('1993-10-01', interval '3' month) # 3个月内
	and l_returnflag = 'R' # 货物被回退
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;
````

````
== Parsed Logical Plan ==
'GlobalLimit 20
+- 'LocalLimit 20
   +- 'Sort ['revenue DESC NULLS LAST], true
      +- 'Aggregate ['c_custkey, 'c_name, 'c_acctbal, 'c_phone, 'n_name, 'c_address, 'c_comment], ['c_custkey, 'c_name, 'sum(('l_extendedprice * (1 - 'l_discount))) AS revenue#535, 'c_acctbal, 'n_name, 'c_address, 'c_phone, 'c_comment]
         +- 'Filter (((('c_custkey = 'o_custkey) AND ('l_orderkey = 'o_orderkey)) AND ('o_orderdate >= 8674)) AND ((('o_orderdate < (8674 + 3 months)) AND ('l_returnflag = R)) AND ('c_nationkey = 'n_nationkey)))
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [customer], [], false
               :  :  +- 'UnresolvedRelation [orders], [], false
               :  +- 'UnresolvedRelation [lineitem], [], false
               +- 'UnresolvedRelation [nation], [], false

== Analyzed Logical Plan ==
c_custkey: bigint, c_name: string, revenue: double, c_acctbal: double, n_name: string, c_address: string, c_phone: string, c_comment: string
GlobalLimit 20
+- LocalLimit 20
   +- Sort [revenue#535 DESC NULLS LAST], true
      +- Aggregate [c_custkey#536L, c_name#537, c_acctbal#541, c_phone#540, n_name#570, c_address#538, c_comment#543], [c_custkey#536L, c_name#537, sum((l_extendedprice#558 * (cast(1 as double) - l_discount#559))) AS revenue#535, c_acctbal#541, n_name#570, c_address#538, c_phone#540, c_comment#543]
         +- Filter ((((c_custkey#536L = o_custkey#545L) AND (l_orderkey#553L = o_orderkey#544L)) AND (o_orderdate#548 >= 8674)) AND (((o_orderdate#548 < 8674 + 3 months) AND (l_returnflag#561 = R)) AND (c_nationkey#539L = n_nationkey#569L)))
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.default.customer
               :  :  :  +- Relation[c_custkey#536L,c_name#537,c_address#538,c_nationkey#539L,c_phone#540,c_acctbal#541,c_mktsegment#542,c_comment#543] parquet
               :  :  +- SubqueryAlias spark_catalog.default.orders
               :  :     +- Relation[o_orderkey#544L,o_custkey#545L,o_orderstatus#546,o_totalprice#547,o_orderdate#548,o_orderpriority#549,o_clerk#550,o_shippriority#551L,o_comment#552] parquet
               :  +- SubqueryAlias spark_catalog.default.lineitem
               :     +- Relation[l_orderkey#553L,l_partkey#554L,l_suppkey#555L,l_linenumber#556L,l_quantity#557,l_extendedprice#558,l_discount#559,l_tax#560,l_returnflag#561,l_linestatus#562,l_shipdate#563,l_commitdate#564,l_receiptdate#565,l_shipinstruct#566,l_shipmode#567,l_comment#568] parquet
               +- SubqueryAlias spark_catalog.default.nation
                  +- Relation[n_nationkey#569L,n_name#570,n_regionkey#571L,n_comment#572] parquet

== Optimized Logical Plan ==
GlobalLimit 20
+- LocalLimit 20
   +- Sort [revenue#535 DESC NULLS LAST], true
      +- Aggregate [c_custkey#536L, c_name#537, c_acctbal#541, c_phone#540, n_name#570, c_address#538, c_comment#543], [c_custkey#536L, c_name#537, sum((l_extendedprice#558 * (1.0 - l_discount#559))) AS revenue#535, c_acctbal#541, n_name#570, c_address#538, c_phone#540, c_comment#543]
         +- Project [c_custkey#536L, c_name#537, c_address#538, c_phone#540, c_acctbal#541, c_comment#543, l_extendedprice#558, l_discount#559, n_name#570]
            +- Join Inner, (c_nationkey#539L = n_nationkey#569L)
               :- Project [c_custkey#536L, c_name#537, c_address#538, c_nationkey#539L, c_phone#540, c_acctbal#541, c_comment#543, l_extendedprice#558, l_discount#559]
               :  +- Join Inner, (l_orderkey#553L = o_orderkey#544L)
               :     :- Project [c_custkey#536L, c_name#537, c_address#538, c_nationkey#539L, c_phone#540, c_acctbal#541, c_comment#543, o_orderkey#544L]
               :     :  +- Join Inner, (c_custkey#536L = o_custkey#545L)
               :     :     :- Project [c_custkey#536L, c_name#537, c_address#538, c_nationkey#539L, c_phone#540, c_acctbal#541, c_comment#543]
               :     :     :  +- Filter (isnotnull(c_custkey#536L) AND isnotnull(c_nationkey#539L))
               :     :     :     +- Relation[c_custkey#536L,c_name#537,c_address#538,c_nationkey#539L,c_phone#540,c_acctbal#541,c_mktsegment#542,c_comment#543] parquet
               :     :     +- Project [o_orderkey#544L, o_custkey#545L]
               :     :        +- Filter ((((isnotnull(o_orderdate#548) AND (o_orderdate#548 >= 8674)) AND (o_orderdate#548 < 8766)) AND isnotnull(o_custkey#545L)) AND isnotnull(o_orderkey#544L))
               :     :           +- Relation[o_orderkey#544L,o_custkey#545L,o_orderstatus#546,o_totalprice#547,o_orderdate#548,o_orderpriority#549,o_clerk#550,o_shippriority#551L,o_comment#552] parquet
               :     +- Project [l_orderkey#553L, l_extendedprice#558, l_discount#559]
               :        +- Filter ((isnotnull(l_returnflag#561) AND (l_returnflag#561 = R)) AND isnotnull(l_orderkey#553L))
               :           +- Relation[l_orderkey#553L,l_partkey#554L,l_suppkey#555L,l_linenumber#556L,l_quantity#557,l_extendedprice#558,l_discount#559,l_tax#560,l_returnflag#561,l_linestatus#562,l_shipdate#563,l_commitdate#564,l_receiptdate#565,l_shipinstruct#566,l_shipmode#567,l_comment#568] parquet
               +- Project [n_nationkey#569L, n_name#570]
                  +- Filter isnotnull(n_nationkey#569L)
                     +- Relation[n_nationkey#569L,n_name#570,n_regionkey#571L,n_comment#572] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=20, orderBy=[revenue#535 DESC NULLS LAST], output=[c_custkey#536L,c_name#537,revenue#535,c_acctbal#541,n_name#570,c_address#538,c_phone#540,c_comment#543])
+- *(1) ColumnarToRow
   +- *(8) HashAggregateTransformer(keys=[c_custkey#536L, c_name#537, c_acctbal#541, c_phone#540, n_name#570, c_address#538, c_comment#543], functions=[sum((l_extendedprice#558 * (1.0 - l_discount#559)))], output=[c_custkey#536L, c_name#537, revenue#535, c_acctbal#541, n_name#570, c_address#538, c_phone#540, c_comment#543])
      +- CoalesceBatches
         +- ColumnarExchange hashpartitioning(c_custkey#536L, c_name#537, c_acctbal#541, c_phone#540, n_name#570, c_address#538, c_comment#543, 1), ENSURE_REQUIREMENTS, [id=#2656], [id=#2656], [OUTPUT] ArrayBuffer(c_custkey:LongType, c_name:StringType, c_acctbal:DoubleType, c_phone:StringType, n_name:StringType, c_address:StringType, c_comment:StringType, sum:DoubleType), [OUTPUT] ArrayBuffer(c_custkey:LongType, c_name:StringType, c_acctbal:DoubleType, c_phone:StringType, n_name:StringType, c_address:StringType, c_comment:StringType, sum:DoubleType)
            +- *(7) HashAggregateTransformer(keys=[c_custkey#536L, c_name#537, knownfloatingpointnormalized(normalizenanandzero(c_acctbal#541)) AS c_acctbal#541, c_phone#540, n_name#570, c_address#538, c_comment#543], functions=[partial_sum((l_extendedprice#558 * (1.0 - l_discount#559)))], output=[c_custkey#536L, c_name#537, c_acctbal#541, c_phone#540, n_name#570, c_address#538, c_comment#543, sum#584])
               +- *(7) ProjectExecTransformer [c_custkey#536L, c_name#537, c_address#538, c_phone#540, c_acctbal#541, c_comment#543, l_extendedprice#558, l_discount#559, n_name#570]
                  +- *(7) ShuffledHashJoinExecTransformer [c_nationkey#539L], [n_nationkey#569L], Inner, BuildRight
                     :- CoalesceBatches
                     :  +- ColumnarExchange hashpartitioning(c_nationkey#539L, 1), ENSURE_REQUIREMENTS, [id=#2645], [id=#2645], [OUTPUT] List(c_custkey:LongType, c_name:StringType, c_address:StringType, c_nationkey:LongType, c_phone:StringType, c_acctbal:DoubleType, c_comment:StringType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(c_custkey:LongType, c_name:StringType, c_address:StringType, c_nationkey:LongType, c_phone:StringType, c_acctbal:DoubleType, c_comment:StringType, l_extendedprice:DoubleType, l_discount:DoubleType)
                     :     +- *(5) ProjectExecTransformer [c_custkey#536L, c_name#537, c_address#538, c_nationkey#539L, c_phone#540, c_acctbal#541, c_comment#543, l_extendedprice#558, l_discount#559]
                     :        +- *(5) ShuffledHashJoinExecTransformer [o_orderkey#544L], [l_orderkey#553L], Inner, BuildRight
                     :           :- CoalesceBatches
                     :           :  +- ColumnarExchange hashpartitioning(o_orderkey#544L, 1), ENSURE_REQUIREMENTS, [id=#2635], [id=#2635], [OUTPUT] List(c_custkey:LongType, c_name:StringType, c_address:StringType, c_nationkey:LongType, c_phone:StringType, c_acctbal:DoubleType, c_comment:StringType, o_orderkey:LongType), [OUTPUT] List(c_custkey:LongType, c_name:StringType, c_address:StringType, c_nationkey:LongType, c_phone:StringType, c_acctbal:DoubleType, c_comment:StringType, o_orderkey:LongType)
                     :           :     +- *(3) ProjectExecTransformer [c_custkey#536L, c_name#537, c_address#538, c_nationkey#539L, c_phone#540, c_acctbal#541, c_comment#543, o_orderkey#544L]
                     :           :        +- *(3) ShuffledHashJoinExecTransformer [c_custkey#536L], [o_custkey#545L], Inner, BuildLeft
                     :           :           :- CoalesceBatches
                     :           :           :  +- ColumnarExchange hashpartitioning(c_custkey#536L, 1), ENSURE_REQUIREMENTS, [id=#2625], [id=#2625], [OUTPUT] List(c_custkey:LongType, c_name:StringType, c_address:StringType, c_nationkey:LongType, c_phone:StringType, c_acctbal:DoubleType, c_comment:StringType), [OUTPUT] List(c_custkey:LongType, c_name:StringType, c_address:StringType, c_nationkey:LongType, c_phone:StringType, c_acctbal:DoubleType, c_comment:StringType)
                     :           :           :     +- *(1) FilterExecTransformer (isnotnull(c_custkey#536L) AND isnotnull(c_nationkey#539L))
                     :           :           :        +- *(1) FileScan parquet default.customer[c_custkey#536L,c_name#537,c_address#538,c_nationkey#539L,c_phone#540,c_acctbal#541,c_comment#543] Batched: true, DataFilters: [isnotnull(c_custkey#536L), isnotnull(c_nationkey#539L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_custkey), IsNotNull(c_nationkey)], ReadSchema: struct<c_custkey:bigint,c_name:string,c_address:string,c_nationkey:bigint,c_phone:string,c_acctba...
                     :           :           +- CoalesceBatches
                     :           :              +- ColumnarExchange hashpartitioning(o_custkey#545L, 1), ENSURE_REQUIREMENTS, [id=#2629], [id=#2629], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType)
                     :           :                 +- *(2) ProjectExecTransformer [o_orderkey#544L, o_custkey#545L]
                     :           :                    +- *(2) FilterExecTransformer ((((isnotnull(o_orderdate#548) AND (o_orderdate#548 >= 8674)) AND (o_orderdate#548 < 8766)) AND isnotnull(o_custkey#545L)) AND isnotnull(o_orderkey#544L))
                     :           :                       +- *(2) FileScan parquet default.orders[o_orderkey#544L,o_custkey#545L,o_orderdate#548] Batched: true, DataFilters: [isnotnull(o_orderdate#548), (o_orderdate#548 >= 8674), (o_orderdate#548 < 8766), isnotnull(o_cus..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderdate), GreaterThanOrEqual(o_orderdate,1993-10-01), LessThan(o_orderdate,1994-01..., ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_orderdate:date>
                     :           +- CoalesceBatches
                     :              +- ColumnarExchange hashpartitioning(l_orderkey#553L, 1), ENSURE_REQUIREMENTS, [id=#2639], [id=#2639], [OUTPUT] List(l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                     :                 +- *(4) ProjectExecTransformer [l_orderkey#553L, l_extendedprice#558, l_discount#559]
                     :                    +- *(4) FilterExecTransformer ((isnotnull(l_returnflag#561) AND (l_returnflag#561 = R)) AND isnotnull(l_orderkey#553L))
                     :                       +- *(4) FileScan parquet default.lineitem[l_orderkey#553L,l_extendedprice#558,l_discount#559,l_returnflag#561] Batched: true, DataFilters: [isnotnull(l_returnflag#561), (l_returnflag#561 = R), isnotnull(l_orderkey#553L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_returnflag), EqualTo(l_returnflag,R), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_extendedprice:double,l_discount:double,l_returnflag:string>
                     +- CoalesceBatches
                        +- ColumnarExchange hashpartitioning(n_nationkey#569L, 1), ENSURE_REQUIREMENTS, [id=#2649], [id=#2649], [OUTPUT] List(n_nationkey:LongType, n_name:StringType), [OUTPUT] List(n_nationkey:LongType, n_name:StringType)
                           +- *(6) FilterExecTransformer isnotnull(n_nationkey#569L)
                              +- *(6) FileScan parquet default.nation[n_nationkey#569L,n_name#570] Batched: true, DataFilters: [isnotnull(n_nationkey#569L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>

````

[返回目录](#目录)

## Q11: 库存价值查询
* Q11 语句是查询库存中某个国家供应的零件的价值。
* Q11 语句的特点是：带有分组、排序、聚集、子查询操作并存的多表连接查询操作。子查询位于分组操作的HAVING条件中。
````mysql
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value # 聚集操作，商品的总价值
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by
	ps_partkey having # 带有HAVING子句的分组操作
		sum(ps_supplycost * ps_availqty) > ( # HAVING子句中包括有子查询
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000 # 子查询中存在聚集操作；FRACTION为0.0001/SF1
			from  # 与父查询的表连接一致
				partsupp,
				supplier,
				nation
			where # 与父查询的WHEWR条件一致
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'GERMANY'
		)
order by # 按商品的价值降序排序
	value desc;
````

````
== Parsed Logical Plan ==
'Sort ['value DESC NULLS LAST], true
+- 'UnresolvedHaving ('sum(('ps_supplycost * 'ps_availqty)) > scalar-subquery#586 [])
   :  +- 'Project [unresolvedalias(('sum(('ps_supplycost * 'ps_availqty)) * 0.0001000000), None)]
   :     +- 'Filter ((('ps_suppkey = 's_suppkey) AND ('s_nationkey = 'n_nationkey)) AND ('n_name = GERMANY))
   :        +- 'Join Inner
   :           :- 'Join Inner
   :           :  :- 'UnresolvedRelation [partsupp], [], false
   :           :  +- 'UnresolvedRelation [supplier], [], false
   :           +- 'UnresolvedRelation [nation], [], false
   +- 'Aggregate ['ps_partkey], ['ps_partkey, 'sum(('ps_supplycost * 'ps_availqty)) AS value#585]
      +- 'Filter ((('ps_suppkey = 's_suppkey) AND ('s_nationkey = 'n_nationkey)) AND ('n_name = GERMANY))
         +- 'Join Inner
            :- 'Join Inner
            :  :- 'UnresolvedRelation [partsupp], [], false
            :  +- 'UnresolvedRelation [supplier], [], false
            +- 'UnresolvedRelation [nation], [], false

== Analyzed Logical Plan ==
ps_partkey: bigint, value: double
Sort [value#585 DESC NULLS LAST], true
+- Project [ps_partkey#587L, value#585]
   +- Filter (sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624 > scalar-subquery#586 [])
      :  +- Aggregate [(sum((ps_supplycost#607 * cast(ps_availqty#606L as double))) * cast(0.0001000000 as double)) AS (sum((ps_supplycost * CAST(ps_availqty AS DOUBLE))) * CAST(0.0001000000 AS DOUBLE))#621]
      :     +- Filter (((ps_suppkey#605L = s_suppkey#609L) AND (s_nationkey#612L = n_nationkey#616L)) AND (n_name#617 = GERMANY))
      :        +- Join Inner
      :           :- Join Inner
      :           :  :- SubqueryAlias spark_catalog.default.partsupp
      :           :  :  +- Relation[ps_partkey#604L,ps_suppkey#605L,ps_availqty#606L,ps_supplycost#607,ps_comment#608] parquet
      :           :  +- SubqueryAlias spark_catalog.default.supplier
      :           :     +- Relation[s_suppkey#609L,s_name#610,s_address#611,s_nationkey#612L,s_phone#613,s_acctbal#614,s_comment#615] parquet
      :           +- SubqueryAlias spark_catalog.default.nation
      :              +- Relation[n_nationkey#616L,n_name#617,n_regionkey#618L,n_comment#619] parquet
      +- Aggregate [ps_partkey#587L], [ps_partkey#587L, sum((ps_supplycost#590 * cast(ps_availqty#589L as double))) AS value#585, sum((ps_supplycost#590 * cast(ps_availqty#589L as double))) AS sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624]
         +- Filter (((ps_suppkey#588L = s_suppkey#592L) AND (s_nationkey#595L = n_nationkey#599L)) AND (n_name#600 = GERMANY))
            +- Join Inner
               :- Join Inner
               :  :- SubqueryAlias spark_catalog.default.partsupp
               :  :  +- Relation[ps_partkey#587L,ps_suppkey#588L,ps_availqty#589L,ps_supplycost#590,ps_comment#591] parquet
               :  +- SubqueryAlias spark_catalog.default.supplier
               :     +- Relation[s_suppkey#592L,s_name#593,s_address#594,s_nationkey#595L,s_phone#596,s_acctbal#597,s_comment#598] parquet
               +- SubqueryAlias spark_catalog.default.nation
                  +- Relation[n_nationkey#599L,n_name#600,n_regionkey#601L,n_comment#602] parquet

== Optimized Logical Plan ==
Sort [value#585 DESC NULLS LAST], true
+- Project [ps_partkey#587L, value#585]
   +- Filter (isnotnull(sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624) AND (sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624 > scalar-subquery#586 []))
      :  +- Aggregate [(sum((ps_supplycost#607 * cast(ps_availqty#606L as double))) * 1.0E-4) AS (sum((ps_supplycost * CAST(ps_availqty AS DOUBLE))) * CAST(0.0001000000 AS DOUBLE))#621]
      :     +- Project [ps_availqty#606L, ps_supplycost#607]
      :        +- Join Inner, (s_nationkey#612L = n_nationkey#616L)
      :           :- Project [ps_availqty#606L, ps_supplycost#607, s_nationkey#612L]
      :           :  +- Join Inner, (ps_suppkey#605L = s_suppkey#609L)
      :           :     :- Project [ps_suppkey#605L, ps_availqty#606L, ps_supplycost#607]
      :           :     :  +- Filter isnotnull(ps_suppkey#605L)
      :           :     :     +- Relation[ps_partkey#604L,ps_suppkey#605L,ps_availqty#606L,ps_supplycost#607,ps_comment#608] parquet
      :           :     +- Project [s_suppkey#609L, s_nationkey#612L]
      :           :        +- Filter (isnotnull(s_suppkey#609L) AND isnotnull(s_nationkey#612L))
      :           :           +- Relation[s_suppkey#609L,s_name#610,s_address#611,s_nationkey#612L,s_phone#613,s_acctbal#614,s_comment#615] parquet
      :           +- Project [n_nationkey#616L]
      :              +- Filter ((isnotnull(n_name#617) AND (n_name#617 = GERMANY)) AND isnotnull(n_nationkey#616L))
      :                 +- Relation[n_nationkey#616L,n_name#617,n_regionkey#618L,n_comment#619] parquet
      +- Aggregate [ps_partkey#587L], [ps_partkey#587L, sum((ps_supplycost#590 * cast(ps_availqty#589L as double))) AS value#585, sum((ps_supplycost#590 * cast(ps_availqty#589L as double))) AS sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624]
         +- Project [ps_partkey#587L, ps_availqty#589L, ps_supplycost#590]
            +- Join Inner, (s_nationkey#595L = n_nationkey#599L)
               :- Project [ps_partkey#587L, ps_availqty#589L, ps_supplycost#590, s_nationkey#595L]
               :  +- Join Inner, (ps_suppkey#588L = s_suppkey#592L)
               :     :- Project [ps_partkey#587L, ps_suppkey#588L, ps_availqty#589L, ps_supplycost#590]
               :     :  +- Filter isnotnull(ps_suppkey#588L)
               :     :     +- Relation[ps_partkey#587L,ps_suppkey#588L,ps_availqty#589L,ps_supplycost#590,ps_comment#591] parquet
               :     +- Project [s_suppkey#592L, s_nationkey#595L]
               :        +- Filter (isnotnull(s_suppkey#592L) AND isnotnull(s_nationkey#595L))
               :           +- Relation[s_suppkey#592L,s_name#593,s_address#594,s_nationkey#595L,s_phone#596,s_acctbal#597,s_comment#598] parquet
               +- Project [n_nationkey#599L]
                  +- Filter ((isnotnull(n_name#600) AND (n_name#600 = GERMANY)) AND isnotnull(n_nationkey#599L))
                     +- Relation[n_nationkey#599L,n_name#600,n_regionkey#601L,n_comment#602] parquet

== Physical Plan ==
*(2) Sort [value#585 DESC NULLS LAST], true, 0
+- *(2) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(value#585 DESC NULLS LAST, 1), ENSURE_REQUIREMENTS, [id=#2982], [id=#2982], [OUTPUT] List(ps_partkey:LongType, value:DoubleType), [OUTPUT] List(ps_partkey:LongType, value:DoubleType)
         +- *(7) ProjectExecTransformer [ps_partkey#587L, value#585]
            +- RowToColumnar
               +- *(1) Filter (isnotnull(sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624) AND (sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624 > Subquery scalar-subquery#586, [id=#2841]))
                  :  +- Subquery scalar-subquery#586, [id=#2841]
                  :     +- *(1) ColumnarToRow
                  :        +- *(5) HashAggregateTransformer(keys=[], functions=[sum((ps_supplycost#607 * cast(ps_availqty#606L as double)))], output=[(sum((ps_supplycost * CAST(ps_availqty AS DOUBLE))) * CAST(0.0001000000 AS DOUBLE))#621])
                  :           +- *(5) HashAggregateTransformer(keys=[], functions=[partial_sum((ps_supplycost#607 * cast(ps_availqty#606L as double)))], output=[sum#632])
                  :              +- *(5) ProjectExecTransformer [ps_availqty#606L, ps_supplycost#607]
                  :                 +- *(5) ShuffledHashJoinExecTransformer [s_nationkey#612L], [n_nationkey#616L], Inner, BuildRight
                  :                    :- CoalesceBatches
                  :                    :  +- ColumnarExchange hashpartitioning(s_nationkey#612L, 1), ENSURE_REQUIREMENTS, [id=#2825], [id=#2825], [OUTPUT] List(ps_availqty:LongType, ps_supplycost:DoubleType, s_nationkey:LongType), [OUTPUT] List(ps_availqty:LongType, ps_supplycost:DoubleType, s_nationkey:LongType)
                  :                    :     +- *(3) ProjectExecTransformer [ps_availqty#606L, ps_supplycost#607, s_nationkey#612L]
                  :                    :        +- *(3) ShuffledHashJoinExecTransformer [ps_suppkey#605L], [s_suppkey#609L], Inner, BuildRight
                  :                    :           :- CoalesceBatches
                  :                    :           :  +- ColumnarExchange hashpartitioning(ps_suppkey#605L, 1), ENSURE_REQUIREMENTS, [id=#2815], [id=#2815], [OUTPUT] List(ps_suppkey:LongType, ps_availqty:LongType, ps_supplycost:DoubleType), [OUTPUT] List(ps_suppkey:LongType, ps_availqty:LongType, ps_supplycost:DoubleType)
                  :                    :           :     +- *(1) FilterExecTransformer isnotnull(ps_suppkey#605L)
                  :                    :           :        +- *(1) FileScan parquet default.partsupp[ps_suppkey#605L,ps_availqty#606L,ps_supplycost#607] Batched: true, DataFilters: [isnotnull(ps_suppkey#605L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey)], ReadSchema: struct<ps_suppkey:bigint,ps_availqty:bigint,ps_supplycost:double>
                  :                    :           +- CoalesceBatches
                  :                    :              +- ColumnarExchange hashpartitioning(s_suppkey#609L, 1), ENSURE_REQUIREMENTS, [id=#2819], [id=#2819], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
                  :                    :                 +- *(2) FilterExecTransformer (isnotnull(s_suppkey#609L) AND isnotnull(s_nationkey#612L))
                  :                    :                    +- *(2) FileScan parquet default.supplier[s_suppkey#609L,s_nationkey#612L] Batched: true, DataFilters: [isnotnull(s_suppkey#609L), isnotnull(s_nationkey#612L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
                  :                    +- CoalesceBatches
                  :                       +- ColumnarExchange hashpartitioning(n_nationkey#616L, 1), ENSURE_REQUIREMENTS, [id=#2829], [id=#2829], [OUTPUT] List(n_nationkey:LongType), [OUTPUT] List(n_nationkey:LongType)
                  :                          +- *(4) ProjectExecTransformer [n_nationkey#616L]
                  :                             +- *(4) FilterExecTransformer ((isnotnull(n_name#617) AND (n_name#617 = GERMANY)) AND isnotnull(n_nationkey#616L))
                  :                                +- *(4) FileScan parquet default.nation[n_nationkey#616L,n_name#617] Batched: true, DataFilters: [isnotnull(n_name#617), (n_name#617 = GERMANY), isnotnull(n_nationkey#616L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_name), EqualTo(n_name,GERMANY), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>
                  +- *(1) ColumnarToRow
                     +- *(6) HashAggregateTransformer(keys=[ps_partkey#587L], functions=[sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))], output=[ps_partkey#587L, value#585, sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))#624])
                        +- CoalesceBatches
                           +- ColumnarExchange hashpartitioning(ps_partkey#587L, 1), ENSURE_REQUIREMENTS, [id=#2959], [id=#2959], [OUTPUT] ArrayBuffer(ps_partkey:LongType, sum:DoubleType), [OUTPUT] ArrayBuffer(ps_partkey:LongType, sum:DoubleType)
                              +- *(5) HashAggregateTransformer(keys=[ps_partkey#587L], functions=[partial_sum((ps_supplycost#590 * cast(ps_availqty#589L as double)))], output=[ps_partkey#587L, sum#630])
                                 +- *(5) ProjectExecTransformer [ps_partkey#587L, ps_availqty#589L, ps_supplycost#590]
                                    +- *(5) ShuffledHashJoinExecTransformer [s_nationkey#595L], [n_nationkey#599L], Inner, BuildRight
                                       :- CoalesceBatches
                                       :  +- ColumnarExchange hashpartitioning(s_nationkey#595L, 1), ENSURE_REQUIREMENTS, [id=#2948], [id=#2948], [OUTPUT] List(ps_partkey:LongType, ps_availqty:LongType, ps_supplycost:DoubleType, s_nationkey:LongType), [OUTPUT] List(ps_partkey:LongType, ps_availqty:LongType, ps_supplycost:DoubleType, s_nationkey:LongType)
                                       :     +- *(3) ProjectExecTransformer [ps_partkey#587L, ps_availqty#589L, ps_supplycost#590, s_nationkey#595L]
                                       :        +- *(3) ShuffledHashJoinExecTransformer [ps_suppkey#588L], [s_suppkey#592L], Inner, BuildRight
                                       :           :- CoalesceBatches
                                       :           :  +- ColumnarExchange hashpartitioning(ps_suppkey#588L, 1), ENSURE_REQUIREMENTS, [id=#2938], [id=#2938], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_availqty:LongType, ps_supplycost:DoubleType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_availqty:LongType, ps_supplycost:DoubleType)
                                       :           :     +- *(1) FilterExecTransformer isnotnull(ps_suppkey#588L)
                                       :           :        +- *(1) FileScan parquet default.partsupp[ps_partkey#587L,ps_suppkey#588L,ps_availqty#589L,ps_supplycost#590] Batched: true, DataFilters: [isnotnull(ps_suppkey#588L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_suppkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_availqty:bigint,ps_supplycost:double>
                                       :           +- CoalesceBatches
                                       :              +- ColumnarExchange hashpartitioning(s_suppkey#592L, 1), ENSURE_REQUIREMENTS, [id=#2942], [id=#2942], [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_nationkey:LongType)
                                       :                 +- *(2) FilterExecTransformer (isnotnull(s_suppkey#592L) AND isnotnull(s_nationkey#595L))
                                       :                    +- *(2) FileScan parquet default.supplier[s_suppkey#592L,s_nationkey#595L] Batched: true, DataFilters: [isnotnull(s_suppkey#592L), isnotnull(s_nationkey#595L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_nationkey:bigint>
                                       +- CoalesceBatches
                                          +- ColumnarExchange hashpartitioning(n_nationkey#599L, 1), ENSURE_REQUIREMENTS, [id=#2952], [id=#2952], [OUTPUT] List(n_nationkey:LongType), [OUTPUT] List(n_nationkey:LongType)
                                             +- *(4) ProjectExecTransformer [n_nationkey#599L]
                                                +- *(4) FilterExecTransformer ((isnotnull(n_name#600) AND (n_name#600 = GERMANY)) AND isnotnull(n_nationkey#599L))
                                                   +- *(4) FileScan parquet default.nation[n_nationkey#599L,n_name#600] Batched: true, DataFilters: [isnotnull(n_name#600), (n_name#600 = GERMANY), isnotnull(n_nationkey#599L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_name), EqualTo(n_name,GERMANY), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>

````

[返回目录](#目录)

## Q12: 货运模式和订单优先级查询
* Q12 语句查询获得货运模式和订单优先级。可以帮助决策：选择便宜的货运模式是否会导致消费者更多的在合同日期之后收到货物，而对紧急优先命令产生负面影响。
* Q12 语句的特点是：带有分组、排序、聚集操作并存的两表连接查询操作。
* TPC-H 标准定义了Q12语句等价的变形SQL，与上述查询语句格式上基本相同，主要是目标列使用了不同的表达方式，在此不再赘述。
````mysql
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT' # OR运算，二者满足其一即可，选出URGENT或HIGH的
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT' # AND运算，二者都不满足，非URGENT非HIGH的
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('MAIL', 'SHIP') # 指定货运模式的类型，在TPC-H标准指定的范围内随机选择，SHIPMODE2必须有别于SHIPMODE1
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= '1994-01-01' # 从1993年到1997年中任一年的一月一号
	and l_receiptdate < date_add('1994-01-01', interval '1' year) # 1年内
group by
	l_shipmode
order by
	l_shipmode;
````

````
== Parsed Logical Plan ==
'Sort ['l_shipmode ASC NULLS FIRST], true
+- 'Aggregate ['l_shipmode], ['l_shipmode, 'sum(CASE WHEN (('o_orderpriority = 1-URGENT) OR ('o_orderpriority = 2-HIGH)) THEN 1 ELSE 0 END) AS high_line_count#633, 'sum(CASE WHEN (NOT ('o_orderpriority = 1-URGENT) AND NOT ('o_orderpriority = 2-HIGH)) THEN 1 ELSE 0 END) AS low_line_count#634]
   +- 'Filter (((('o_orderkey = 'l_orderkey) AND 'l_shipmode IN (MAIL,SHIP)) AND ('l_commitdate < 'l_receiptdate)) AND ((('l_shipdate < 'l_commitdate) AND ('l_receiptdate >= 8766)) AND ('l_receiptdate < (8766 + 1 years))))
      +- 'Join Inner
         :- 'UnresolvedRelation [orders], [], false
         +- 'UnresolvedRelation [lineitem], [], false

== Analyzed Logical Plan ==
l_shipmode: string, high_line_count: bigint, low_line_count: bigint
Project [l_shipmode#658, high_line_count#633L, low_line_count#634L]
+- Sort [l_shipmode#658 ASC NULLS FIRST], true
   +- Aggregate [l_shipmode#658], [l_shipmode#658, sum(cast(CASE WHEN ((o_orderpriority#640 = 1-URGENT) OR (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint)) AS high_line_count#633L, sum(cast(CASE WHEN (NOT (o_orderpriority#640 = 1-URGENT) AND NOT (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint)) AS low_line_count#634L]
      +- Filter ((((o_orderkey#635L = l_orderkey#644L) AND l_shipmode#658 IN (MAIL,SHIP)) AND (l_commitdate#655 < l_receiptdate#656)) AND (((l_shipdate#654 < l_commitdate#655) AND (l_receiptdate#656 >= 8766)) AND (l_receiptdate#656 < 8766 + 1 years)))
         +- Join Inner
            :- SubqueryAlias spark_catalog.default.orders
            :  +- Relation[o_orderkey#635L,o_custkey#636L,o_orderstatus#637,o_totalprice#638,o_orderdate#639,o_orderpriority#640,o_clerk#641,o_shippriority#642L,o_comment#643] parquet
            +- SubqueryAlias spark_catalog.default.lineitem
               +- Relation[l_orderkey#644L,l_partkey#645L,l_suppkey#646L,l_linenumber#647L,l_quantity#648,l_extendedprice#649,l_discount#650,l_tax#651,l_returnflag#652,l_linestatus#653,l_shipdate#654,l_commitdate#655,l_receiptdate#656,l_shipinstruct#657,l_shipmode#658,l_comment#659] parquet

== Optimized Logical Plan ==
Sort [l_shipmode#658 ASC NULLS FIRST], true
+- Aggregate [l_shipmode#658], [l_shipmode#658, sum(cast(CASE WHEN ((o_orderpriority#640 = 1-URGENT) OR (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint)) AS high_line_count#633L, sum(cast(CASE WHEN (NOT (o_orderpriority#640 = 1-URGENT) AND NOT (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint)) AS low_line_count#634L]
   +- Project [o_orderpriority#640, l_shipmode#658]
      +- Join Inner, (o_orderkey#635L = l_orderkey#644L)
         :- Project [o_orderkey#635L, o_orderpriority#640]
         :  +- Filter isnotnull(o_orderkey#635L)
         :     +- Relation[o_orderkey#635L,o_custkey#636L,o_orderstatus#637,o_totalprice#638,o_orderdate#639,o_orderpriority#640,o_clerk#641,o_shippriority#642L,o_comment#643] parquet
         +- Project [l_orderkey#644L, l_shipmode#658]
            +- Filter ((((((((isnotnull(l_commitdate#655) AND isnotnull(l_receiptdate#656)) AND isnotnull(l_shipdate#654)) AND l_shipmode#658 IN (MAIL,SHIP)) AND (l_commitdate#655 < l_receiptdate#656)) AND (l_shipdate#654 < l_commitdate#655)) AND (l_receiptdate#656 >= 8766)) AND (l_receiptdate#656 < 9131)) AND isnotnull(l_orderkey#644L))
               +- Relation[l_orderkey#644L,l_partkey#645L,l_suppkey#646L,l_linenumber#647L,l_quantity#648,l_extendedprice#649,l_discount#650,l_tax#651,l_returnflag#652,l_linestatus#653,l_shipdate#654,l_commitdate#655,l_receiptdate#656,l_shipinstruct#657,l_shipmode#658,l_comment#659] parquet

== Physical Plan ==
*(3) Sort [l_shipmode#658 ASC NULLS FIRST], true, 0
+- *(3) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(l_shipmode#658 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#3154], [id=#3154], [OUTPUT] List(l_shipmode:StringType, high_line_count:LongType, low_line_count:LongType), [OUTPUT] List(l_shipmode:StringType, high_line_count:LongType, low_line_count:LongType)
         +- *(5) HashAggregateTransformer(keys=[l_shipmode#658], functions=[sum(cast(CASE WHEN ((o_orderpriority#640 = 1-URGENT) OR (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (NOT (o_orderpriority#640 = 1-URGENT) AND NOT (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint))], output=[l_shipmode#658, high_line_count#633L, low_line_count#634L])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(l_shipmode#658, 1), ENSURE_REQUIREMENTS, [id=#3149], [id=#3149], [OUTPUT] ArrayBuffer(l_shipmode:StringType, sum:LongType, sum:LongType), [OUTPUT] ArrayBuffer(l_shipmode:StringType, sum:LongType, sum:LongType)
                  +- RowToColumnar
                     +- *(2) HashAggregate(keys=[l_shipmode#658], functions=[partial_sum(cast(CASE WHEN ((o_orderpriority#640 = 1-URGENT) OR (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (NOT (o_orderpriority#640 = 1-URGENT) AND NOT (o_orderpriority#640 = 2-HIGH)) THEN 1 ELSE 0 END as bigint))], output=[l_shipmode#658, sum#668L, sum#669L])
                        +- *(2) ColumnarToRow
                           +- *(4) ProjectExecTransformer [o_orderpriority#640, l_shipmode#658]
                              +- *(4) ShuffledHashJoinExecTransformer [o_orderkey#635L], [l_orderkey#644L], Inner, BuildLeft
                                 :- CoalesceBatches
                                 :  +- ColumnarExchange hashpartitioning(o_orderkey#635L, 1), ENSURE_REQUIREMENTS, [id=#3102], [id=#3102], [OUTPUT] List(o_orderkey:LongType, o_orderpriority:StringType), [OUTPUT] List(o_orderkey:LongType, o_orderpriority:StringType)
                                 :     +- *(1) FilterExecTransformer isnotnull(o_orderkey#635L)
                                 :        +- *(1) FileScan parquet default.orders[o_orderkey#635L,o_orderpriority#640] Batched: true, DataFilters: [isnotnull(o_orderkey#635L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_orderpriority:string>
                                 +- CoalesceBatches
                                    +- ColumnarExchange hashpartitioning(l_orderkey#644L, 1), ENSURE_REQUIREMENTS, [id=#3138], [id=#3138], [OUTPUT] List(l_orderkey:LongType, l_shipmode:StringType), [OUTPUT] List(l_orderkey:LongType, l_shipmode:StringType)
                                       +- *(3) ProjectExecTransformer [l_orderkey#644L, l_shipmode#658]
                                          +- RowToColumnar
                                             +- *(1) Filter ((((((((isnotnull(l_commitdate#655) AND isnotnull(l_receiptdate#656)) AND isnotnull(l_shipdate#654)) AND l_shipmode#658 IN (MAIL,SHIP)) AND (l_commitdate#655 < l_receiptdate#656)) AND (l_shipdate#654 < l_commitdate#655)) AND (l_receiptdate#656 >= 8766)) AND (l_receiptdate#656 < 9131)) AND isnotnull(l_orderkey#644L))
                                                +- *(1) ColumnarToRow
                                                   +- *(2) FileScan parquet default.lineitem[l_orderkey#644L,l_shipdate#654,l_commitdate#655,l_receiptdate#656,l_shipmode#658] Batched: true, DataFilters: [isnotnull(l_commitdate#655), isnotnull(l_receiptdate#656), isnotnull(l_shipdate#654), l_shipmode..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_commitdate), IsNotNull(l_receiptdate), IsNotNull(l_shipdate), In(l_shipmode, [MAIL,S..., ReadSchema: struct<l_orderkey:bigint,l_shipdate:date,l_commitdate:date,l_receiptdate:date,l_shipmode:string>

````

[返回目录](#目录)

## Q13: 消费者订单数量查询 ✓
* Q13语句查询获得消费者的订单数量，包括过去和现在都没有订单记录的消费者。
* Q13语句的特点是：带有分组、排序、聚集、子查询、左外连接操作并存的查询操作。
TPC-H标准定义了Q13语句等价的变形SQL，与上述查询语句格式上不相同，上述语句使用子查询作为查询的对象，变形的SQL把子查询部分变为视图，然后基于视图做查询，这种做法的意义在于有些数据库不支持如上语法，但存在等价的其他语法
````mysql
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from  # 子查询中包括左外连接操作
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%' 
				# WORD1 为以下四个可能值中任意一个：special、pending、unusual、express
				# WORD2 为以下四个可能值中任意一个：packages、requests、accounts、deposits
		group by
			c_custkey
	) c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;
````

````
== Parsed Logical Plan ==
'Sort ['custdist DESC NULLS LAST, 'c_count DESC NULLS LAST], true
+- 'Aggregate ['c_count], ['c_count, 'count(1) AS custdist#670]
   +- 'UnresolvedSubqueryColumnAliases [c_custkey, c_count]
      +- 'SubqueryAlias c_orders
         +- 'Aggregate ['c_custkey], ['c_custkey, unresolvedalias('count('o_orderkey), None)]
            +- 'Join LeftOuter, (('c_custkey = 'o_custkey) AND NOT 'o_comment LIKE %special%requests%)
               :- 'UnresolvedRelation [customer], [], false
               +- 'UnresolvedRelation [orders], [], false

== Analyzed Logical Plan ==
c_count: bigint, custdist: bigint
Sort [custdist#670L DESC NULLS LAST, c_count#692L DESC NULLS LAST], true
+- Aggregate [c_count#692L], [c_count#692L, count(1) AS custdist#670L]
   +- Project [c_custkey#672L AS c_custkey#691L, count(o_orderkey)#690L AS c_count#692L]
      +- SubqueryAlias c_orders
         +- Aggregate [c_custkey#672L], [c_custkey#672L, count(o_orderkey#680L) AS count(o_orderkey)#690L]
            +- Join LeftOuter, ((c_custkey#672L = o_custkey#681L) AND NOT o_comment#688 LIKE %special%requests%)
               :- SubqueryAlias spark_catalog.default.customer
               :  +- Relation[c_custkey#672L,c_name#673,c_address#674,c_nationkey#675L,c_phone#676,c_acctbal#677,c_mktsegment#678,c_comment#679] parquet
               +- SubqueryAlias spark_catalog.default.orders
                  +- Relation[o_orderkey#680L,o_custkey#681L,o_orderstatus#682,o_totalprice#683,o_orderdate#684,o_orderpriority#685,o_clerk#686,o_shippriority#687L,o_comment#688] parquet

== Optimized Logical Plan ==
Sort [custdist#670L DESC NULLS LAST, c_count#692L DESC NULLS LAST], true
+- Aggregate [c_count#692L], [c_count#692L, count(1) AS custdist#670L]
   +- Aggregate [c_custkey#672L], [count(o_orderkey#680L) AS c_count#692L]
      +- Project [c_custkey#672L, o_orderkey#680L]
         +- Join LeftOuter, (c_custkey#672L = o_custkey#681L)
            :- Project [c_custkey#672L]
            :  +- Relation[c_custkey#672L,c_name#673,c_address#674,c_nationkey#675L,c_phone#676,c_acctbal#677,c_mktsegment#678,c_comment#679] parquet
            +- Project [o_orderkey#680L, o_custkey#681L]
               +- Filter ((isnotnull(o_comment#688) AND NOT o_comment#688 LIKE %special%requests%) AND isnotnull(o_custkey#681L))
                  +- Relation[o_orderkey#680L,o_custkey#681L,o_orderstatus#682,o_totalprice#683,o_orderdate#684,o_orderpriority#685,o_clerk#686,o_shippriority#687L,o_comment#688] parquet

== Physical Plan ==
*(3) Sort [custdist#670L DESC NULLS LAST, c_count#692L DESC NULLS LAST], true, 0
+- *(3) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(custdist#670L DESC NULLS LAST, c_count#692L DESC NULLS LAST, 1), ENSURE_REQUIREMENTS, [id=#3340], [id=#3340], [OUTPUT] List(c_count:LongType, custdist:LongType), [OUTPUT] List(c_count:LongType, custdist:LongType)
         +- *(6) HashAggregateTransformer(keys=[c_count#692L], functions=[count(1)], output=[c_count#692L, custdist#670L])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(c_count#692L, 1), ENSURE_REQUIREMENTS, [id=#3335], [id=#3335], [OUTPUT] ArrayBuffer(c_count:LongType, count:LongType), [OUTPUT] ArrayBuffer(c_count:LongType, count:LongType)
                  +- *(5) HashAggregateTransformer(keys=[c_count#692L], functions=[partial_count(1)], output=[c_count#692L, count#696L])
                     +- *(5) HashAggregateTransformer(keys=[c_custkey#672L], functions=[count(o_orderkey#680L)], output=[c_count#692L])
                        +- RowToColumnar
                           +- *(2) HashAggregate(keys=[c_custkey#672L], functions=[partial_count(o_orderkey#680L)], output=[c_custkey#672L, count#698L])
                              +- *(2) ColumnarToRow
                                 +- *(4) ProjectExecTransformer [c_custkey#672L, o_orderkey#680L]
                                    +- *(4) ShuffledHashJoinExecTransformer [c_custkey#672L], [o_custkey#681L], LeftOuter, BuildRight
                                       :- CoalesceBatches
                                       :  +- ColumnarExchange hashpartitioning(c_custkey#672L, 1), ENSURE_REQUIREMENTS, [id=#3280], [id=#3280], [OUTPUT] List(c_custkey:LongType), [OUTPUT] List(c_custkey:LongType)
                                       :     +- *(1) FileScan parquet default.customer[c_custkey#672L] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<c_custkey:bigint>
                                       +- CoalesceBatches
                                          +- ColumnarExchange hashpartitioning(o_custkey#681L, 1), ENSURE_REQUIREMENTS, [id=#3320], [id=#3320], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType)
                                             +- *(3) ProjectExecTransformer [o_orderkey#680L, o_custkey#681L]
                                                +- RowToColumnar
                                                   +- *(1) Filter ((isnotnull(o_comment#688) AND NOT o_comment#688 LIKE %special%requests%) AND isnotnull(o_custkey#681L))
                                                      +- *(1) ColumnarToRow
                                                         +- *(2) FileScan parquet default.orders[o_orderkey#680L,o_custkey#681L,o_comment#688] Batched: true, DataFilters: [isnotnull(o_comment#688), NOT o_comment#688 LIKE %special%requests%, isnotnull(o_custkey#681L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_comment), IsNotNull(o_custkey)], ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_comment:string>

````

如 MySQL 就不支持如上语法，需要使用如下等价形式。
````mysql

# 创建视图，相当与标准Q13的子查询内容
create view orders_per_cust:s (custkey, ordercount) as
select
  c_custkey, count(o_orderkey)
from customer left outer join orders on 
     c_custkey = o_custkey and o_comment not like '%:1%:2%'
group by c_custkey;


# 对视图进行查询
select
  ordercount, count(*) as custdist 
from orders_per_cust:s 
group by ordercount
order by custdist desc, ordercount desc;

drop view orders_per_cust:s;
````

[返回目录](#目录)

## Q14: 促销效果查询 ✓
* Q14语句查询获得某一个月的收入中有多大的百分比是来自促销零件。用以监视促销带来的市场反应。
* Q14语句的特点是：带有分组、排序、聚集、子查询、左外连接操作并存的查询操作。
* TPC-H标准定义了Q14语句等价的变形SQL，与上述查询语句格式上基本相同，主要是目标列使用了不同的表达方式，在此不再赘述。
````mysql
select
	100.00 * sum(case
		when p_type like 'PROMO%' # 促销零件
			then l_extendedprice * (1 - l_discount) # 某一特定时间的收入
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= '1995-09-01' # DATE是从1993年到1997年中任一年的任一月的一号
	and l_shipdate < date_add('1995-09-01', interval '1' month);
````

````
== Parsed Logical Plan ==
'Project [((100.00 * 'sum(CASE WHEN 'p_type LIKE PROMO% THEN ('l_extendedprice * (1 - 'l_discount)) ELSE 0 END)) / 'sum(('l_extendedprice * (1 - 'l_discount)))) AS promo_revenue#699]
+- 'Filter ((('l_partkey = 'p_partkey) AND ('l_shipdate >= 9374)) AND ('l_shipdate < (9374 + 1 months)))
   +- 'Join Inner
      :- 'UnresolvedRelation [lineitem], [], false
      +- 'UnresolvedRelation [part], [], false

== Analyzed Logical Plan ==
promo_revenue: double
Aggregate [((cast(100.00 as double) * sum(CASE WHEN p_type#720 LIKE PROMO% THEN (l_extendedprice#705 * (cast(1 as double) - l_discount#706)) ELSE cast(0 as double) END)) / sum((l_extendedprice#705 * (cast(1 as double) - l_discount#706)))) AS promo_revenue#699]
+- Filter (((l_partkey#701L = p_partkey#716L) AND (l_shipdate#710 >= 9374)) AND (l_shipdate#710 < 9374 + 1 months))
   +- Join Inner
      :- SubqueryAlias spark_catalog.default.lineitem
      :  +- Relation[l_orderkey#700L,l_partkey#701L,l_suppkey#702L,l_linenumber#703L,l_quantity#704,l_extendedprice#705,l_discount#706,l_tax#707,l_returnflag#708,l_linestatus#709,l_shipdate#710,l_commitdate#711,l_receiptdate#712,l_shipinstruct#713,l_shipmode#714,l_comment#715] parquet
      +- SubqueryAlias spark_catalog.default.part
         +- Relation[p_partkey#716L,p_name#717,p_mfgr#718,p_brand#719,p_type#720,p_size#721L,p_container#722,p_retailprice#723,p_comment#724] parquet

== Optimized Logical Plan ==
Aggregate [((100.0 * sum(CASE WHEN StartsWith(p_type#720, PROMO) THEN (l_extendedprice#705 * (1.0 - l_discount#706)) ELSE 0.0 END)) / sum((l_extendedprice#705 * (1.0 - l_discount#706)))) AS promo_revenue#699]
+- Project [l_extendedprice#705, l_discount#706, p_type#720]
   +- Join Inner, (l_partkey#701L = p_partkey#716L)
      :- Project [l_partkey#701L, l_extendedprice#705, l_discount#706]
      :  +- Filter (((isnotnull(l_shipdate#710) AND (l_shipdate#710 >= 9374)) AND (l_shipdate#710 < 9404)) AND isnotnull(l_partkey#701L))
      :     +- Relation[l_orderkey#700L,l_partkey#701L,l_suppkey#702L,l_linenumber#703L,l_quantity#704,l_extendedprice#705,l_discount#706,l_tax#707,l_returnflag#708,l_linestatus#709,l_shipdate#710,l_commitdate#711,l_receiptdate#712,l_shipinstruct#713,l_shipmode#714,l_comment#715] parquet
      +- Project [p_partkey#716L, p_type#720]
         +- Filter isnotnull(p_partkey#716L)
            +- Relation[p_partkey#716L,p_name#717,p_mfgr#718,p_brand#719,p_type#720,p_size#721L,p_container#722,p_retailprice#723,p_comment#724] parquet

== Physical Plan ==
*(2) ColumnarToRow
+- *(4) HashAggregateTransformer(keys=[], functions=[sum(CASE WHEN StartsWith(p_type#720, PROMO) THEN (l_extendedprice#705 * (1.0 - l_discount#706)) ELSE 0.0 END), sum((l_extendedprice#705 * (1.0 - l_discount#706)))], output=[promo_revenue#699])
   +- RowToColumnar
      +- *(1) HashAggregate(keys=[], functions=[partial_sum(CASE WHEN StartsWith(p_type#720, PROMO) THEN (l_extendedprice#705 * (1.0 - l_discount#706)) ELSE 0.0 END), partial_sum((l_extendedprice#705 * (1.0 - l_discount#706)))], output=[sum#730, sum#731])
         +- *(1) ColumnarToRow
            +- *(3) ProjectExecTransformer [l_extendedprice#705, l_discount#706, p_type#720]
               +- *(3) ShuffledHashJoinExecTransformer [l_partkey#701L], [p_partkey#716L], Inner, BuildRight
                  :- CoalesceBatches
                  :  +- ColumnarExchange hashpartitioning(l_partkey#701L, 1), ENSURE_REQUIREMENTS, [id=#3419], [id=#3419], [OUTPUT] List(l_partkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_partkey:LongType, l_extendedprice:DoubleType, l_discount:DoubleType)
                  :     +- *(1) ProjectExecTransformer [l_partkey#701L, l_extendedprice#705, l_discount#706]
                  :        +- *(1) FilterExecTransformer (((isnotnull(l_shipdate#710) AND (l_shipdate#710 >= 9374)) AND (l_shipdate#710 < 9404)) AND isnotnull(l_partkey#701L))
                  :           +- *(1) FileScan parquet default.lineitem[l_partkey#701L,l_extendedprice#705,l_discount#706,l_shipdate#710] Batched: true, DataFilters: [isnotnull(l_shipdate#710), (l_shipdate#710 >= 9374), (l_shipdate#710 < 9404), isnotnull(l_partke..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1995-09-01), LessThan(l_shipdate,1995-10-01..., ReadSchema: struct<l_partkey:bigint,l_extendedprice:double,l_discount:double,l_shipdate:date>
                  +- CoalesceBatches
                     +- ColumnarExchange hashpartitioning(p_partkey#716L, 1), ENSURE_REQUIREMENTS, [id=#3423], [id=#3423], [OUTPUT] List(p_partkey:LongType, p_type:StringType), [OUTPUT] List(p_partkey:LongType, p_type:StringType)
                        +- *(2) FilterExecTransformer isnotnull(p_partkey#716L)
                           +- *(2) FileScan parquet default.part[p_partkey#716L,p_type#720] Batched: true, DataFilters: [isnotnull(p_partkey#716L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_partkey)], ReadSchema: struct<p_partkey:bigint,p_type:string>

````

[返回目录](#目录)

## Q15: 头等供货商查询
* Q15语句查询获得某段时间内为总收入贡献最多的供货商（排名第一）的信息。可用以决定对哪些头等供货商给予奖励、给予更多订单、给予特别认证、给予鼓舞等激励。
* Q15语句的特点是：带有分排序、聚集、聚集子查询操作并存的普通表与视图的连接操作。
````mysql
create view revenue0 (supplier_no, total_revenue) as # 创建复杂视图（带有分组操作）
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount)) # 获取供货商为公司带来的总利润
	from
		lineitem
	where
		l_shipdate >= '1996-01-01' # DATE 是从1993年一月到1997年十月中任一月的一号
		and l_shipdate < date_add('1996-01-01', interval '3' month) # 3个月内
	group by # 分组键与查询对象之一相同
		l_suppkey;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue0  # 普通表与复杂视图进行连接操作
where
	s_suppkey = supplier_no # 聚集子查询
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0 # 聚集子查询从视图获得数据
	)
order by
	s_suppkey;

drop view revenue0;
````

````
== Parsed Logical Plan ==
'Sort ['s_suppkey ASC NULLS FIRST], true
+- 'Project ['s_suppkey, 's_name, 's_address, 's_phone, 'total_revenue]
   +- 'Filter (('s_suppkey = 'supplier_no) AND ('total_revenue = scalar-subquery#736 []))
      :  +- 'Project [unresolvedalias('max('total_revenue), None)]
      :     +- 'SubqueryAlias revenue1
      :        +- 'Aggregate ['supplier_no], ['l_suppkey AS supplier_no#734, 'sum(('l_extendedprice * (1 - 'l_discount))) AS total_revenue#735]
      :           +- 'Filter (('l_shipdate >= 9496) AND ('l_shipdate < (9496 + 3 months)))
      :              +- 'UnresolvedRelation [lineitem], [], false
      +- 'Join Inner
         :- 'UnresolvedRelation [supplier], [], false
         +- 'SubqueryAlias revenue0
            +- 'Aggregate ['supplier_no], ['l_suppkey AS supplier_no#732, 'sum(('l_extendedprice * (1 - 'l_discount))) AS total_revenue#733]
               +- 'Filter (('l_shipdate >= 9496) AND ('l_shipdate < (9496 + 3 months)))
                  +- 'UnresolvedRelation [lineitem], [], false

== Analyzed Logical Plan ==
s_suppkey: bigint, s_name: string, s_address: string, s_phone: string, total_revenue: double
Sort [s_suppkey#737L ASC NULLS FIRST], true
+- Project [s_suppkey#737L, s_name#738, s_address#739, s_phone#741, total_revenue#733]
   +- Filter ((s_suppkey#737L = supplier_no#732L) AND (total_revenue#733 = scalar-subquery#736 []))
      :  +- Aggregate [max(total_revenue#735) AS max(total_revenue)#779]
      :     +- SubqueryAlias revenue1
      :        +- Aggregate [l_suppkey#763L], [l_suppkey#763L AS supplier_no#734L, sum((l_extendedprice#766 * (cast(1 as double) - l_discount#767))) AS total_revenue#735]
      :           +- Filter ((l_shipdate#771 >= 9496) AND (l_shipdate#771 < 9496 + 3 months))
      :              +- SubqueryAlias spark_catalog.default.lineitem
      :                 +- Relation[l_orderkey#761L,l_partkey#762L,l_suppkey#763L,l_linenumber#764L,l_quantity#765,l_extendedprice#766,l_discount#767,l_tax#768,l_returnflag#769,l_linestatus#770,l_shipdate#771,l_commitdate#772,l_receiptdate#773,l_shipinstruct#774,l_shipmode#775,l_comment#776] parquet
      +- Join Inner
         :- SubqueryAlias spark_catalog.default.supplier
         :  +- Relation[s_suppkey#737L,s_name#738,s_address#739,s_nationkey#740L,s_phone#741,s_acctbal#742,s_comment#743] parquet
         +- SubqueryAlias revenue0
            +- Aggregate [l_suppkey#746L], [l_suppkey#746L AS supplier_no#732L, sum((l_extendedprice#749 * (cast(1 as double) - l_discount#750))) AS total_revenue#733]
               +- Filter ((l_shipdate#754 >= 9496) AND (l_shipdate#754 < 9496 + 3 months))
                  +- SubqueryAlias spark_catalog.default.lineitem
                     +- Relation[l_orderkey#744L,l_partkey#745L,l_suppkey#746L,l_linenumber#747L,l_quantity#748,l_extendedprice#749,l_discount#750,l_tax#751,l_returnflag#752,l_linestatus#753,l_shipdate#754,l_commitdate#755,l_receiptdate#756,l_shipinstruct#757,l_shipmode#758,l_comment#759] parquet

== Optimized Logical Plan ==
Sort [s_suppkey#737L ASC NULLS FIRST], true
+- Project [s_suppkey#737L, s_name#738, s_address#739, s_phone#741, total_revenue#733]
   +- Join Inner, (s_suppkey#737L = supplier_no#732L)
      :- Project [s_suppkey#737L, s_name#738, s_address#739, s_phone#741]
      :  +- Filter isnotnull(s_suppkey#737L)
      :     +- Relation[s_suppkey#737L,s_name#738,s_address#739,s_nationkey#740L,s_phone#741,s_acctbal#742,s_comment#743] parquet
      +- Filter (isnotnull(total_revenue#733) AND (total_revenue#733 = scalar-subquery#736 []))
         :  +- Aggregate [max(total_revenue#735) AS max(total_revenue)#779]
         :     +- Aggregate [l_suppkey#763L], [sum((l_extendedprice#766 * (1.0 - l_discount#767))) AS total_revenue#735]
         :        +- Project [l_suppkey#763L, l_extendedprice#766, l_discount#767]
         :           +- Filter ((isnotnull(l_shipdate#771) AND (l_shipdate#771 >= 9496)) AND (l_shipdate#771 < 9587))
         :              +- Relation[l_orderkey#761L,l_partkey#762L,l_suppkey#763L,l_linenumber#764L,l_quantity#765,l_extendedprice#766,l_discount#767,l_tax#768,l_returnflag#769,l_linestatus#770,l_shipdate#771,l_commitdate#772,l_receiptdate#773,l_shipinstruct#774,l_shipmode#775,l_comment#776] parquet
         +- Aggregate [l_suppkey#746L], [l_suppkey#746L AS supplier_no#732L, sum((l_extendedprice#749 * (1.0 - l_discount#750))) AS total_revenue#733]
            +- Project [l_suppkey#746L, l_extendedprice#749, l_discount#750]
               +- Filter (((isnotnull(l_shipdate#754) AND (l_shipdate#754 >= 9496)) AND (l_shipdate#754 < 9587)) AND isnotnull(l_suppkey#746L))
                  +- Relation[l_orderkey#744L,l_partkey#745L,l_suppkey#746L,l_linenumber#747L,l_quantity#748,l_extendedprice#749,l_discount#750,l_tax#751,l_returnflag#752,l_linestatus#753,l_shipdate#754,l_commitdate#755,l_receiptdate#756,l_shipinstruct#757,l_shipmode#758,l_comment#759] parquet

== Physical Plan ==
*(2) Sort [s_suppkey#737L ASC NULLS FIRST], true, 0
+- *(2) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(s_suppkey#737L ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#3635], [id=#3635], [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_phone:StringType, total_revenue:DoubleType), [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_phone:StringType, total_revenue:DoubleType)
         +- *(4) ProjectExecTransformer [s_suppkey#737L, s_name#738, s_address#739, s_phone#741, total_revenue#733]
            +- *(4) ShuffledHashJoinExecTransformer [s_suppkey#737L], [supplier_no#732L], Inner, BuildLeft
               :- CoalesceBatches
               :  +- ColumnarExchange hashpartitioning(s_suppkey#737L, 1), ENSURE_REQUIREMENTS, [id=#3606], [id=#3606], [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_phone:StringType), [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_phone:StringType)
               :     +- *(1) FilterExecTransformer isnotnull(s_suppkey#737L)
               :        +- *(1) FileScan parquet default.supplier[s_suppkey#737L,s_name#738,s_address#739,s_phone#741] Batched: true, DataFilters: [isnotnull(s_suppkey#737L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey)], ReadSchema: struct<s_suppkey:bigint,s_name:string,s_address:string,s_phone:string>
               +- RowToColumnar
                  +- *(1) Filter (isnotnull(total_revenue#733) AND (total_revenue#733 = Subquery scalar-subquery#736, [id=#3540]))
                     :  +- Subquery scalar-subquery#736, [id=#3540]
                     :     +- *(1) HashAggregate(keys=[], functions=[max(total_revenue#735)], output=[max(total_revenue)#779])
                     :        +- *(1) HashAggregate(keys=[], functions=[partial_max(total_revenue#735)], output=[max#788])
                     :           +- *(1) ColumnarToRow
                     :              +- *(2) HashAggregateTransformer(keys=[l_suppkey#763L], functions=[sum((l_extendedprice#766 * (1.0 - l_discount#767)))], output=[total_revenue#735])
                     :                 +- CoalesceBatches
                     :                    +- ColumnarExchange hashpartitioning(l_suppkey#763L, 1), ENSURE_REQUIREMENTS, [id=#3527], [id=#3527], [OUTPUT] ArrayBuffer(l_suppkey:LongType, sum:DoubleType), [OUTPUT] ArrayBuffer(l_suppkey:LongType, sum:DoubleType)
                     :                       +- *(1) HashAggregateTransformer(keys=[l_suppkey#763L], functions=[partial_sum((l_extendedprice#766 * (1.0 - l_discount#767)))], output=[l_suppkey#763L, sum#790])
                     :                          +- *(1) ProjectExecTransformer [l_suppkey#763L, l_extendedprice#766, l_discount#767]
                     :                             +- *(1) FilterExecTransformer ((isnotnull(l_shipdate#771) AND (l_shipdate#771 >= 9496)) AND (l_shipdate#771 < 9587))
                     :                                +- *(1) FileScan parquet default.lineitem[l_suppkey#763L,l_extendedprice#766,l_discount#767,l_shipdate#771] Batched: true, DataFilters: [isnotnull(l_shipdate#771), (l_shipdate#771 >= 9496), (l_shipdate#771 < 9587)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1996-01-01), LessThan(l_shipdate,1996-04-01)], ReadSchema: struct<l_suppkey:bigint,l_extendedprice:double,l_discount:double,l_shipdate:date>
                     +- *(1) ColumnarToRow
                        +- *(3) HashAggregateTransformer(keys=[l_suppkey#746L], functions=[sum((l_extendedprice#749 * (1.0 - l_discount#750)))], output=[supplier_no#732L, total_revenue#733])
                           +- CoalesceBatches
                              +- ColumnarExchange hashpartitioning(l_suppkey#746L, 1), ENSURE_REQUIREMENTS, [id=#3610], [id=#3610], [OUTPUT] ArrayBuffer(l_suppkey:LongType, sum:DoubleType), [OUTPUT] ArrayBuffer(l_suppkey:LongType, sum:DoubleType)
                                 +- *(2) HashAggregateTransformer(keys=[l_suppkey#746L], functions=[partial_sum((l_extendedprice#749 * (1.0 - l_discount#750)))], output=[l_suppkey#746L, sum#786])
                                    +- *(2) ProjectExecTransformer [l_suppkey#746L, l_extendedprice#749, l_discount#750]
                                       +- *(2) FilterExecTransformer (((isnotnull(l_shipdate#754) AND (l_shipdate#754 >= 9496)) AND (l_shipdate#754 < 9587)) AND isnotnull(l_suppkey#746L))
                                          +- *(2) FileScan parquet default.lineitem[l_suppkey#746L,l_extendedprice#749,l_discount#750,l_shipdate#754] Batched: true, DataFilters: [isnotnull(l_shipdate#754), (l_shipdate#754 >= 9496), (l_shipdate#754 < 9587), isnotnull(l_suppke..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1996-01-01), LessThan(l_shipdate,1996-04-01..., ReadSchema: struct<l_suppkey:bigint,l_extendedprice:double,l_discount:double,l_shipdate:date>

````

* TPC-H标准定义了Q15语句等价的变形SQL，与上述查询语句格式上不相同，上述查询语句首先定义了视图，然后用表与视图连接；变形的SQL定了WITH语句，然后用WITH的对象与表进行连接。变形SQL的语句如下：
````mysql
WITH revenue (supplier_no, total_revenue) as (
SELECT 
  l_suppkey, SUM(l_extendedprice * (1-l_discount))
FROM lineitem
WHERE l_shipdate >= date ':1' AND 
      l_shipdate < date ':1' + interval '3' month
GROUP BY l_suppkey
)

SELECT 
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM supplier, revenue
WHERE s_suppkey = supplier_no
AND total_revenue = ( 
  SELECT 
  MAX(total_revenue)
  FROM revenue 
) ORDER BY s_suppkey;
````
[返回目录](#目录)


## Q16: 零件/供货商关系查询
* Q16 语句查询获得能够以指定的贡献条件供应零件的供货商数量。可用于决定在订单量大，任务紧急时，是否有充足的供货商。
* Q16语句的特点是：带有分组、排序、聚集、去重、NOT IN子查询操作并存的两表连接操作。
````mysql
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt # 聚集、去重操作
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#45' # BRAND＝Brand＃MN ，M和N是两个字母，代表两个数值，相互独立，取值在1到5之间
	and p_type not like 'MEDIUM POLISHED%' # 消费者不感兴趣的类型和尺寸
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9) # TYPEX是在1到50之间任意选择的一组八个不同的值
	and ps_suppkey not in ( # NOT IN子查询，消费者排除某些供货商
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by  # 按数量降序排列，按品牌、种类、尺寸升序排列
	supplier_cnt desc,  
	p_brand,
	p_type,
	p_size;
````

````
== Parsed Logical Plan ==
'Sort ['supplier_cnt DESC NULLS LAST, 'p_brand ASC NULLS FIRST, 'p_type ASC NULLS FIRST, 'p_size ASC NULLS FIRST], true
+- 'Aggregate ['p_brand, 'p_type, 'p_size], ['p_brand, 'p_type, 'p_size, 'count(distinct 'ps_suppkey) AS supplier_cnt#792]
   +- 'Filter (((('p_partkey = 'ps_partkey) AND NOT ('p_brand = Brand#45)) AND NOT 'p_type LIKE MEDIUM POLISHED%) AND ('p_size IN (49,14,23,45,19,3,36,9) AND NOT 'ps_suppkey IN (list#791 [])))
      :  +- 'Project ['s_suppkey]
      :     +- 'Filter 's_comment LIKE %Customer%Complaints%
      :        +- 'UnresolvedRelation [supplier], [], false
      +- 'Join Inner
         :- 'UnresolvedRelation [partsupp], [], false
         +- 'UnresolvedRelation [part], [], false

== Analyzed Logical Plan ==
p_brand: string, p_type: string, p_size: bigint, supplier_cnt: bigint
Sort [supplier_cnt#792L DESC NULLS LAST, p_brand#801 ASC NULLS FIRST, p_type#802 ASC NULLS FIRST, p_size#803L ASC NULLS FIRST], true
+- Aggregate [p_brand#801, p_type#802, p_size#803L], [p_brand#801, p_type#802, p_size#803L, count(distinct ps_suppkey#794L) AS supplier_cnt#792L]
   +- Filter ((((p_partkey#798L = ps_partkey#793L) AND NOT (p_brand#801 = Brand#45)) AND NOT p_type#802 LIKE MEDIUM POLISHED%) AND (cast(p_size#803L as bigint) IN (cast(49 as bigint),cast(14 as bigint),cast(23 as bigint),cast(45 as bigint),cast(19 as bigint),cast(3 as bigint),cast(36 as bigint),cast(9 as bigint)) AND NOT ps_suppkey#794L IN (list#791 [])))
      :  +- Project [s_suppkey#807L]
      :     +- Filter s_comment#813 LIKE %Customer%Complaints%
      :        +- SubqueryAlias spark_catalog.default.supplier
      :           +- Relation[s_suppkey#807L,s_name#808,s_address#809,s_nationkey#810L,s_phone#811,s_acctbal#812,s_comment#813] parquet
      +- Join Inner
         :- SubqueryAlias spark_catalog.default.partsupp
         :  +- Relation[ps_partkey#793L,ps_suppkey#794L,ps_availqty#795L,ps_supplycost#796,ps_comment#797] parquet
         +- SubqueryAlias spark_catalog.default.part
            +- Relation[p_partkey#798L,p_name#799,p_mfgr#800,p_brand#801,p_type#802,p_size#803L,p_container#804,p_retailprice#805,p_comment#806] parquet

== Optimized Logical Plan ==
Sort [supplier_cnt#792L DESC NULLS LAST, p_brand#801 ASC NULLS FIRST, p_type#802 ASC NULLS FIRST, p_size#803L ASC NULLS FIRST], true
+- Aggregate [p_brand#801, p_type#802, p_size#803L], [p_brand#801, p_type#802, p_size#803L, count(distinct ps_suppkey#794L) AS supplier_cnt#792L]
   +- Project [ps_suppkey#794L, p_brand#801, p_type#802, p_size#803L]
      +- Join Inner, (p_partkey#798L = ps_partkey#793L)
         :- Join LeftAnti, ((ps_suppkey#794L = s_suppkey#807L) OR isnull((ps_suppkey#794L = s_suppkey#807L)))
         :  :- Project [ps_partkey#793L, ps_suppkey#794L]
         :  :  +- Filter isnotnull(ps_partkey#793L)
         :  :     +- Relation[ps_partkey#793L,ps_suppkey#794L,ps_availqty#795L,ps_supplycost#796,ps_comment#797] parquet
         :  +- Project [s_suppkey#807L]
         :     +- Filter (isnotnull(s_comment#813) AND s_comment#813 LIKE %Customer%Complaints%)
         :        +- Relation[s_suppkey#807L,s_name#808,s_address#809,s_nationkey#810L,s_phone#811,s_acctbal#812,s_comment#813] parquet
         +- Project [p_partkey#798L, p_brand#801, p_type#802, p_size#803L]
            +- Filter (((((isnotnull(p_brand#801) AND isnotnull(p_type#802)) AND NOT (p_brand#801 = Brand#45)) AND NOT StartsWith(p_type#802, MEDIUM POLISHED)) AND p_size#803L IN (49,14,23,45,19,3,36,9)) AND isnotnull(p_partkey#798L))
               +- Relation[p_partkey#798L,p_name#799,p_mfgr#800,p_brand#801,p_type#802,p_size#803L,p_container#804,p_retailprice#805,p_comment#806] parquet

== Physical Plan ==
*(5) Sort [supplier_cnt#792L DESC NULLS LAST, p_brand#801 ASC NULLS FIRST, p_type#802 ASC NULLS FIRST, p_size#803L ASC NULLS FIRST], true, 0
+- *(5) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(supplier_cnt#792L DESC NULLS LAST, p_brand#801 ASC NULLS FIRST, p_type#802 ASC NULLS FIRST, p_size#803L ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#3885], [id=#3885], [OUTPUT] List(p_brand:StringType, p_type:StringType, p_size:LongType, supplier_cnt:LongType), [OUTPUT] List(p_brand:StringType, p_type:StringType, p_size:LongType, supplier_cnt:LongType)
         +- *(6) HashAggregateTransformer(keys=[p_brand#801, p_type#802, p_size#803L], functions=[count(distinct ps_suppkey#794L)], output=[p_brand#801, p_type#802, p_size#803L, supplier_cnt#792L])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(p_brand#801, p_type#802, p_size#803L, 1), ENSURE_REQUIREMENTS, [id=#3880], [id=#3880], [OUTPUT] ArrayBuffer(p_brand:StringType, p_type:StringType, p_size:LongType, count:LongType), [OUTPUT] ArrayBuffer(p_brand:StringType, p_type:StringType, p_size:LongType, count:LongType)
                  +- RowToColumnar
                     +- *(4) HashAggregate(keys=[p_brand#801, p_type#802, p_size#803L], functions=[partial_count(distinct ps_suppkey#794L)], output=[p_brand#801, p_type#802, p_size#803L, count#825L])
                        +- *(4) ColumnarToRow
                           +- *(5) HashAggregateTransformer(keys=[p_brand#801, p_type#802, p_size#803L, ps_suppkey#794L], functions=[], output=[p_brand#801, p_type#802, p_size#803L, ps_suppkey#794L])
                              +- CoalesceBatches
                                 +- ColumnarExchange hashpartitioning(p_brand#801, p_type#802, p_size#803L, ps_suppkey#794L, 1), ENSURE_REQUIREMENTS, [id=#3870], [id=#3870], [OUTPUT] ArrayBuffer(p_brand:StringType, p_type:StringType, p_size:LongType, ps_suppkey:LongType), [OUTPUT] ArrayBuffer(p_brand:StringType, p_type:StringType, p_size:LongType, ps_suppkey:LongType)
                                    +- *(4) HashAggregateTransformer(keys=[p_brand#801, p_type#802, p_size#803L, ps_suppkey#794L], functions=[], output=[p_brand#801, p_type#802, p_size#803L, ps_suppkey#794L])
                                       +- *(4) ProjectExecTransformer [ps_suppkey#794L, p_brand#801, p_type#802, p_size#803L]
                                          +- *(4) ShuffledHashJoinExecTransformer [ps_partkey#793L], [p_partkey#798L], Inner, BuildRight
                                             :- CoalesceBatches
                                             :  +- ColumnarExchange hashpartitioning(ps_partkey#793L, 1), ENSURE_REQUIREMENTS, [id=#3855], [id=#3855], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType)
                                             :     +- RowToColumnar
                                             :        +- *(2) BroadcastHashJoin [ps_suppkey#794L], [s_suppkey#807L], LeftAnti, BuildRight, true
                                             :           :- *(2) ColumnarToRow
                                             :           :  +- *(1) FilterExecTransformer isnotnull(ps_partkey#793L)
                                             :           :     +- *(1) FileScan parquet default.partsupp[ps_partkey#793L,ps_suppkey#794L] Batched: true, DataFilters: [isnotnull(ps_partkey#793L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_partkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint>
                                             :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]),true), [id=#3850]
                                             :              +- *(1) ColumnarToRow
                                             :                 +- *(2) ProjectExecTransformer [s_suppkey#807L]
                                             :                    +- *(2) FilterExecTransformer (isnotnull(s_comment#813) AND s_comment#813 LIKE %Customer%Complaints%)
                                             :                       +- *(2) FileScan parquet default.supplier[s_suppkey#807L,s_comment#813] Batched: true, DataFilters: [isnotnull(s_comment#813), s_comment#813 LIKE %Customer%Complaints%], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_comment)], ReadSchema: struct<s_suppkey:bigint,s_comment:string>
                                             +- CoalesceBatches
                                                +- ColumnarExchange hashpartitioning(p_partkey#798L, 1), ENSURE_REQUIREMENTS, [id=#3863], [id=#3863], [OUTPUT] List(p_partkey:LongType, p_brand:StringType, p_type:StringType, p_size:LongType), [OUTPUT] List(p_partkey:LongType, p_brand:StringType, p_type:StringType, p_size:LongType)
                                                   +- RowToColumnar
                                                      +- *(3) Filter (((((isnotnull(p_brand#801) AND isnotnull(p_type#802)) AND NOT (p_brand#801 = Brand#45)) AND NOT StartsWith(p_type#802, MEDIUM POLISHED)) AND p_size#803L IN (49,14,23,45,19,3,36,9)) AND isnotnull(p_partkey#798L))
                                                         +- *(3) ColumnarToRow
                                                            +- *(3) FileScan parquet default.part[p_partkey#798L,p_brand#801,p_type#802,p_size#803L] Batched: true, DataFilters: [isnotnull(p_brand#801), isnotnull(p_type#802), NOT (p_brand#801 = Brand#45), NOT StartsWith(p_ty..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_brand), IsNotNull(p_type), Not(EqualTo(p_brand,Brand#45)), Not(StringStartsWith(p_ty..., ReadSchema: struct<p_partkey:bigint,p_brand:string,p_type:string,p_size:bigint>

````

[返回目录](#目录)

此查询找出有多少供应商可以提供具有给定属性的零件。 例如，它可用于确定是否有足够数量的供应商来订购大量订购的零件。

## Q17: 小订单收入查询

- Q17 语句确定：如果不再订购某些小批量的零件，平均将损失多少年收入。这样，可以将销售集中在更大的出货量上的商品来减少管理费用。
- **业务含义**：该查询考虑给定品牌和给定包装类型的零件，并确定数据库中过去 7 年所有订单中此类零件的平均订购数。如果丢失那些**零件订购数少于平均数 20%** 的订单，那么年平均（未贴现）总收入损失是多少？

* Q17 语句的特点是：带有聚集、聚集子查询操作并存的两表连接操作。
````mysql
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#23' # 指定品牌。 BRAND＝’Brand#MN’ ，M和N是两个字母，代表两个数值，相互独立，取值在1到5之间
	and p_container = 'MED BOX' # 指定包装类型。在TPC-H标准指定的范围内随机选择
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);
````

````
== Parsed Logical Plan ==
'Project [('sum('l_extendedprice) / 7.0) AS avg_yearly#828]
+- 'Filter ((('p_partkey = 'l_partkey) AND ('p_brand = Brand#23)) AND (('p_container = MED BOX) AND ('l_quantity < scalar-subquery#827 [])))
   :  +- 'Project [unresolvedalias((0.2 * 'avg('l_quantity)), None)]
   :     +- 'Filter ('l_partkey = 'p_partkey)
   :        +- 'UnresolvedRelation [lineitem], [], false
   +- 'Join Inner
      :- 'UnresolvedRelation [lineitem], [], false
      +- 'UnresolvedRelation [part], [], false

== Analyzed Logical Plan ==
avg_yearly: double
Aggregate [(sum(l_extendedprice#834) / cast(7.0 as double)) AS avg_yearly#828]
+- Filter (((p_partkey#845L = l_partkey#830L) AND (p_brand#848 = Brand#23)) AND ((p_container#851 = MED BOX) AND (l_quantity#833 < scalar-subquery#827 [p_partkey#845L])))
   :  +- Aggregate [(cast(0.2 as double) * avg(l_quantity#858)) AS (CAST(0.2 AS DOUBLE) * avg(l_quantity))#871]
   :     +- Filter (l_partkey#855L = outer(p_partkey#845L))
   :        +- SubqueryAlias spark_catalog.default.lineitem
   :           +- Relation[l_orderkey#854L,l_partkey#855L,l_suppkey#856L,l_linenumber#857L,l_quantity#858,l_extendedprice#859,l_discount#860,l_tax#861,l_returnflag#862,l_linestatus#863,l_shipdate#864,l_commitdate#865,l_receiptdate#866,l_shipinstruct#867,l_shipmode#868,l_comment#869] parquet
   +- Join Inner
      :- SubqueryAlias spark_catalog.default.lineitem
      :  +- Relation[l_orderkey#829L,l_partkey#830L,l_suppkey#831L,l_linenumber#832L,l_quantity#833,l_extendedprice#834,l_discount#835,l_tax#836,l_returnflag#837,l_linestatus#838,l_shipdate#839,l_commitdate#840,l_receiptdate#841,l_shipinstruct#842,l_shipmode#843,l_comment#844] parquet
      +- SubqueryAlias spark_catalog.default.part
         +- Relation[p_partkey#845L,p_name#846,p_mfgr#847,p_brand#848,p_type#849,p_size#850L,p_container#851,p_retailprice#852,p_comment#853] parquet

== Optimized Logical Plan ==
Aggregate [(sum(l_extendedprice#834) / 7.0) AS avg_yearly#828]
+- Project [l_extendedprice#834]
   +- Join Inner, ((l_quantity#833 < (CAST(0.2 AS DOUBLE) * avg(l_quantity))#871) AND (l_partkey#855L = p_partkey#845L))
      :- Project [l_quantity#833, l_extendedprice#834, p_partkey#845L]
      :  +- Join Inner, (p_partkey#845L = l_partkey#830L)
      :     :- Project [l_partkey#830L, l_quantity#833, l_extendedprice#834]
      :     :  +- Filter (isnotnull(l_partkey#830L) AND isnotnull(l_quantity#833))
      :     :     +- Relation[l_orderkey#829L,l_partkey#830L,l_suppkey#831L,l_linenumber#832L,l_quantity#833,l_extendedprice#834,l_discount#835,l_tax#836,l_returnflag#837,l_linestatus#838,l_shipdate#839,l_commitdate#840,l_receiptdate#841,l_shipinstruct#842,l_shipmode#843,l_comment#844] parquet
      :     +- Project [p_partkey#845L]
      :        +- Filter ((((isnotnull(p_brand#848) AND isnotnull(p_container#851)) AND (p_brand#848 = Brand#23)) AND (p_container#851 = MED BOX)) AND isnotnull(p_partkey#845L))
      :           +- Relation[p_partkey#845L,p_name#846,p_mfgr#847,p_brand#848,p_type#849,p_size#850L,p_container#851,p_retailprice#852,p_comment#853] parquet
      +- Filter isnotnull((CAST(0.2 AS DOUBLE) * avg(l_quantity))#871)
         +- Aggregate [l_partkey#855L], [(0.2 * avg(l_quantity#858)) AS (CAST(0.2 AS DOUBLE) * avg(l_quantity))#871, l_partkey#855L]
            +- Project [l_partkey#855L, l_quantity#858]
               +- Filter isnotnull(l_partkey#855L)
                  +- Relation[l_orderkey#854L,l_partkey#855L,l_suppkey#856L,l_linenumber#857L,l_quantity#858,l_extendedprice#859,l_discount#860,l_tax#861,l_returnflag#862,l_linestatus#863,l_shipdate#864,l_commitdate#865,l_receiptdate#866,l_shipinstruct#867,l_shipmode#868,l_comment#869] parquet

== Physical Plan ==
*(3) ColumnarToRow
+- *(5) HashAggregateTransformer(keys=[], functions=[sum(l_extendedprice#834)], output=[avg_yearly#828])
   +- RowToColumnar
      +- *(2) HashAggregate(keys=[], functions=[partial_sum(l_extendedprice#834)], output=[sum#875])
         +- *(2) ColumnarToRow
            +- *(4) ProjectExecTransformer [l_extendedprice#834]
               +- *(4) ShuffledHashJoinExecTransformer [p_partkey#845L], [l_partkey#855L], Inner, BuildRight, (l_quantity#833 < (CAST(0.2 AS DOUBLE) * avg(l_quantity))#871)
                  :- *(4) ProjectExecTransformer [l_quantity#833, l_extendedprice#834, p_partkey#845L]
                  :  +- *(4) ShuffledHashJoinExecTransformer [l_partkey#830L], [p_partkey#845L], Inner, BuildRight
                  :     :- CoalesceBatches
                  :     :  +- ColumnarExchange hashpartitioning(l_partkey#830L, 1), ENSURE_REQUIREMENTS, [id=#4035], [id=#4035], [OUTPUT] List(l_partkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType), [OUTPUT] List(l_partkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType)
                  :     :     +- *(1) FilterExecTransformer (isnotnull(l_partkey#830L) AND isnotnull(l_quantity#833))
                  :     :        +- *(1) FileScan parquet default.lineitem[l_partkey#830L,l_quantity#833,l_extendedprice#834] Batched: true, DataFilters: [isnotnull(l_partkey#830L), isnotnull(l_quantity#833)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_partkey), IsNotNull(l_quantity)], ReadSchema: struct<l_partkey:bigint,l_quantity:double,l_extendedprice:double>
                  :     +- CoalesceBatches
                  :        +- ColumnarExchange hashpartitioning(p_partkey#845L, 1), ENSURE_REQUIREMENTS, [id=#4039], [id=#4039], [OUTPUT] List(p_partkey:LongType), [OUTPUT] List(p_partkey:LongType)
                  :           +- *(2) ProjectExecTransformer [p_partkey#845L]
                  :              +- *(2) FilterExecTransformer ((((isnotnull(p_brand#848) AND isnotnull(p_container#851)) AND (p_brand#848 = Brand#23)) AND (p_container#851 = MED BOX)) AND isnotnull(p_partkey#845L))
                  :                 +- *(2) FileScan parquet default.part[p_partkey#845L,p_brand#848,p_container#851] Batched: true, DataFilters: [isnotnull(p_brand#848), isnotnull(p_container#851), (p_brand#848 = Brand#23), (p_container#851 =..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_brand), IsNotNull(p_container), EqualTo(p_brand,Brand#23), EqualTo(p_container,MED B..., ReadSchema: struct<p_partkey:bigint,p_brand:string,p_container:string>
                  +- *(4) FilterExecTransformer isnotnull((CAST(0.2 AS DOUBLE) * avg(l_quantity))#871)
                     +- *(4) HashAggregateTransformer(keys=[l_partkey#855L], functions=[avg(l_quantity#858)], output=[(CAST(0.2 AS DOUBLE) * avg(l_quantity))#871, l_partkey#855L])
                        +- CoalesceBatches
                           +- ColumnarExchange hashpartitioning(l_partkey#855L, 1), ENSURE_REQUIREMENTS, [id=#4068], [id=#4068], [OUTPUT] List(l_partkey:LongType, sum:DoubleType, count:LongType), [OUTPUT] List(l_partkey:LongType, sum:DoubleType, count:LongType)
                              +- RowToColumnar
                                 +- *(1) HashAggregate(keys=[l_partkey#855L], functions=[partial_avg(l_quantity#858)], output=[l_partkey#855L, sum#878, count#879L])
                                    +- *(1) ColumnarToRow
                                       +- *(3) FilterExecTransformer isnotnull(l_partkey#855L)
                                          +- *(3) FileScan parquet default.lineitem[l_partkey#855L,l_quantity#858] Batched: true, DataFilters: [isnotnull(l_partkey#855L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_partkey)], ReadSchema: struct<l_partkey:bigint,l_quantity:double>

````

[返回目录](#目录)

## Q18: 大订单顾客查询
* Q18 语句根据客户的下单量对客户进行排名，**大订单**是指总数量在一定量级以上的订单。
* **业务含义**：**查找曾经下过大订单的前 100 位客户的列表**。查询列出客户名称、客户关键字、订单关键字、日期和总价以及订单数量。
* Q18语句的特点是：带有分组、排序、聚集、IN子查询操作并存的三表连接操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。
````mysql
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity) # 订货总数
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 300 # QUANTITY是位于312到315之间的任意值
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;
````

````
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['o_totalprice DESC NULLS LAST, 'o_orderdate ASC NULLS FIRST], true
      +- 'Aggregate ['c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice], ['c_name, 'c_custkey, 'o_orderkey, 'o_orderdate, 'o_totalprice, unresolvedalias('sum('l_quantity), None)]
         +- 'Filter (('o_orderkey IN (list#880 []) AND ('c_custkey = 'o_custkey)) AND ('o_orderkey = 'l_orderkey))
            :  +- 'UnresolvedHaving ('sum('l_quantity) > 300)
            :     +- 'Aggregate ['l_orderkey], ['l_orderkey]
            :        +- 'UnresolvedRelation [lineitem], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'UnresolvedRelation [customer], [], false
               :  +- 'UnresolvedRelation [orders], [], false
               +- 'UnresolvedRelation [lineitem], [], false

== Analyzed Logical Plan ==
c_name: string, c_custkey: bigint, o_orderkey: bigint, o_orderdate: date, o_totalprice: double, sum(l_quantity): double
GlobalLimit 100
+- LocalLimit 100
   +- Project [c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892, sum(l_quantity)#935]
      +- Sort [o_totalprice#892 DESC NULLS LAST, o_orderdate#893 ASC NULLS FIRST], true
         +- Aggregate [c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892], [c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892, sum(l_quantity#902) AS sum(l_quantity)#935]
            +- Filter ((o_orderkey#889L IN (list#880 []) AND (c_custkey#881L = o_custkey#890L)) AND (o_orderkey#889L = l_orderkey#898L))
               :  +- Project [l_orderkey#914L]
               :     +- Filter (sum(l_quantity#918)#932 > cast(300 as double))
               :        +- Aggregate [l_orderkey#914L], [l_orderkey#914L, sum(l_quantity#918) AS sum(l_quantity#918)#932]
               :           +- SubqueryAlias spark_catalog.default.lineitem
               :              +- Relation[l_orderkey#914L,l_partkey#915L,l_suppkey#916L,l_linenumber#917L,l_quantity#918,l_extendedprice#919,l_discount#920,l_tax#921,l_returnflag#922,l_linestatus#923,l_shipdate#924,l_commitdate#925,l_receiptdate#926,l_shipinstruct#927,l_shipmode#928,l_comment#929] parquet
               +- Join Inner
                  :- Join Inner
                  :  :- SubqueryAlias spark_catalog.default.customer
                  :  :  +- Relation[c_custkey#881L,c_name#882,c_address#883,c_nationkey#884L,c_phone#885,c_acctbal#886,c_mktsegment#887,c_comment#888] parquet
                  :  +- SubqueryAlias spark_catalog.default.orders
                  :     +- Relation[o_orderkey#889L,o_custkey#890L,o_orderstatus#891,o_totalprice#892,o_orderdate#893,o_orderpriority#894,o_clerk#895,o_shippriority#896L,o_comment#897] parquet
                  +- SubqueryAlias spark_catalog.default.lineitem
                     +- Relation[l_orderkey#898L,l_partkey#899L,l_suppkey#900L,l_linenumber#901L,l_quantity#902,l_extendedprice#903,l_discount#904,l_tax#905,l_returnflag#906,l_linestatus#907,l_shipdate#908,l_commitdate#909,l_receiptdate#910,l_shipinstruct#911,l_shipmode#912,l_comment#913] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [o_totalprice#892 DESC NULLS LAST, o_orderdate#893 ASC NULLS FIRST], true
      +- Aggregate [c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892], [c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892, sum(l_quantity#902) AS sum(l_quantity)#935]
         +- Project [c_custkey#881L, c_name#882, o_orderkey#889L, o_totalprice#892, o_orderdate#893, l_quantity#902]
            +- Join Inner, (o_orderkey#889L = l_orderkey#898L)
               :- Project [c_custkey#881L, c_name#882, o_orderkey#889L, o_totalprice#892, o_orderdate#893]
               :  +- Join Inner, (c_custkey#881L = o_custkey#890L)
               :     :- Project [c_custkey#881L, c_name#882]
               :     :  +- Filter isnotnull(c_custkey#881L)
               :     :     +- Relation[c_custkey#881L,c_name#882,c_address#883,c_nationkey#884L,c_phone#885,c_acctbal#886,c_mktsegment#887,c_comment#888] parquet
               :     +- Join LeftSemi, (o_orderkey#889L = l_orderkey#914L)
               :        :- Project [o_orderkey#889L, o_custkey#890L, o_totalprice#892, o_orderdate#893]
               :        :  +- Filter (isnotnull(o_custkey#890L) AND isnotnull(o_orderkey#889L))
               :        :     +- Relation[o_orderkey#889L,o_custkey#890L,o_orderstatus#891,o_totalprice#892,o_orderdate#893,o_orderpriority#894,o_clerk#895,o_shippriority#896L,o_comment#897] parquet
               :        +- Project [l_orderkey#914L]
               :           +- Filter (isnotnull(sum(l_quantity#918)#932) AND (sum(l_quantity#918)#932 > 300.0))
               :              +- Aggregate [l_orderkey#914L], [l_orderkey#914L, sum(l_quantity#918) AS sum(l_quantity#918)#932]
               :                 +- Project [l_orderkey#914L, l_quantity#918]
               :                    +- Relation[l_orderkey#914L,l_partkey#915L,l_suppkey#916L,l_linenumber#917L,l_quantity#918,l_extendedprice#919,l_discount#920,l_tax#921,l_returnflag#922,l_linestatus#923,l_shipdate#924,l_commitdate#925,l_receiptdate#926,l_shipinstruct#927,l_shipmode#928,l_comment#929] parquet
               +- Join LeftSemi, (l_orderkey#898L = l_orderkey#914L)
                  :- Project [l_orderkey#898L, l_quantity#902]
                  :  +- Filter isnotnull(l_orderkey#898L)
                  :     +- Relation[l_orderkey#898L,l_partkey#899L,l_suppkey#900L,l_linenumber#901L,l_quantity#902,l_extendedprice#903,l_discount#904,l_tax#905,l_returnflag#906,l_linestatus#907,l_shipdate#908,l_commitdate#909,l_receiptdate#910,l_shipinstruct#911,l_shipmode#912,l_comment#913] parquet
                  +- Project [l_orderkey#914L]
                     +- Filter (isnotnull(sum(l_quantity#918)#932) AND (sum(l_quantity#918)#932 > 300.0))
                        +- Aggregate [l_orderkey#914L], [l_orderkey#914L, sum(l_quantity#918) AS sum(l_quantity#918)#932]
                           +- Project [l_orderkey#914L, l_quantity#918]
                              +- Relation[l_orderkey#914L,l_partkey#915L,l_suppkey#916L,l_linenumber#917L,l_quantity#918,l_extendedprice#919,l_discount#920,l_tax#921,l_returnflag#922,l_linestatus#923,l_shipdate#924,l_commitdate#925,l_receiptdate#926,l_shipinstruct#927,l_shipmode#928,l_comment#929] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[o_totalprice#892 DESC NULLS LAST,o_orderdate#893 ASC NULLS FIRST], output=[c_name#882,c_custkey#881L,o_orderkey#889L,o_orderdate#893,o_totalprice#892,sum(l_quantity)#935])
+- *(3) ColumnarToRow
   +- *(8) HashAggregateTransformer(keys=[c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892], functions=[sum(l_quantity#902)], output=[c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892, sum(l_quantity)#935])
      +- *(8) HashAggregateTransformer(keys=[c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, knownfloatingpointnormalized(normalizenanandzero(o_totalprice#892)) AS o_totalprice#892], functions=[partial_sum(l_quantity#902)], output=[c_name#882, c_custkey#881L, o_orderkey#889L, o_orderdate#893, o_totalprice#892, sum#945])
         +- *(8) ProjectExecTransformer [c_custkey#881L, c_name#882, o_orderkey#889L, o_totalprice#892, o_orderdate#893, l_quantity#902]
            +- *(8) ShuffledHashJoinExecTransformer [o_orderkey#889L], [l_orderkey#898L], Inner, BuildRight
               :- CoalesceBatches
               :  +- ColumnarExchange hashpartitioning(o_orderkey#889L, 1), ENSURE_REQUIREMENTS, [id=#4412], [id=#4412], [OUTPUT] List(c_custkey:LongType, c_name:StringType, o_orderkey:LongType, o_totalprice:DoubleType, o_orderdate:DateType), [OUTPUT] List(c_custkey:LongType, c_name:StringType, o_orderkey:LongType, o_totalprice:DoubleType, o_orderdate:DateType)
               :     +- *(5) ProjectExecTransformer [c_custkey#881L, c_name#882, o_orderkey#889L, o_totalprice#892, o_orderdate#893]
               :        +- *(5) ShuffledHashJoinExecTransformer [c_custkey#881L], [o_custkey#890L], Inner, BuildLeft
               :           :- CoalesceBatches
               :           :  +- ColumnarExchange hashpartitioning(c_custkey#881L, 1), ENSURE_REQUIREMENTS, [id=#4343], [id=#4343], [OUTPUT] List(c_custkey:LongType, c_name:StringType), [OUTPUT] List(c_custkey:LongType, c_name:StringType)
               :           :     +- *(1) FilterExecTransformer isnotnull(c_custkey#881L)
               :           :        +- *(1) FileScan parquet default.customer[c_custkey#881L,c_name#882] Batched: true, DataFilters: [isnotnull(c_custkey#881L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_custkey)], ReadSchema: struct<c_custkey:bigint,c_name:string>
               :           +- CoalesceBatches
               :              +- ColumnarExchange hashpartitioning(o_custkey#890L, 1), ENSURE_REQUIREMENTS, [id=#4406], [id=#4406], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_totalprice:DoubleType, o_orderdate:DateType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_totalprice:DoubleType, o_orderdate:DateType)
               :                 +- *(4) ShuffledHashJoinExecTransformer [o_orderkey#889L], [l_orderkey#914L], LeftSemi, BuildRight
               :                    :- CoalesceBatches
               :                    :  +- ColumnarExchange hashpartitioning(o_orderkey#889L, 1), ENSURE_REQUIREMENTS, [id=#4347], [id=#4347], [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_totalprice:DoubleType, o_orderdate:DateType), [OUTPUT] List(o_orderkey:LongType, o_custkey:LongType, o_totalprice:DoubleType, o_orderdate:DateType)
               :                    :     +- *(2) FilterExecTransformer (isnotnull(o_custkey#890L) AND isnotnull(o_orderkey#889L))
               :                    :        +- *(2) FileScan parquet default.orders[o_orderkey#889L,o_custkey#890L,o_totalprice#892,o_orderdate#893] Batched: true, DataFilters: [isnotnull(o_custkey#890L), isnotnull(o_orderkey#889L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_custkey), IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_custkey:bigint,o_totalprice:double,o_orderdate:date>
               :                    +- *(4) ProjectExecTransformer [l_orderkey#914L]
               :                       +- *(4) FilterExecTransformer (isnotnull(sum(l_quantity#918)#932) AND (sum(l_quantity#918)#932 > 300.0))
               :                          +- *(4) HashAggregateTransformer(keys=[l_orderkey#914L], functions=[sum(l_quantity#918)], output=[l_orderkey#914L, sum(l_quantity#918)#932])
               :                             +- CoalesceBatches
               :                                +- ColumnarExchange hashpartitioning(l_orderkey#914L, 1), ENSURE_REQUIREMENTS, [id=#4398], [id=#4398], [OUTPUT] ArrayBuffer(l_orderkey:LongType, sum:DoubleType), [OUTPUT] ArrayBuffer(l_orderkey:LongType, sum:DoubleType)
               :                                   +- RowToColumnar
               :                                      +- *(1) HashAggregate(keys=[l_orderkey#914L], functions=[partial_sum(l_quantity#918)], output=[l_orderkey#914L, sum#947])
               :                                         +- *(1) ColumnarToRow
               :                                            +- *(3) FileScan parquet default.lineitem[l_orderkey#914L,l_quantity#918] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<l_orderkey:bigint,l_quantity:double>
               +- *(8) ShuffledHashJoinExecTransformer [l_orderkey#898L], [l_orderkey#914L], LeftSemi, BuildRight
                  :- CoalesceBatches
                  :  +- ColumnarExchange hashpartitioning(l_orderkey#898L, 1), ENSURE_REQUIREMENTS, [id=#4372], [id=#4372], [OUTPUT] List(l_orderkey:LongType, l_quantity:DoubleType), [OUTPUT] List(l_orderkey:LongType, l_quantity:DoubleType)
                  :     +- *(6) FilterExecTransformer isnotnull(l_orderkey#898L)
                  :        +- *(6) FileScan parquet default.lineitem[l_orderkey#898L,l_quantity#902] Batched: true, DataFilters: [isnotnull(l_orderkey#898L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_quantity:double>
                  +- *(8) ProjectExecTransformer [l_orderkey#914L]
                     +- *(8) FilterExecTransformer (isnotnull(sum(l_quantity#918)#932) AND (sum(l_quantity#918)#932 > 300.0))
                        +- *(8) HashAggregateTransformer(keys=[l_orderkey#914L], functions=[sum(l_quantity#918)], output=[l_orderkey#914L, sum(l_quantity#918)#932])
                           +- CoalesceBatches
                              +- ReusedExchange [l_orderkey#914L, sum#947], ColumnarExchange hashpartitioning(l_orderkey#914L, 1), ENSURE_REQUIREMENTS, [id=#4398], [id=#4398], [OUTPUT] ArrayBuffer(l_orderkey:LongType, sum:DoubleType)

````

[返回目录](#目录)

## Q19: 折扣收入查询 ✓

- Q19 语句查询计算**以特定方式处理的**零件的**总折扣销售收入**。此查询演示由数据挖掘工具以编程方式生成的 SQL。
- **业务含义**：查找三种不同类型零件的所有订单的总折扣收入，这些零件通过空运和亲自交付。 根据特定品牌、包装列表和一系列尺寸的组合选择零件。

* Q19 语句的特点是：带有分组、排序、聚集、IN子查询操作并存的三表连接操作 

The Potential Part Promotion Query identifies suppliers in a particular nation having selected parts that may be candidates for a promotional offer. 

Business Question The Potential Part Promotion query identifies suppliers who have an excess of a given part available; an excess is defined to be more than 50% of the parts like the given part that the supplier shipped in a given year for a given nation. Only parts whose names share a certain naming convention are considered.

````mysql
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#12' # 特定品牌。BRAND1、BRAND2、BRAND3＝‘Brand＃MN’，M和N是两个字母，代表两个数值，相互独立，取值在1到5之间 
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') # 包装范围
		and l_quantity >= 1 and l_quantity <= 1 + 10 # QUANTITY1 是1到10之间的任意取值
		and p_size between 1 and 5 # 尺寸范围
		and l_shipmode in ('AIR', 'AIR REG') # 运输模式，如下带有阴影的粗体表示的条件是相同的，存在条件化简的可能
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#23'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 10 and l_quantity <= 10 + 10 # QUANTITY2 是10到20之间的任意取值 
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#34'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 20 and l_quantity <= 20 + 10 # QUANTITY3 是20到30之间的任意取值
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
````

````
== Parsed Logical Plan ==
'Project ['sum(('l_extendedprice * (1 - 'l_discount))) AS revenue#952]
+- 'Filter (((((('p_partkey = 'l_partkey) AND ('p_brand = Brand#12)) AND ('p_container IN (SM CASE,SM BOX,SM PACK,SM PKG) AND ('l_quantity >= 1))) AND ((('l_quantity <= (1 + 10)) AND (('p_size >= 1) AND ('p_size <= 5))) AND ('l_shipmode IN (AIR,AIR REG) AND ('l_shipinstruct = DELIVER IN PERSON)))) OR (((('p_partkey = 'l_partkey) AND ('p_brand = Brand#23)) AND ('p_container IN (MED BAG,MED BOX,MED PKG,MED PACK) AND ('l_quantity >= 10))) AND ((('l_quantity <= (10 + 10)) AND (('p_size >= 1) AND ('p_size <= 10))) AND ('l_shipmode IN (AIR,AIR REG) AND ('l_shipinstruct = DELIVER IN PERSON))))) OR (((('p_partkey = 'l_partkey) AND ('p_brand = Brand#34)) AND ('p_container IN (LG CASE,LG BOX,LG PACK,LG PKG) AND ('l_quantity >= 20))) AND ((('l_quantity <= (20 + 10)) AND (('p_size >= 1) AND ('p_size <= 15))) AND ('l_shipmode IN (AIR,AIR REG) AND ('l_shipinstruct = DELIVER IN PERSON)))))
   +- 'Join Inner
      :- 'UnresolvedRelation [lineitem], [], false
      +- 'UnresolvedRelation [part], [], false

== Analyzed Logical Plan ==
revenue: double
Aggregate [sum((l_extendedprice#958 * (cast(1 as double) - l_discount#959))) AS revenue#952]
+- Filter ((((((p_partkey#969L = l_partkey#954L) AND (p_brand#972 = Brand#12)) AND (p_container#975 IN (SM CASE,SM BOX,SM PACK,SM PKG) AND (l_quantity#957 >= cast(1 as double)))) AND (((l_quantity#957 <= cast((1 + 10) as double)) AND ((p_size#974L >= cast(1 as bigint)) AND (p_size#974L <= cast(5 as bigint)))) AND (l_shipmode#967 IN (AIR,AIR REG) AND (l_shipinstruct#966 = DELIVER IN PERSON)))) OR ((((p_partkey#969L = l_partkey#954L) AND (p_brand#972 = Brand#23)) AND (p_container#975 IN (MED BAG,MED BOX,MED PKG,MED PACK) AND (l_quantity#957 >= cast(10 as double)))) AND (((l_quantity#957 <= cast((10 + 10) as double)) AND ((p_size#974L >= cast(1 as bigint)) AND (p_size#974L <= cast(10 as bigint)))) AND (l_shipmode#967 IN (AIR,AIR REG) AND (l_shipinstruct#966 = DELIVER IN PERSON))))) OR ((((p_partkey#969L = l_partkey#954L) AND (p_brand#972 = Brand#34)) AND (p_container#975 IN (LG CASE,LG BOX,LG PACK,LG PKG) AND (l_quantity#957 >= cast(20 as double)))) AND (((l_quantity#957 <= cast((20 + 10) as double)) AND ((p_size#974L >= cast(1 as bigint)) AND (p_size#974L <= cast(15 as bigint)))) AND (l_shipmode#967 IN (AIR,AIR REG) AND (l_shipinstruct#966 = DELIVER IN PERSON)))))
   +- Join Inner
      :- SubqueryAlias spark_catalog.default.lineitem
      :  +- Relation[l_orderkey#953L,l_partkey#954L,l_suppkey#955L,l_linenumber#956L,l_quantity#957,l_extendedprice#958,l_discount#959,l_tax#960,l_returnflag#961,l_linestatus#962,l_shipdate#963,l_commitdate#964,l_receiptdate#965,l_shipinstruct#966,l_shipmode#967,l_comment#968] parquet
      +- SubqueryAlias spark_catalog.default.part
         +- Relation[p_partkey#969L,p_name#970,p_mfgr#971,p_brand#972,p_type#973,p_size#974L,p_container#975,p_retailprice#976,p_comment#977] parquet

== Optimized Logical Plan ==
Aggregate [sum((l_extendedprice#958 * (1.0 - l_discount#959))) AS revenue#952]
+- Project [l_extendedprice#958, l_discount#959]
   +- Join Inner, ((p_partkey#969L = l_partkey#954L) AND (((((((p_brand#972 = Brand#12) AND p_container#975 IN (SM CASE,SM BOX,SM PACK,SM PKG)) AND (l_quantity#957 >= 1.0)) AND (l_quantity#957 <= 11.0)) AND (p_size#974L <= 5)) OR (((((p_brand#972 = Brand#23) AND p_container#975 IN (MED BAG,MED BOX,MED PKG,MED PACK)) AND (l_quantity#957 >= 10.0)) AND (l_quantity#957 <= 20.0)) AND (p_size#974L <= 10))) OR (((((p_brand#972 = Brand#34) AND p_container#975 IN (LG CASE,LG BOX,LG PACK,LG PKG)) AND (l_quantity#957 >= 20.0)) AND (l_quantity#957 <= 30.0)) AND (p_size#974L <= 15))))
      :- Project [l_partkey#954L, l_quantity#957, l_extendedprice#958, l_discount#959]
      :  +- Filter ((((isnotnull(l_shipinstruct#966) AND l_shipmode#967 IN (AIR,AIR REG)) AND (l_shipinstruct#966 = DELIVER IN PERSON)) AND isnotnull(l_partkey#954L)) AND ((((l_quantity#957 >= 1.0) AND (l_quantity#957 <= 11.0)) OR ((l_quantity#957 >= 10.0) AND (l_quantity#957 <= 20.0))) OR ((l_quantity#957 >= 20.0) AND (l_quantity#957 <= 30.0))))
      :     +- Relation[l_orderkey#953L,l_partkey#954L,l_suppkey#955L,l_linenumber#956L,l_quantity#957,l_extendedprice#958,l_discount#959,l_tax#960,l_returnflag#961,l_linestatus#962,l_shipdate#963,l_commitdate#964,l_receiptdate#965,l_shipinstruct#966,l_shipmode#967,l_comment#968] parquet
      +- Project [p_partkey#969L, p_brand#972, p_size#974L, p_container#975]
         +- Filter (((isnotnull(p_size#974L) AND (p_size#974L >= 1)) AND isnotnull(p_partkey#969L)) AND (((((p_brand#972 = Brand#12) AND p_container#975 IN (SM CASE,SM BOX,SM PACK,SM PKG)) AND (p_size#974L <= 5)) OR (((p_brand#972 = Brand#23) AND p_container#975 IN (MED BAG,MED BOX,MED PKG,MED PACK)) AND (p_size#974L <= 10))) OR (((p_brand#972 = Brand#34) AND p_container#975 IN (LG CASE,LG BOX,LG PACK,LG PKG)) AND (p_size#974L <= 15))))
            +- Relation[p_partkey#969L,p_name#970,p_mfgr#971,p_brand#972,p_type#973,p_size#974L,p_container#975,p_retailprice#976,p_comment#977] parquet

== Physical Plan ==
*(6) ColumnarToRow
+- *(4) HashAggregateTransformer(keys=[], functions=[sum((l_extendedprice#958 * (1.0 - l_discount#959)))], output=[revenue#952])
   +- *(4) HashAggregateTransformer(keys=[], functions=[partial_sum((l_extendedprice#958 * (1.0 - l_discount#959)))], output=[sum#981])
      +- *(4) ProjectExecTransformer [l_extendedprice#958, l_discount#959]
         +- RowToColumnar
            +- *(5) ShuffledHashJoin [l_partkey#954L], [p_partkey#969L], Inner, BuildRight, (((((((p_brand#972 = Brand#12) AND p_container#975 IN (SM CASE,SM BOX,SM PACK,SM PKG)) AND (l_quantity#957 >= 1.0)) AND (l_quantity#957 <= 11.0)) AND (p_size#974L <= 5)) OR (((((p_brand#972 = Brand#23) AND p_container#975 IN (MED BAG,MED BOX,MED PKG,MED PACK)) AND (l_quantity#957 >= 10.0)) AND (l_quantity#957 <= 20.0)) AND (p_size#974L <= 10))) OR (((((p_brand#972 = Brand#34) AND p_container#975 IN (LG CASE,LG BOX,LG PACK,LG PKG)) AND (l_quantity#957 >= 20.0)) AND (l_quantity#957 <= 30.0)) AND (p_size#974L <= 15)))
               :- *(2) ColumnarToRow
               :  +- CoalesceBatches
               :     +- ColumnarExchange hashpartitioning(l_partkey#954L, 1), ENSURE_REQUIREMENTS, [id=#4620], [id=#4620], [OUTPUT] List(l_partkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType), [OUTPUT] List(l_partkey:LongType, l_quantity:DoubleType, l_extendedprice:DoubleType, l_discount:DoubleType)
               :        +- *(2) ProjectExecTransformer [l_partkey#954L, l_quantity#957, l_extendedprice#958, l_discount#959]
               :           +- RowToColumnar
               :              +- *(1) Filter ((((isnotnull(l_shipinstruct#966) AND l_shipmode#967 IN (AIR,AIR REG)) AND (l_shipinstruct#966 = DELIVER IN PERSON)) AND isnotnull(l_partkey#954L)) AND ((((l_quantity#957 >= 1.0) AND (l_quantity#957 <= 11.0)) OR ((l_quantity#957 >= 10.0) AND (l_quantity#957 <= 20.0))) OR ((l_quantity#957 >= 20.0) AND (l_quantity#957 <= 30.0))))
               :                 +- *(1) ColumnarToRow
               :                    +- *(1) FileScan parquet default.lineitem[l_partkey#954L,l_quantity#957,l_extendedprice#958,l_discount#959,l_shipinstruct#966,l_shipmode#967] Batched: true, DataFilters: [isnotnull(l_shipinstruct#966), l_shipmode#967 IN (AIR,AIR REG), (l_shipinstruct#966 = DELIVER IN..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipinstruct), In(l_shipmode, [AIR,AIR REG]), EqualTo(l_shipinstruct,DELIVER IN PERS..., ReadSchema: struct<l_partkey:bigint,l_quantity:double,l_extendedprice:double,l_discount:double,l_shipinstruct...
               +- *(4) ColumnarToRow
                  +- CoalesceBatches
                     +- ColumnarExchange hashpartitioning(p_partkey#969L, 1), ENSURE_REQUIREMENTS, [id=#4631], [id=#4631], [OUTPUT] List(p_partkey:LongType, p_brand:StringType, p_size:LongType, p_container:StringType), [OUTPUT] List(p_partkey:LongType, p_brand:StringType, p_size:LongType, p_container:StringType)
                        +- RowToColumnar
                           +- *(3) Filter (((isnotnull(p_size#974L) AND (p_size#974L >= 1)) AND isnotnull(p_partkey#969L)) AND (((((p_brand#972 = Brand#12) AND p_container#975 IN (SM CASE,SM BOX,SM PACK,SM PKG)) AND (p_size#974L <= 5)) OR (((p_brand#972 = Brand#23) AND p_container#975 IN (MED BAG,MED BOX,MED PKG,MED PACK)) AND (p_size#974L <= 10))) OR (((p_brand#972 = Brand#34) AND p_container#975 IN (LG CASE,LG BOX,LG PACK,LG PKG)) AND (p_size#974L <= 15))))
                              +- *(3) ColumnarToRow
                                 +- *(3) FileScan parquet default.part[p_partkey#969L,p_brand#972,p_size#974L,p_container#975] Batched: true, DataFilters: [isnotnull(p_size#974L), (p_size#974L >= 1), isnotnull(p_partkey#969L), (((((p_brand#972 = Brand#..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_size), GreaterThanOrEqual(p_size,1), IsNotNull(p_partkey), Or(Or(And(And(EqualTo(p_b..., ReadSchema: struct<p_partkey:bigint,p_brand:string,p_size:bigint,p_container:string>

````

[返回目录](#目录)

## Q20: 供货商竞争力查询
* Q20 语句查询确定在某一年内，找出指定国家的能对某一零件商品提供更有竞争力价格的供货商。所谓更有竞争力的供货商，是指那些零件有过剩的供货商，超过供或商在某一年中货运给定国的某一零件的50％则为过剩。
* Q20 语句的特点是：带有排序、聚集、IN子查询、普通子查询操作并存的两表连接操作。

找出指定国家里==能促销零部件==的供应商，所谓能促销，是指那些零件有过剩的供货商，所谓过剩，是指对于给定的年份，该供应商的供货量超过给定零部件总供货量的 50%。

````mysql
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in ( # 第一层的IN子查询
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in ( # 第二层嵌套的IN子查询
				select
					p_partkey
				from
					part
				where
					p_name like 'forest%' # COLOR为产生P_NAME的值的列表中的任意值
			)
			and ps_availqty > ( # 第二层嵌套的子查询
				select
					0.5 * sum(l_quantity) # 聚集子查询
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= '1994-01-01' # DATE为在1993年至1997年的任一年的一月一号
					and l_shipdate < date_add('1994-01-01', interval '1' year) # 1年内
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA' # TPC-H标准定义的任意值
order by
	s_name;
````

````
== Parsed Logical Plan ==
'Sort ['s_name ASC NULLS FIRST], true
+- 'Project ['s_name, 's_address]
   +- 'Filter (('s_suppkey IN (list#984 []) AND ('s_nationkey = 'n_nationkey)) AND ('n_name = CANADA))
      :  +- 'Project ['ps_suppkey]
      :     +- 'Filter ('ps_partkey IN (list#982 []) AND ('ps_availqty > scalar-subquery#983 []))
      :        :  :- 'Project ['p_partkey]
      :        :  :  +- 'Filter 'p_name LIKE forest%
      :        :  :     +- 'UnresolvedRelation [part], [], false
      :        :  +- 'Project [unresolvedalias((0.5 * 'sum('l_quantity)), None)]
      :        :     +- 'Filter ((('l_partkey = 'ps_partkey) AND ('l_suppkey = 'ps_suppkey)) AND (('l_shipdate >= 8766) AND ('l_shipdate < (8766 + 1 years))))
      :        :        +- 'UnresolvedRelation [lineitem], [], false
      :        +- 'UnresolvedRelation [partsupp], [], false
      +- 'Join Inner
         :- 'UnresolvedRelation [supplier], [], false
         +- 'UnresolvedRelation [nation], [], false

== Analyzed Logical Plan ==
s_name: string, s_address: string
Sort [s_name#986 ASC NULLS FIRST], true
+- Project [s_name#986, s_address#987]
   +- Filter ((s_suppkey#985L IN (list#984 []) AND (s_nationkey#988L = n_nationkey#992L)) AND (n_name#993 = CANADA))
      :  +- Project [ps_suppkey#997L]
      :     +- Filter (ps_partkey#996L IN (list#982 []) AND (cast(ps_availqty#998L as double) > scalar-subquery#983 [ps_partkey#996L && ps_suppkey#997L]))
      :        :  :- Project [p_partkey#1001L]
      :        :  :  +- Filter p_name#1002 LIKE forest%
      :        :  :     +- SubqueryAlias spark_catalog.default.part
      :        :  :        +- Relation[p_partkey#1001L,p_name#1002,p_mfgr#1003,p_brand#1004,p_type#1005,p_size#1006L,p_container#1007,p_retailprice#1008,p_comment#1009] parquet
      :        :  +- Aggregate [(cast(0.5 as double) * sum(l_quantity#1014)) AS (CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027]
      :        :     +- Filter (((l_partkey#1011L = outer(ps_partkey#996L)) AND (l_suppkey#1012L = outer(ps_suppkey#997L))) AND ((l_shipdate#1020 >= 8766) AND (l_shipdate#1020 < 8766 + 1 years)))
      :        :        +- SubqueryAlias spark_catalog.default.lineitem
      :        :           +- Relation[l_orderkey#1010L,l_partkey#1011L,l_suppkey#1012L,l_linenumber#1013L,l_quantity#1014,l_extendedprice#1015,l_discount#1016,l_tax#1017,l_returnflag#1018,l_linestatus#1019,l_shipdate#1020,l_commitdate#1021,l_receiptdate#1022,l_shipinstruct#1023,l_shipmode#1024,l_comment#1025] parquet
      :        +- SubqueryAlias spark_catalog.default.partsupp
      :           +- Relation[ps_partkey#996L,ps_suppkey#997L,ps_availqty#998L,ps_supplycost#999,ps_comment#1000] parquet
      +- Join Inner
         :- SubqueryAlias spark_catalog.default.supplier
         :  +- Relation[s_suppkey#985L,s_name#986,s_address#987,s_nationkey#988L,s_phone#989,s_acctbal#990,s_comment#991] parquet
         +- SubqueryAlias spark_catalog.default.nation
            +- Relation[n_nationkey#992L,n_name#993,n_regionkey#994L,n_comment#995] parquet

== Optimized Logical Plan ==
Sort [s_name#986 ASC NULLS FIRST], true
+- Project [s_name#986, s_address#987]
   +- Join Inner, (s_nationkey#988L = n_nationkey#992L)
      :- Project [s_name#986, s_address#987, s_nationkey#988L]
      :  +- Join LeftSemi, (s_suppkey#985L = ps_suppkey#997L)
      :     :- Project [s_suppkey#985L, s_name#986, s_address#987, s_nationkey#988L]
      :     :  +- Filter isnotnull(s_nationkey#988L)
      :     :     +- Relation[s_suppkey#985L,s_name#986,s_address#987,s_nationkey#988L,s_phone#989,s_acctbal#990,s_comment#991] parquet
      :     +- Project [ps_suppkey#997L]
      :        +- Join Inner, (((cast(ps_availqty#998L as double) > (CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027) AND (l_partkey#1011L = ps_partkey#996L)) AND (l_suppkey#1012L = ps_suppkey#997L))
      :           :- Join LeftSemi, (ps_partkey#996L = p_partkey#1001L)
      :           :  :- Project [ps_partkey#996L, ps_suppkey#997L, ps_availqty#998L]
      :           :  :  +- Filter ((isnotnull(ps_availqty#998L) AND isnotnull(ps_partkey#996L)) AND isnotnull(ps_suppkey#997L))
      :           :  :     +- Relation[ps_partkey#996L,ps_suppkey#997L,ps_availqty#998L,ps_supplycost#999,ps_comment#1000] parquet
      :           :  +- Project [p_partkey#1001L]
      :           :     +- Filter (isnotnull(p_name#1002) AND StartsWith(p_name#1002, forest))
      :           :        +- Relation[p_partkey#1001L,p_name#1002,p_mfgr#1003,p_brand#1004,p_type#1005,p_size#1006L,p_container#1007,p_retailprice#1008,p_comment#1009] parquet
      :           +- Filter isnotnull((CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027)
      :              +- Aggregate [l_partkey#1011L, l_suppkey#1012L], [(0.5 * sum(l_quantity#1014)) AS (CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027, l_partkey#1011L, l_suppkey#1012L]
      :                 +- Join LeftSemi, (l_partkey#1011L = p_partkey#1001L)
      :                    :- Project [l_partkey#1011L, l_suppkey#1012L, l_quantity#1014]
      :                    :  +- Filter ((((isnotnull(l_shipdate#1020) AND (l_shipdate#1020 >= 8766)) AND (l_shipdate#1020 < 9131)) AND isnotnull(l_partkey#1011L)) AND isnotnull(l_suppkey#1012L))
      :                    :     +- Relation[l_orderkey#1010L,l_partkey#1011L,l_suppkey#1012L,l_linenumber#1013L,l_quantity#1014,l_extendedprice#1015,l_discount#1016,l_tax#1017,l_returnflag#1018,l_linestatus#1019,l_shipdate#1020,l_commitdate#1021,l_receiptdate#1022,l_shipinstruct#1023,l_shipmode#1024,l_comment#1025] parquet
      :                    +- Project [p_partkey#1001L]
      :                       +- Filter (isnotnull(p_name#1002) AND StartsWith(p_name#1002, forest))
      :                          +- Relation[p_partkey#1001L,p_name#1002,p_mfgr#1003,p_brand#1004,p_type#1005,p_size#1006L,p_container#1007,p_retailprice#1008,p_comment#1009] parquet
      +- Project [n_nationkey#992L]
         +- Filter ((isnotnull(n_name#993) AND (n_name#993 = CANADA)) AND isnotnull(n_nationkey#992L))
            +- Relation[n_nationkey#992L,n_name#993,n_regionkey#994L,n_comment#995] parquet

== Physical Plan ==
*(4) Sort [s_name#986 ASC NULLS FIRST], true, 0
+- *(4) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(s_name#986 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#5142], [id=#5142], [OUTPUT] List(s_name:StringType, s_address:StringType), [OUTPUT] List(s_name:StringType, s_address:StringType)
         +- *(14) ProjectExecTransformer [s_name#986, s_address#987]
            +- *(14) ShuffledHashJoinExecTransformer [s_nationkey#988L], [n_nationkey#992L], Inner, BuildRight
               :- CoalesceBatches
               :  +- ColumnarExchange hashpartitioning(s_nationkey#988L, 1), ENSURE_REQUIREMENTS, [id=#5136], [id=#5136], [OUTPUT] List(s_name:StringType, s_address:StringType, s_nationkey:LongType), [OUTPUT] List(s_name:StringType, s_address:StringType, s_nationkey:LongType)
               :     +- *(12) ProjectExecTransformer [s_name#986, s_address#987, s_nationkey#988L]
               :        +- *(12) ShuffledHashJoinExecTransformer [s_suppkey#985L], [ps_suppkey#997L], LeftSemi, BuildRight
               :           :- CoalesceBatches
               :           :  +- ColumnarExchange hashpartitioning(s_suppkey#985L, 1), ENSURE_REQUIREMENTS, [id=#4934], [id=#4934], [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_address:StringType, s_nationkey:LongType)
               :           :     +- *(1) FilterExecTransformer isnotnull(s_nationkey#988L)
               :           :        +- *(1) FileScan parquet default.supplier[s_suppkey#985L,s_name#986,s_address#987,s_nationkey#988L] Batched: true, DataFilters: [isnotnull(s_nationkey#988L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_name:string,s_address:string,s_nationkey:bigint>
               :           +- CoalesceBatches
               :              +- ColumnarExchange hashpartitioning(ps_suppkey#997L, 1), ENSURE_REQUIREMENTS, [id=#5130], [id=#5130], [OUTPUT] List(ps_suppkey:LongType), [OUTPUT] List(ps_suppkey:LongType)
               :                 +- *(11) ProjectExecTransformer [ps_suppkey#997L]
               :                    +- *(11) ShuffledHashJoinExecTransformer [ps_partkey#996L, ps_suppkey#997L], [l_partkey#1011L, l_suppkey#1012L], Inner, BuildLeft, (cast(ps_availqty#998L as double) > (CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027)
               :                       :- CoalesceBatches
               :                       :  +- ColumnarExchange hashpartitioning(ps_partkey#996L, ps_suppkey#997L, 1), ENSURE_REQUIREMENTS, [id=#5018], [id=#5018], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_availqty:LongType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_availqty:LongType)
               :                       :     +- *(5) ShuffledHashJoinExecTransformer [ps_partkey#996L], [p_partkey#1001L], LeftSemi, BuildRight
               :                       :        :- CoalesceBatches
               :                       :        :  +- ColumnarExchange hashpartitioning(ps_partkey#996L, 1), ENSURE_REQUIREMENTS, [id=#4938], [id=#4938], [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_availqty:LongType), [OUTPUT] List(ps_partkey:LongType, ps_suppkey:LongType, ps_availqty:LongType)
               :                       :        :     +- *(2) FilterExecTransformer ((isnotnull(ps_availqty#998L) AND isnotnull(ps_partkey#996L)) AND isnotnull(ps_suppkey#997L))
               :                       :        :        +- *(2) FileScan parquet default.partsupp[ps_partkey#996L,ps_suppkey#997L,ps_availqty#998L] Batched: true, DataFilters: [isnotnull(ps_availqty#998L), isnotnull(ps_partkey#996L), isnotnull(ps_suppkey#997L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/partsupp], PartitionFilters: [], PushedFilters: [IsNotNull(ps_availqty), IsNotNull(ps_partkey), IsNotNull(ps_suppkey)], ReadSchema: struct<ps_partkey:bigint,ps_suppkey:bigint,ps_availqty:bigint>
               :                       :        +- CoalesceBatches
               :                       :           +- ColumnarExchange hashpartitioning(p_partkey#1001L, 1), ENSURE_REQUIREMENTS, [id=#5013], [id=#5013], [OUTPUT] List(p_partkey:LongType), [OUTPUT] List(p_partkey:LongType)
               :                       :              +- *(4) ProjectExecTransformer [p_partkey#1001L]
               :                       :                 +- RowToColumnar
               :                       :                    +- *(1) Filter (isnotnull(p_name#1002) AND StartsWith(p_name#1002, forest))
               :                       :                       +- *(1) ColumnarToRow
               :                       :                          +- *(3) FileScan parquet default.part[p_partkey#1001L,p_name#1002] Batched: true, DataFilters: [isnotnull(p_name#1002), StartsWith(p_name#1002, forest)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/part], PartitionFilters: [], PushedFilters: [IsNotNull(p_name), StringStartsWith(p_name,forest)], ReadSchema: struct<p_partkey:bigint,p_name:string>
               :                       +- CoalesceBatches
               :                          +- ColumnarExchange hashpartitioning(l_partkey#1011L, l_suppkey#1012L, 1), ENSURE_REQUIREMENTS, [id=#5124], [id=#5124], [OUTPUT] List((CAST(0.5 AS DOUBLE) * sum(l_quantity)):DoubleType, l_partkey:LongType, l_suppkey:LongType), [OUTPUT] List((CAST(0.5 AS DOUBLE) * sum(l_quantity)):DoubleType, l_partkey:LongType, l_suppkey:LongType)
               :                             +- *(10) FilterExecTransformer isnotnull((CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027)
               :                                +- *(10) HashAggregateTransformer(keys=[l_partkey#1011L, l_suppkey#1012L], functions=[sum(l_quantity#1014)], output=[(CAST(0.5 AS DOUBLE) * sum(l_quantity))#1027, l_partkey#1011L, l_suppkey#1012L])
               :                                   +- RowToColumnar
               :                                      +- *(3) HashAggregate(keys=[l_partkey#1011L, l_suppkey#1012L], functions=[partial_sum(l_quantity#1014)], output=[l_partkey#1011L, l_suppkey#1012L, sum#1031])
               :                                         +- *(3) ColumnarToRow
               :                                            +- *(9) ShuffledHashJoinExecTransformer [l_partkey#1011L], [p_partkey#1001L], LeftSemi, BuildRight
               :                                               :- CoalesceBatches
               :                                               :  +- ColumnarExchange hashpartitioning(l_partkey#1011L, 1), ENSURE_REQUIREMENTS, [id=#4957], [id=#4957], [OUTPUT] List(l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType), [OUTPUT] List(l_partkey:LongType, l_suppkey:LongType, l_quantity:DoubleType)
               :                                               :     +- *(6) ProjectExecTransformer [l_partkey#1011L, l_suppkey#1012L, l_quantity#1014]
               :                                               :        +- *(6) FilterExecTransformer ((((isnotnull(l_shipdate#1020) AND (l_shipdate#1020 >= 8766)) AND (l_shipdate#1020 < 9131)) AND isnotnull(l_partkey#1011L)) AND isnotnull(l_suppkey#1012L))
               :                                               :           +- *(6) FileScan parquet default.lineitem[l_partkey#1011L,l_suppkey#1012L,l_quantity#1014,l_shipdate#1020] Batched: true, DataFilters: [isnotnull(l_shipdate#1020), (l_shipdate#1020 >= 8766), (l_shipdate#1020 < 9131), isnotnull(l_par..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1994-01-01), LessThan(l_shipdate,1995-01-01..., ReadSchema: struct<l_partkey:bigint,l_suppkey:bigint,l_quantity:double,l_shipdate:date>
               :                                               +- CoalesceBatches
               :                                                  +- ReusedExchange [p_partkey#1001L], ColumnarExchange hashpartitioning(p_partkey#1001L, 1), ENSURE_REQUIREMENTS, [id=#5013], [id=#5013], [OUTPUT] List(p_partkey:LongType)
               +- CoalesceBatches
                  +- ColumnarExchange hashpartitioning(n_nationkey#992L, 1), ENSURE_REQUIREMENTS, [id=#4995], [id=#4995], [OUTPUT] List(n_nationkey:LongType), [OUTPUT] List(n_nationkey:LongType)
                     +- *(13) ProjectExecTransformer [n_nationkey#992L]
                        +- *(13) FilterExecTransformer ((isnotnull(n_name#993) AND (n_name#993 = CANADA)) AND isnotnull(n_nationkey#992L))
                           +- *(13) FileScan parquet default.nation[n_nationkey#992L,n_name#993] Batched: true, DataFilters: [isnotnull(n_name#993), (n_name#993 = CANADA), isnotnull(n_nationkey#992L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_name), EqualTo(n_name,CANADA), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>

````

[返回目录](#目录)

## Q21: 不能按时交货的供货商查询
* Q21语句查询获得不能按时交货的供货商。对于**当前状态为“F”的==多供货商订单==**，找到==唯一==未能满足承诺交货日期的供应商。
* Q21语句的特点是：带有分组、排序、聚集、EXISTS子查询、NOT EXISTS子查询操作并存的四表连接操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。
````mysql
select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate # 未能满足承诺交货日期的商品
	and exists ( # EXISTS子查询
    -- 属于多供货商订单的商品
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (# NOT EXISTS子查询
    -- 多供货商订单里，未能满足承诺交货日期的商品，来之于同一个供货商
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'SAUDI ARABIA' # TPC-H标准定义的任意值
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;
````

````
== Parsed Logical Plan ==
'GlobalLimit 100
+- 'LocalLimit 100
   +- 'Sort ['numwait DESC NULLS LAST, 's_name ASC NULLS FIRST], true
      +- 'Aggregate ['s_name], ['s_name, 'count(1) AS numwait#1034]
         +- 'Filter (((('s_suppkey = 'l1.l_suppkey) AND ('o_orderkey = 'l1.l_orderkey)) AND (('o_orderstatus = F) AND ('l1.l_receiptdate > 'l1.l_commitdate))) AND ((exists#1032 [] AND NOT exists#1033 []) AND (('s_nationkey = 'n_nationkey) AND ('n_name = SAUDI ARABIA))))
            :  :- 'Project [*]
            :  :  +- 'Filter (('l2.l_orderkey = 'l1.l_orderkey) AND NOT ('l2.l_suppkey = 'l1.l_suppkey))
            :  :     +- 'SubqueryAlias l2
            :  :        +- 'UnresolvedRelation [lineitem], [], false
            :  +- 'Project [*]
            :     +- 'Filter ((('l3.l_orderkey = 'l1.l_orderkey) AND NOT ('l3.l_suppkey = 'l1.l_suppkey)) AND ('l3.l_receiptdate > 'l3.l_commitdate))
            :        +- 'SubqueryAlias l3
            :           +- 'UnresolvedRelation [lineitem], [], false
            +- 'Join Inner
               :- 'Join Inner
               :  :- 'Join Inner
               :  :  :- 'UnresolvedRelation [supplier], [], false
               :  :  +- 'SubqueryAlias l1
               :  :     +- 'UnresolvedRelation [lineitem], [], false
               :  +- 'UnresolvedRelation [orders], [], false
               +- 'UnresolvedRelation [nation], [], false

== Analyzed Logical Plan ==
s_name: string, numwait: bigint
GlobalLimit 100
+- LocalLimit 100
   +- Sort [numwait#1034L DESC NULLS LAST, s_name#1037 ASC NULLS FIRST], true
      +- Aggregate [s_name#1037], [s_name#1037, count(1) AS numwait#1034L]
         +- Filter ((((s_suppkey#1036L = l_suppkey#1045L) AND (o_orderkey#1059L = l_orderkey#1043L)) AND ((o_orderstatus#1061 = F) AND (l_receiptdate#1055 > l_commitdate#1054))) AND ((exists#1032 [l_orderkey#1043L && l_suppkey#1045L] AND NOT exists#1033 [l_orderkey#1043L && l_suppkey#1045L]) AND ((s_nationkey#1039L = n_nationkey#1068L) AND (n_name#1069 = SAUDI ARABIA))))
            :  :- Project [l_orderkey#1072L, l_partkey#1073L, l_suppkey#1074L, l_linenumber#1075L, l_quantity#1076, l_extendedprice#1077, l_discount#1078, l_tax#1079, l_returnflag#1080, l_linestatus#1081, l_shipdate#1082, l_commitdate#1083, l_receiptdate#1084, l_shipinstruct#1085, l_shipmode#1086, l_comment#1087]
            :  :  +- Filter ((l_orderkey#1072L = outer(l_orderkey#1043L)) AND NOT (l_suppkey#1074L = outer(l_suppkey#1045L)))
            :  :     +- SubqueryAlias l2
            :  :        +- SubqueryAlias spark_catalog.default.lineitem
            :  :           +- Relation[l_orderkey#1072L,l_partkey#1073L,l_suppkey#1074L,l_linenumber#1075L,l_quantity#1076,l_extendedprice#1077,l_discount#1078,l_tax#1079,l_returnflag#1080,l_linestatus#1081,l_shipdate#1082,l_commitdate#1083,l_receiptdate#1084,l_shipinstruct#1085,l_shipmode#1086,l_comment#1087] parquet
            :  +- Project [l_orderkey#1088L, l_partkey#1089L, l_suppkey#1090L, l_linenumber#1091L, l_quantity#1092, l_extendedprice#1093, l_discount#1094, l_tax#1095, l_returnflag#1096, l_linestatus#1097, l_shipdate#1098, l_commitdate#1099, l_receiptdate#1100, l_shipinstruct#1101, l_shipmode#1102, l_comment#1103]
            :     +- Filter (((l_orderkey#1088L = outer(l_orderkey#1043L)) AND NOT (l_suppkey#1090L = outer(l_suppkey#1045L))) AND (l_receiptdate#1100 > l_commitdate#1099))
            :        +- SubqueryAlias l3
            :           +- SubqueryAlias spark_catalog.default.lineitem
            :              +- Relation[l_orderkey#1088L,l_partkey#1089L,l_suppkey#1090L,l_linenumber#1091L,l_quantity#1092,l_extendedprice#1093,l_discount#1094,l_tax#1095,l_returnflag#1096,l_linestatus#1097,l_shipdate#1098,l_commitdate#1099,l_receiptdate#1100,l_shipinstruct#1101,l_shipmode#1102,l_comment#1103] parquet
            +- Join Inner
               :- Join Inner
               :  :- Join Inner
               :  :  :- SubqueryAlias spark_catalog.default.supplier
               :  :  :  +- Relation[s_suppkey#1036L,s_name#1037,s_address#1038,s_nationkey#1039L,s_phone#1040,s_acctbal#1041,s_comment#1042] parquet
               :  :  +- SubqueryAlias l1
               :  :     +- SubqueryAlias spark_catalog.default.lineitem
               :  :        +- Relation[l_orderkey#1043L,l_partkey#1044L,l_suppkey#1045L,l_linenumber#1046L,l_quantity#1047,l_extendedprice#1048,l_discount#1049,l_tax#1050,l_returnflag#1051,l_linestatus#1052,l_shipdate#1053,l_commitdate#1054,l_receiptdate#1055,l_shipinstruct#1056,l_shipmode#1057,l_comment#1058] parquet
               :  +- SubqueryAlias spark_catalog.default.orders
               :     +- Relation[o_orderkey#1059L,o_custkey#1060L,o_orderstatus#1061,o_totalprice#1062,o_orderdate#1063,o_orderpriority#1064,o_clerk#1065,o_shippriority#1066L,o_comment#1067] parquet
               +- SubqueryAlias spark_catalog.default.nation
                  +- Relation[n_nationkey#1068L,n_name#1069,n_regionkey#1070L,n_comment#1071] parquet

== Optimized Logical Plan ==
GlobalLimit 100
+- LocalLimit 100
   +- Sort [numwait#1034L DESC NULLS LAST, s_name#1037 ASC NULLS FIRST], true
      +- Aggregate [s_name#1037], [s_name#1037, count(1) AS numwait#1034L]
         +- Project [s_name#1037]
            +- Join Inner, (s_nationkey#1039L = n_nationkey#1068L)
               :- Project [s_name#1037, s_nationkey#1039L]
               :  +- Join Inner, (o_orderkey#1059L = l_orderkey#1043L)
               :     :- Project [s_name#1037, s_nationkey#1039L, l_orderkey#1043L]
               :     :  +- Join Inner, (s_suppkey#1036L = l_suppkey#1045L)
               :     :     :- Project [s_suppkey#1036L, s_name#1037, s_nationkey#1039L]
               :     :     :  +- Filter (isnotnull(s_suppkey#1036L) AND isnotnull(s_nationkey#1039L))
               :     :     :     +- Relation[s_suppkey#1036L,s_name#1037,s_address#1038,s_nationkey#1039L,s_phone#1040,s_acctbal#1041,s_comment#1042] parquet
               :     :     +- Join LeftAnti, ((l_orderkey#1088L = l_orderkey#1043L) AND NOT (l_suppkey#1090L = l_suppkey#1045L))
               :     :        :- Join LeftSemi, ((l_orderkey#1072L = l_orderkey#1043L) AND NOT (l_suppkey#1074L = l_suppkey#1045L))
               :     :        :  :- Project [l_orderkey#1043L, l_suppkey#1045L]
               :     :        :  :  +- Filter ((((isnotnull(l_receiptdate#1055) AND isnotnull(l_commitdate#1054)) AND (l_receiptdate#1055 > l_commitdate#1054)) AND isnotnull(l_suppkey#1045L)) AND isnotnull(l_orderkey#1043L))
               :     :        :  :     +- Relation[l_orderkey#1043L,l_partkey#1044L,l_suppkey#1045L,l_linenumber#1046L,l_quantity#1047,l_extendedprice#1048,l_discount#1049,l_tax#1050,l_returnflag#1051,l_linestatus#1052,l_shipdate#1053,l_commitdate#1054,l_receiptdate#1055,l_shipinstruct#1056,l_shipmode#1057,l_comment#1058] parquet
               :     :        :  +- Project [l_orderkey#1072L, l_suppkey#1074L]
               :     :        :     +- Relation[l_orderkey#1072L,l_partkey#1073L,l_suppkey#1074L,l_linenumber#1075L,l_quantity#1076,l_extendedprice#1077,l_discount#1078,l_tax#1079,l_returnflag#1080,l_linestatus#1081,l_shipdate#1082,l_commitdate#1083,l_receiptdate#1084,l_shipinstruct#1085,l_shipmode#1086,l_comment#1087] parquet
               :     :        +- Project [l_orderkey#1088L, l_suppkey#1090L]
               :     :           +- Filter ((isnotnull(l_receiptdate#1100) AND isnotnull(l_commitdate#1099)) AND (l_receiptdate#1100 > l_commitdate#1099))
               :     :              +- Relation[l_orderkey#1088L,l_partkey#1089L,l_suppkey#1090L,l_linenumber#1091L,l_quantity#1092,l_extendedprice#1093,l_discount#1094,l_tax#1095,l_returnflag#1096,l_linestatus#1097,l_shipdate#1098,l_commitdate#1099,l_receiptdate#1100,l_shipinstruct#1101,l_shipmode#1102,l_comment#1103] parquet
               :     +- Project [o_orderkey#1059L]
               :        +- Filter ((isnotnull(o_orderstatus#1061) AND (o_orderstatus#1061 = F)) AND isnotnull(o_orderkey#1059L))
               :           +- Relation[o_orderkey#1059L,o_custkey#1060L,o_orderstatus#1061,o_totalprice#1062,o_orderdate#1063,o_orderpriority#1064,o_clerk#1065,o_shippriority#1066L,o_comment#1067] parquet
               +- Project [n_nationkey#1068L]
                  +- Filter ((isnotnull(n_name#1069) AND (n_name#1069 = SAUDI ARABIA)) AND isnotnull(n_nationkey#1068L))
                     +- Relation[n_nationkey#1068L,n_name#1069,n_regionkey#1070L,n_comment#1071] parquet

== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[numwait#1034L DESC NULLS LAST,s_name#1037 ASC NULLS FIRST], output=[s_name#1037,numwait#1034L])
+- *(6) ColumnarToRow
   +- *(10) HashAggregateTransformer(keys=[s_name#1037], functions=[count(1)], output=[s_name#1037, numwait#1034L])
      +- CoalesceBatches
         +- ColumnarExchange hashpartitioning(s_name#1037, 1), ENSURE_REQUIREMENTS, [id=#5484], [id=#5484], [OUTPUT] ArrayBuffer(s_name:StringType, count:LongType), [OUTPUT] ArrayBuffer(s_name:StringType, count:LongType)
            +- *(9) HashAggregateTransformer(keys=[s_name#1037], functions=[partial_count(1)], output=[s_name#1037, count#1107L])
               +- *(9) ProjectExecTransformer [s_name#1037]
                  +- *(9) ShuffledHashJoinExecTransformer [s_nationkey#1039L], [n_nationkey#1068L], Inner, BuildRight
                     :- CoalesceBatches
                     :  +- ColumnarExchange hashpartitioning(s_nationkey#1039L, 1), ENSURE_REQUIREMENTS, [id=#5477], [id=#5477], [OUTPUT] List(s_name:StringType, s_nationkey:LongType), [OUTPUT] List(s_name:StringType, s_nationkey:LongType)
                     :     +- *(7) ProjectExecTransformer [s_name#1037, s_nationkey#1039L]
                     :        +- *(7) ShuffledHashJoinExecTransformer [l_orderkey#1043L], [o_orderkey#1059L], Inner, BuildRight
                     :           :- CoalesceBatches
                     :           :  +- ColumnarExchange hashpartitioning(l_orderkey#1043L, 1), ENSURE_REQUIREMENTS, [id=#5471], [id=#5471], [OUTPUT] List(s_name:StringType, s_nationkey:LongType, l_orderkey:LongType), [OUTPUT] List(s_name:StringType, s_nationkey:LongType, l_orderkey:LongType)
                     :           :     +- *(5) ProjectExecTransformer [s_name#1037, s_nationkey#1039L, l_orderkey#1043L]
                     :           :        +- *(5) ShuffledHashJoinExecTransformer [s_suppkey#1036L], [l_suppkey#1045L], Inner, BuildLeft
                     :           :           :- CoalesceBatches
                     :           :           :  +- ColumnarExchange hashpartitioning(s_suppkey#1036L, 1), ENSURE_REQUIREMENTS, [id=#5395], [id=#5395], [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_nationkey:LongType), [OUTPUT] List(s_suppkey:LongType, s_name:StringType, s_nationkey:LongType)
                     :           :           :     +- *(1) FilterExecTransformer (isnotnull(s_suppkey#1036L) AND isnotnull(s_nationkey#1039L))
                     :           :           :        +- *(1) FileScan parquet default.supplier[s_suppkey#1036L,s_name#1037,s_nationkey#1039L] Batched: true, DataFilters: [isnotnull(s_suppkey#1036L), isnotnull(s_nationkey#1039L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/supplier], PartitionFilters: [], PushedFilters: [IsNotNull(s_suppkey), IsNotNull(s_nationkey)], ReadSchema: struct<s_suppkey:bigint,s_name:string,s_nationkey:bigint>
                     :           :           +- CoalesceBatches
                     :           :              +- ColumnarExchange hashpartitioning(l_suppkey#1045L, 1), ENSURE_REQUIREMENTS, [id=#5465], [id=#5465], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType)
                     :           :                 +- RowToColumnar
                     :           :                    +- *(5) ShuffledHashJoin [l_orderkey#1043L], [l_orderkey#1088L], LeftAnti, BuildRight, NOT (l_suppkey#1090L = l_suppkey#1045L)
                     :           :                       :- *(3) ShuffledHashJoin [l_orderkey#1043L], [l_orderkey#1072L], LeftSemi, BuildRight, NOT (l_suppkey#1074L = l_suppkey#1045L)
                     :           :                       :  :- *(1) ColumnarToRow
                     :           :                       :  :  +- CoalesceBatches
                     :           :                       :  :     +- ColumnarExchange hashpartitioning(l_orderkey#1043L, 1), ENSURE_REQUIREMENTS, [id=#5399], [id=#5399], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType)
                     :           :                       :  :        +- *(2) ProjectExecTransformer [l_orderkey#1043L, l_suppkey#1045L]
                     :           :                       :  :           +- *(2) FilterExecTransformer ((((isnotnull(l_receiptdate#1055) AND isnotnull(l_commitdate#1054)) AND (l_receiptdate#1055 > l_commitdate#1054)) AND isnotnull(l_suppkey#1045L)) AND isnotnull(l_orderkey#1043L))
                     :           :                       :  :              +- *(2) FileScan parquet default.lineitem[l_orderkey#1043L,l_suppkey#1045L,l_commitdate#1054,l_receiptdate#1055] Batched: true, DataFilters: [isnotnull(l_receiptdate#1055), isnotnull(l_commitdate#1054), (l_receiptdate#1055 > l_commitdate#..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_receiptdate), IsNotNull(l_commitdate), IsNotNull(l_suppkey), IsNotNull(l_orderkey)], ReadSchema: struct<l_orderkey:bigint,l_suppkey:bigint,l_commitdate:date,l_receiptdate:date>
                     :           :                       :  +- *(2) ColumnarToRow
                     :           :                       :     +- CoalesceBatches
                     :           :                       :        +- ColumnarExchange hashpartitioning(l_orderkey#1072L, 1), ENSURE_REQUIREMENTS, [id=#5403], [id=#5403], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType)
                     :           :                       :           +- *(3) FileScan parquet default.lineitem[l_orderkey#1072L,l_suppkey#1074L] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<l_orderkey:bigint,l_suppkey:bigint>
                     :           :                       +- *(4) ColumnarToRow
                     :           :                          +- CoalesceBatches
                     :           :                             +- ColumnarExchange hashpartitioning(l_orderkey#1088L, 1), ENSURE_REQUIREMENTS, [id=#5408], [id=#5408], [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType), [OUTPUT] List(l_orderkey:LongType, l_suppkey:LongType)
                     :           :                                +- *(4) ProjectExecTransformer [l_orderkey#1088L, l_suppkey#1090L]
                     :           :                                   +- *(4) FilterExecTransformer ((isnotnull(l_receiptdate#1100) AND isnotnull(l_commitdate#1099)) AND (l_receiptdate#1100 > l_commitdate#1099))
                     :           :                                      +- *(4) FileScan parquet default.lineitem[l_orderkey#1088L,l_suppkey#1090L,l_commitdate#1099,l_receiptdate#1100] Batched: true, DataFilters: [isnotnull(l_receiptdate#1100), isnotnull(l_commitdate#1099), (l_receiptdate#1100 > l_commitdate#..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/lineitem], PartitionFilters: [], PushedFilters: [IsNotNull(l_receiptdate), IsNotNull(l_commitdate)], ReadSchema: struct<l_orderkey:bigint,l_suppkey:bigint,l_commitdate:date,l_receiptdate:date>
                     :           +- CoalesceBatches
                     :              +- ColumnarExchange hashpartitioning(o_orderkey#1059L, 1), ENSURE_REQUIREMENTS, [id=#5423], [id=#5423], [OUTPUT] List(o_orderkey:LongType), [OUTPUT] List(o_orderkey:LongType)
                     :                 +- *(6) ProjectExecTransformer [o_orderkey#1059L]
                     :                    +- *(6) FilterExecTransformer ((isnotnull(o_orderstatus#1061) AND (o_orderstatus#1061 = F)) AND isnotnull(o_orderkey#1059L))
                     :                       +- *(6) FileScan parquet default.orders[o_orderkey#1059L,o_orderstatus#1061] Batched: true, DataFilters: [isnotnull(o_orderstatus#1061), (o_orderstatus#1061 = F), isnotnull(o_orderkey#1059L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [IsNotNull(o_orderstatus), EqualTo(o_orderstatus,F), IsNotNull(o_orderkey)], ReadSchema: struct<o_orderkey:bigint,o_orderstatus:string>
                     +- CoalesceBatches
                        +- ColumnarExchange hashpartitioning(n_nationkey#1068L, 1), ENSURE_REQUIREMENTS, [id=#5433], [id=#5433], [OUTPUT] List(n_nationkey:LongType), [OUTPUT] List(n_nationkey:LongType)
                           +- *(8) ProjectExecTransformer [n_nationkey#1068L]
                              +- *(8) FilterExecTransformer ((isnotnull(n_name#1069) AND (n_name#1069 = SAUDI ARABIA)) AND isnotnull(n_nationkey#1068L))
                                 +- *(8) FileScan parquet default.nation[n_nationkey#1068L,n_name#1069] Batched: true, DataFilters: [isnotnull(n_name#1069), (n_name#1069 = SAUDI ARABIA), isnotnull(n_nationkey#1068L)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/nation], PartitionFilters: [], PushedFilters: [IsNotNull(n_name), EqualTo(n_name,SAUDI ARABIA), IsNotNull(n_nationkey)], ReadSchema: struct<n_nationkey:bigint,n_name:string>

````

[返回目录](#目录)

## Q22: 全球销售机会查询

Q22 语句查询可能会进行购买的客户所在的地理位置。

1. 此查询计算特定国家/地区代码范围内有 7 年未下订单，但帐户余额高于平均水平的“正”帐户余额的客户数量。
2. 能反应出普通消费者的的态度，即购买意向。
3. 国家代码定义为 c_phone 的前两个字符。

Q22 语句的特点是：带有分组、排序、聚集、EXISTS子查询、NOT EXISTS子查询操作并存的四表连接操作。

````mysql
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	( # 第一层子查询
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17') # I1…I7是在TPC-H中定义国家代码的可能值中不重复的任意值
			and c_acctbal > ( # 第二层第一个聚集子查询
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists ( # 第二层第二个NOT EXISTS子查询
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
````

````
== Parsed Logical Plan ==
'Sort ['cntrycode ASC NULLS FIRST], true
+- 'Aggregate ['cntrycode], ['cntrycode, 'count(1) AS numcust#1111, 'sum('c_acctbal) AS totacctbal#1112]
   +- 'SubqueryAlias custsale
      +- 'Project [substring('c_phone, 1, 2) AS cntrycode#1110, 'c_acctbal]
         +- 'Filter ((substring('c_phone, 1, 2) IN (13,31,23,29,30,18,17) AND ('c_acctbal > scalar-subquery#1108 [])) AND NOT exists#1109 [])
            :  :- 'Project [unresolvedalias('avg('c_acctbal), None)]
            :  :  +- 'Filter (('c_acctbal > 0.00) AND substring('c_phone, 1, 2) IN (13,31,23,29,30,18,17))
            :  :     +- 'UnresolvedRelation [customer], [], false
            :  +- 'Project [*]
            :     +- 'Filter ('o_custkey = 'c_custkey)
            :        +- 'UnresolvedRelation [orders], [], false
            +- 'UnresolvedRelation [customer], [], false

== Analyzed Logical Plan ==
cntrycode: string, numcust: bigint, totacctbal: double
Project [cntrycode#1110, numcust#1111L, totacctbal#1112]
+- Sort [cntrycode#1110 ASC NULLS FIRST], true
   +- Aggregate [cntrycode#1110], [cntrycode#1110, count(1) AS numcust#1111L, sum(c_acctbal#1119) AS totacctbal#1112]
      +- SubqueryAlias custsale
         +- Project [substring(c_phone#1118, 1, 2) AS cntrycode#1110, c_acctbal#1119]
            +- Filter ((substring(c_phone#1118, 1, 2) IN (13,31,23,29,30,18,17) AND (c_acctbal#1119 > scalar-subquery#1108 [])) AND NOT exists#1109 [c_custkey#1114L])
               :  :- Aggregate [avg(c_acctbal#1127) AS avg(c_acctbal)#1131]
               :  :  +- Filter ((c_acctbal#1127 > cast(0.00 as double)) AND substring(c_phone#1126, 1, 2) IN (13,31,23,29,30,18,17))
               :  :     +- SubqueryAlias spark_catalog.default.customer
               :  :        +- Relation[c_custkey#1122L,c_name#1123,c_address#1124,c_nationkey#1125L,c_phone#1126,c_acctbal#1127,c_mktsegment#1128,c_comment#1129] parquet
               :  +- Project [o_orderkey#1132L, o_custkey#1133L, o_orderstatus#1134, o_totalprice#1135, o_orderdate#1136, o_orderpriority#1137, o_clerk#1138, o_shippriority#1139L, o_comment#1140]
               :     +- Filter (o_custkey#1133L = outer(c_custkey#1114L))
               :        +- SubqueryAlias spark_catalog.default.orders
               :           +- Relation[o_orderkey#1132L,o_custkey#1133L,o_orderstatus#1134,o_totalprice#1135,o_orderdate#1136,o_orderpriority#1137,o_clerk#1138,o_shippriority#1139L,o_comment#1140] parquet
               +- SubqueryAlias spark_catalog.default.customer
                  +- Relation[c_custkey#1114L,c_name#1115,c_address#1116,c_nationkey#1117L,c_phone#1118,c_acctbal#1119,c_mktsegment#1120,c_comment#1121] parquet

== Optimized Logical Plan ==
Sort [cntrycode#1110 ASC NULLS FIRST], true
+- Aggregate [cntrycode#1110], [cntrycode#1110, count(1) AS numcust#1111L, sum(c_acctbal#1119) AS totacctbal#1112]
   +- Project [substring(c_phone#1118, 1, 2) AS cntrycode#1110, c_acctbal#1119]
      +- Join LeftAnti, (o_custkey#1133L = c_custkey#1114L)
         :- Project [c_custkey#1114L, c_phone#1118, c_acctbal#1119]
         :  +- Filter ((isnotnull(c_acctbal#1119) AND substring(c_phone#1118, 1, 2) IN (13,31,23,29,30,18,17)) AND (c_acctbal#1119 > scalar-subquery#1108 []))
         :     :  +- Aggregate [avg(c_acctbal#1127) AS avg(c_acctbal)#1131]
         :     :     +- Project [c_acctbal#1127]
         :     :        +- Filter ((isnotnull(c_acctbal#1127) AND (c_acctbal#1127 > 0.0)) AND substring(c_phone#1126, 1, 2) IN (13,31,23,29,30,18,17))
         :     :           +- Relation[c_custkey#1122L,c_name#1123,c_address#1124,c_nationkey#1125L,c_phone#1126,c_acctbal#1127,c_mktsegment#1128,c_comment#1129] parquet
         :     +- Relation[c_custkey#1114L,c_name#1115,c_address#1116,c_nationkey#1117L,c_phone#1118,c_acctbal#1119,c_mktsegment#1120,c_comment#1121] parquet
         +- Project [o_custkey#1133L]
            +- Relation[o_orderkey#1132L,o_custkey#1133L,o_orderstatus#1134,o_totalprice#1135,o_orderdate#1136,o_orderpriority#1137,o_clerk#1138,o_shippriority#1139L,o_comment#1140] parquet

== Physical Plan ==
*(3) Sort [cntrycode#1110 ASC NULLS FIRST], true, 0
+- *(3) ColumnarToRow
   +- CoalesceBatches
      +- ColumnarExchange rangepartitioning(cntrycode#1110 ASC NULLS FIRST, 1), ENSURE_REQUIREMENTS, [id=#5775], [id=#5775], [OUTPUT] List(cntrycode:StringType, numcust:LongType, totacctbal:DoubleType), [OUTPUT] List(cntrycode:StringType, numcust:LongType, totacctbal:DoubleType)
         +- *(4) HashAggregateTransformer(keys=[cntrycode#1110], functions=[count(1), sum(c_acctbal#1119)], output=[cntrycode#1110, numcust#1111L, totacctbal#1112])
            +- CoalesceBatches
               +- ColumnarExchange hashpartitioning(cntrycode#1110, 1), ENSURE_REQUIREMENTS, [id=#5770], [id=#5770], [OUTPUT] ArrayBuffer(cntrycode:StringType, count:LongType, sum:DoubleType), [OUTPUT] ArrayBuffer(cntrycode:StringType, count:LongType, sum:DoubleType)
                  +- RowToColumnar
                     +- *(2) HashAggregate(keys=[cntrycode#1110], functions=[partial_count(1), partial_sum(c_acctbal#1119)], output=[cntrycode#1110, count#1148L, sum#1149])
                        +- *(2) Project [substring(c_phone#1118, 1, 2) AS cntrycode#1110, c_acctbal#1119]
                           +- *(2) ColumnarToRow
                              +- *(3) ShuffledHashJoinExecTransformer [c_custkey#1114L], [o_custkey#1133L], LeftAnti, BuildRight
                                 :- CoalesceBatches
                                 :  +- ColumnarExchange hashpartitioning(c_custkey#1114L, 1), ENSURE_REQUIREMENTS, [id=#5759], [id=#5759], [OUTPUT] List(c_custkey:LongType, c_phone:StringType, c_acctbal:DoubleType), [OUTPUT] List(c_custkey:LongType, c_phone:StringType, c_acctbal:DoubleType)
                                 :     +- RowToColumnar
                                 :        +- *(1) Filter ((isnotnull(c_acctbal#1119) AND substring(c_phone#1118, 1, 2) IN (13,31,23,29,30,18,17)) AND (c_acctbal#1119 > Subquery scalar-subquery#1108, [id=#5646]))
                                 :           :  +- Subquery scalar-subquery#1108, [id=#5646]
                                 :           :     +- *(3) ColumnarToRow
                                 :           :        +- *(3) HashAggregateTransformer(keys=[], functions=[avg(c_acctbal#1127)], output=[avg(c_acctbal)#1131])
                                 :           :           +- CoalesceBatches
                                 :           :              +- ColumnarExchange SinglePartition, ENSURE_REQUIREMENTS, [id=#5638], [id=#5638], [OUTPUT] List(sum:DoubleType, count:LongType), [OUTPUT] List(sum:DoubleType, count:LongType)
                                 :           :                 +- RowToColumnar
                                 :           :                    +- *(2) HashAggregate(keys=[], functions=[partial_avg(c_acctbal#1127)], output=[sum#1152, count#1153L])
                                 :           :                       +- *(2) ColumnarToRow
                                 :           :                          +- *(2) ProjectExecTransformer [c_acctbal#1127]
                                 :           :                             +- RowToColumnar
                                 :           :                                +- *(1) Filter ((isnotnull(c_acctbal#1127) AND (c_acctbal#1127 > 0.0)) AND substring(c_phone#1126, 1, 2) IN (13,31,23,29,30,18,17))
                                 :           :                                   +- *(1) ColumnarToRow
                                 :           :                                      +- *(1) FileScan parquet default.customer[c_phone#1126,c_acctbal#1127] Batched: true, DataFilters: [isnotnull(c_acctbal#1127), (c_acctbal#1127 > 0.0), substring(c_phone#1126, 1, 2) IN (13,31,23,29..., Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_acctbal), GreaterThan(c_acctbal,0.0)], ReadSchema: struct<c_phone:string,c_acctbal:double>
                                 :           +- *(1) ColumnarToRow
                                 :              +- *(1) FileScan parquet default.customer[c_custkey#1114L,c_phone#1118,c_acctbal#1119] Batched: true, DataFilters: [isnotnull(c_acctbal#1119), substring(c_phone#1118, 1, 2) IN (13,31,23,29,30,18,17)], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_acctbal)], ReadSchema: struct<c_custkey:bigint,c_phone:string,c_acctbal:double>
                                 +- CoalesceBatches
                                    +- ColumnarExchange hashpartitioning(o_custkey#1133L, 1), ENSURE_REQUIREMENTS, [id=#5736], [id=#5736], [OUTPUT] List(o_custkey:LongType), [OUTPUT] List(o_custkey:LongType)
                                       +- *(2) FileScan parquet default.orders[o_custkey#1133L] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex[file:/data1/test_output/tpch-data-sf10/order], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<o_custkey:bigint>

````

[返回目录](#目录)

# 分析

## 子查询展开

### 1. [Q2](#q2: 最小代价供货商查询)❗️
### 2. [Q11: 库存价值查询](#q11: 库存价值查询)
### 3. [Q16: 零件供货商关系查询](#q16: 零件供货商关系查询)，not in => left anti join
### 4. [Q17: 小订单收入查询](#q17: 小订单收入查询)，相关子查询改成 inner join
### 5. [Q18: 大订单顾客查询](#q18: 大订单顾客查询)，用 **left semi join** 替换不相关的 IN 表达式
### 6. [Q20: 供货商竞争力查询](#q20: 供货商竞争力查询)，相关标量子查询展开
### 7. [Q21: 不能按时交货的供货商查询](#q21: 不能按时交货的供货商查询)

## 模型

|          |                                                              |
| -------- | ------------------------------------------------------------ |
| lineitem | Q1，Q3，Q4，Q5，[Q6](#q6: 预测收入变化查询 ✓)，[Q7](#q7: 货运盈利情况查询)，[Q8](#q8: 国家市场份额查询)，[Q9](#q9: 产品类型利润估量查询  ✓)，[Q10](#q10: 货运存在问题的查询 ✓)，[Q12](#q12: 货运模式和订单优先级查询)，[Q14](#Q14: 促销效果查询 ✓)，[Q17](#q17: 小订单收入查询)，[Q19](#Q19: 折扣收入查询 ✓)，[Q20](#q20: 供货商竞争力查询)，[Q21](#q21: 不能按时交货的供货商查询) |
| partsupp | Q2，[Q11](#q11: 库存价值查询)                                |
| 不知道   | Q13，[Q15](#q15: 头等供货商查询)，[Q16](#q16: 零件供货商关系查询)，[Q18](#q18: 大订单顾客查询)，[Q22](#q22: 全球销售机会查询) |

### 细节分析


这里 C 是指 calcite 的 `LatticeSuggester`，图表的含义如下：

1. 🟡 无法推荐
2. ❌ UT 错
3. ✅ 推荐



| 业务查询                                      |  C   | 模型（事实）或 Join graph 逻辑计划                           |
| --------------------------------------------- | :--: | ------------------------------------------------------------ |
| [Q1](#q1: 价格统计报告查询 ✓)                 |      | lineitem                                                     |
| [Q2](#q2: 最小代价供货商查询)                 |  ❌   | 有两个Join graph， #2  属于标量子查询，Spark 的 `RewriteCorrelatedScalarSubquery` 会改成 `left join`，calcite 应该有相关的规则，==感觉这两个可以 Join graph 搞成一个模型==<br />1.![](http://www.plantuml.com/plantuml/png/SoWkIImgAStDuL8ioKZDJLKeo4dCpEFAJInGA01AA5HpAGJ41pW6XNYxHYhOsYbeQGLuQsbnrO9L59SM5EHafXQ1L0N71uS61A2eKj1ush3dHK_pqzbRdirOhs2eyBHZWqAmcvQXWOw1WO4cI8-VOuYdlnzKbvitq5ourI33GGOh10HZEW2a5YI1QIV2aBp4aipyFB0HVb2Bq9ndPAhmPCUgfxFtFbstJHEiBCZL7Taz2Xfk-KMfcUbv2jwfbGeb-K2TGGWrG2CqeI5Hb9eEj0je5LWQqoQ8HNCsFEtP1T82yv-L5PBdj7HrTUt0NWSAmqIHb4Az2rSA3tuhAxWC5q8C5hXmzT00iR922Wqkc1ZKwmZL82-1PP1ynEMGcfS2J2i0)<br />2.![](http://www.plantuml.com/plantuml/png/SoWkIImgAStDuL8ioKZDJLKeo4dCpEFAJInGA2ujACZ9J2rIgEPI28uF3Wq8y7QDLB2rKz3I2l3MqbBGUDgmvqLFyzFPMvxDMAzWgF2qOuD2i9kMeO6EWO619aYFdsE8fxyVL9URDrSj5zZW0g2D8CD11Yi41CS7E0Q5e1WDKWkImBGJOKXUOabcVXxO23-eHMZECp9LUB9ZrTFPUv-kswO9LXRawexi7WKDj_oYrCpqF0Nlr4g5adoWpY246g0HcborOBkAKjE1TW6j0ajZsWInQCw6fpsxG5fW_hEoGlA3bgwkhXqyJAsiu39S235OYC7LGtrfPONaHIwO6DG_2DKWTu9ba9p9vP2QbmACNW00) |
| [Q3](#Q3: 发货优先级查询 ✓)                   |      | ![](http://www.plantuml.com/plantuml/png/SoWkIImgAStDuL8ioKZDJLKeo4dCpEFAJInGoCdCIyqiISrLgEPIy8dt3t9n3V9sZLImjLFGqWhmrj9ISFDMq73BBqhDJCzJy4wjL2Z91yhumrLgQMc2D0Ou1eL0z04rW0I2GmC2K0HyFFrS_K9zdLo348PprNA17P5Vb99QL0noWe8_DZTeF6tOywAd-UdiBSzch5SmL7ZQiK4Xs4tBqDH0ny30G1EipJaq6Em2e3RiRdO6Au4u6MPnScbnINvkQX7OQeW2acm6w_9p3F7qzmFgmscr7rGDt79Og5jP8OMvWfQkhguTD32ih10XmGKJWliIeXJkV2ihkDkv75BpKe0n3G00) |
| [Q4](#q4: 订单优先级查询)                     |      | 这个子查询会逻辑被优化是 orders  和 lineitem  的 `left semi join`<br />==如果是以 lineitem 为中心的模型，我们应该怎么改写==？ |
| [Q5](#q5: 某地区供货商为公司带来的收入查询 ✓) |  🟡   | 这里多了一个条件，要求 cutstomer 和  supplier 是一个国家![](http://www.plantuml.com/plantuml/png/bPBF2jD04CRl-nHpR1TRKg6NHKjhJQMa9Y5f3vvAiKi9v4_8duD8GV3YeGyWr2Vme3s8YEZR6F4ntCswcIrQrEliD_FxvjkPbjMkgEea9iakXpXAULJn19uJM6szuA4TEFOJk3y1Ids7KbWuq5ja1OyEAJZy6LIPUF4w9QLzsUZFWhNg4mso46vy_wgmMhgsktdym0vmE3cIraGk34a3Fiv1IQiewpoF8rv8LOZ3EvUzviMN7wUNpVd3zzVlOGJDgmk42pgoxbYMrPSzxcozz5oqTEp8ShQOwuU_qwNGHvvPrzwVrcML9SePpBVpJRZ-DsjZxKrqcUR-dBYB6X6vXcV5dXTbYyOl-I-srpc-YTPBcU1qLqLPse8DPoRvxXY-VNhRdBysxpxsMrbWF_hBaW5gA0K9awo27mHBe8ndawH0RoxDwRDiqHVDc77FG0U3mUHNNenXM4INC0WbrV_AMBU_CLeTEAhrVJA6rqAmYEUd6rI0ZBduGh2czaIrYe0n8m5oO_ozuN1uxHxV66HPNf8fJ_Tr4ly4) |
| [Q6](#q6: 预测收入变化查询 ✓)                 |  ✅   | lineitem                                                     |
| [Q7](#q7: 货运盈利情况查询)                   |  ✅   | ![](http://www.plantuml.com/plantuml/png/jPF1IiD048Rl-nJZhLqif8LU5YojDP6IceIaFNYIO1SDP9EIR0uYWk35WmyWz0qyg0SH3_esHdmCwQQRJQMXUF2wyyzy_v_9z5Fk9pnZ8ThXvvHHO7wI8kRF8GmY6d3Au1B1-CHs3tJNr8zXhmSOEoR6O9ZGCEA41cSHcFG2U0mOswKIu-QoqHcuarMVOIuVlAdZh3sCHvPkJO_sTLSDHrT8c8YJ6KtIfGMmDtbevDVlttSt-UBbw_u9sf3VFeDdu7PNszKqhIdQXbDFE134JPw7XKHObk3JBEKneub0Iy4VoABB6an6jYNwQj2MqWYuO4S-3-98a5LzZTtfmkVRGxxuo1zVcqBcweTRKAxIO0nbq3IRpyEW2EgfvMpDAMDwlrEkXUZyVuhY5o54BHgtMgrUkK52olMXwfkq9FM5OK8gPf6y0K8g5x3IAwwi82G_9Y5UpVnE7K0UXfHNHyaAwjDebh7m1m00) |
| [Q8](#q8: 国家市场份额查询)                   |  ❌   | 修改SQL后，可以推荐出<br />![](http://www.plantuml.com/plantuml/png/jLFDIiD04BxlKuotTh6G5dfPiBJML4fg4fBsu4c2NJJG_91i3oA2uCM33o3q3Joe1n4F-ZP6V0oRNJ99fQLUlEvyttpVDxFJJOMNY7am8plYd0SS0Yz9IU35CFD3xWiUm2M1qOdb7EYEeH_3NWSejGrAOM10RH0bt3yBmU0N8A9bnK8afVMSQFUSiU8j2Vc3Ex7jbOVHqDJDoT6-xfJDoHMH9g9aofCqjm3MEW-rxFhz--ucMxnytJz14xBRPt07jDdMTZLDgqjQV-9A1qfnZUU-X2ZBXV3fF1LHm1CfZO2_AqkMsHiFBLFnrakRY57YKZlqX1-5KhbIhsgtsl3vzf0jFhB7rxg4EVhX5Ycdn50A63ITn_7CLq5TNCxsd4LCztVAbH2j_q-XlWXZvQ9fez7eu0OPm_MHyWsNWtXbcB6AMLBS06EL2r1uG5si458Cap5tG__MHW6SlO9NMXKLjR3OIqG-LRjwMdWrbOPOLmqbaJ6aaIuFfyl3_m40) |
| [Q9](#q9: 产品类型利润估量查询  ✓)            |  ✅   | ![](http://www.plantuml.com/plantuml/png/XPBFIlD05CNtUOhxxThXAwH2DmEMrff8IKs6f5suAe4E6iYVaAGBaOBWneKF8FGDNAWB4HVwDaPy33D3fpV1WDixvvxpkoSPvOMV5UierFuL5ppY4FbPhaL-2c4Gyw3W4LnfC5iwxh7XMiOP70w146OH0gO57JF9U70UWyKleKYg5sUl9AGh5jdOdU_sJ4iCl0LZZS5iQXlsulJ8SD5SssWI8ibMFCi50ZXj39to-ltxxgRSldpTFq4Vojjdy4pI7-Z_TLtlohN9mfC4OgsLUQ8a0haAbibfLOl8PVKBcd3vEat3W6T2vk5TVy8fDczfZ-TJnyOsDfWjp2jZvltE5qhM2CIvme_z8aXYMPnTZq6uWm5ylZsKsu_oyRKhPQvnWhBMfbpKL1QoBFMRK5hxIO3qUhtXxkjHYfzEKvrHMhzUodVCbDQ06_QY25JlMw8KIwhhLK5jUlMcjPYe1V5KZTMqAMrwurNeFEBnQXs5Fm00) |
| [Q10](#q10: 货运存在问题的查询 ✓)             |  ✅   | 和 Q4 一个模型？<br />![](http://www.plantuml.com/plantuml/png/bP51IiD068NtSuhVNVjZ8HMw6R2qraHAQXAcxSAL13je83CfwMGX8WXkN7W0eJTmeIv4NEXjZ7WCoQJfj10Gjo_lvNlp_jv2XQdAH4nsr0KN7EPn64aYmZd4aUIHuWAk28nEVNPeCzS-WVqk80Ok8ZWkD9mavT6v19TVWae0qLyx4Pj5CEYpoIhdk8KmdWR1bZ0QUhOtFJwmcVavkIQwH9BEUBeeAe1Vrw6HttpytD_cozVlXsTeGtxt0cC7snrhrxAifeuDfcFTG1FhEWzAYwvSWS-oXKe4JpMwDFoBh5DUVpBqlJ9NZ_QCHyCrMuOgIgGcc-zRxBqEVBq_vilF_EcjgMtCFle3nOm74GfMTM5AD-uBs6grkglXAJMh4oDLQvJMQYfArpiHevaLAd_vF4ettaPwNCuo4Vy2) |
| [Q11](#q11: 库存价值查询)                     |  ❌   | 修改SQL后，可以推荐出<br />![](http://www.plantuml.com/plantuml/png/SoWkIImgAStDuL8ioKZDJLKeoQ_auifDB50eBYqeoCbCBLAevb88ZWyE3GZmTerKiBLJqD8AyDRIKj1ush3dHK_pqzbRdirOhs2eyBHZWqAmcvQXWOw1WO4cI8-VOuYdlnzKbvitLot4u0AWXI33WGO71CS7E0Q5e1WCKXc0nKA82H0ZyX9BClFpG2vJy8DQW-Ou8rE59pjMFPszz-cwRPfWPK6kxiYkKT1moozApKpFK_1EhGJw6gWp2A46Q8Gcs3POA5XP8Vj7IrTNrmxg2Yih92TmmJndPGNr0q8vmYYu39T3QbuACF01) |
| [Q12](#q12: 货运模式和订单优先级查询)         |  ✅   | `lineitem inner join order`                                  |
| [Q13](#Q13: 消费者订单数量查询 ✓)             |      | ==`customer left outer join orders`==                        |
| [Q14](#Q14: 促销效果查询 ✓)                   |  ❌   | `lineitem inner join part`                                   |
| [Q15](#q15: 头等供货商查询)                   |  ❌   |                                                              |
| [Q16](#q16: 零件供货商关系查询)               |  ❌   | 感觉 calcite 对子查询的执行计划支持不好？spark 的执行计划改成了 `left anti join`，所以问题是什么情况下，可以把 `left_anti_join` => `left join` <br />![](http://www.plantuml.com/plantuml/png/TLDHgzem57xFhpWmlBUMNc6xTQ3eaAcmRefZClPOgiOPbwQXIH_SyByl2NYQtj2dVidvpldETtAwKBgeTLDoKXskzAX17GkHItFtZm18X_xGcyEYAAa1ez68tCc3VsPSqzhBEDI5EC7i4pn_jkXTEdw6E7vwYcCJtbirzOlfIoHpVPCqWUrwbVtSmlVT9jj_MQ-6mvOjSyN-Kc02edIImFiqWG-l2QHZWytPmDUF2Ko6NZkohkpNzGEX1TUSHKuS9Km1pmVCNbxQEH2_f0jvLD8Pcsgav8pMLbUvaz5r-5YL9HMwRtgOw9DXatrRms3PA5sLj1uigrBoWWcj1g5XB04TSJI6SJI7DoGeuPb44WaMn8RSX9K_OfJky-gdCYKgBHhEexP8pskw70ZrGgl8HgjuWrOxb3TfgCxfMUU5q2o_LamOwQXLWpaWjXkx-s4tbgc7cP3cZ1cxrDMTWe70125T5TjGyARJcls-w7vcwsTgEMZWgw7bcH1kAze_0UR-3E_4excYNPqWx6kI1HMdfkJ_0G00) |
| [Q17](#q17: 小订单收入查询)                   |  🟡   | `lineitem inner join part` 以及 `lineitem`：<br />![](http://www.plantuml.com/plantuml/png/SoWkIImgAStDuSfFoafDBb58B4k7CNCoyr8pIr9pumsvWA8ADhgwkiXYC6K5G0Cmj1Z1H3BpybABuhFoC_D0Mf1vQ0OQOKf2ZCOq2LMj4KGjicvCQR2_75BpKe1U1G00) |
| [Q18](#q18: 大订单顾客查询)                   |  🟡   | 这个 spark 的执行计划使用了 subplan 重用（[1](https://issues.apache.org/jira/browse/SPARK-13523)，[2](https://github.com/apache/spark/pull/28885)），子查询先执行，然后 linetime 和 orders 分别与之 `left semi join`。<br />![](http://www.plantuml.com/plantuml/png/bP4zRe0m38Ltdo9p90R2XZfmawY0IqFpK-LdG5NltW8rYX8OocRxlS_FP0ZnrSTaDBXnnIcoa4pBle2nnRlqoSQDSQwbypFw3zmuXszIQcLHHJIoXSFqqZHDthnq-1PbGAFawfILRUKNK4XAbkXtmODgtzqphvRt291T8VvvHGUxgTZSFLqXg7ymGvdjZtCERzEBAxsWz8ISKeZEeATQMOkUVjxlbzSdXHprgCvzGnNaouBbf7fSF-g0Tav6_m00) |
| [Q19](#Q19: 折扣收入查询 ✓)                   |      | `lineitem inner join part`                                   |
| [Q20](#q20: 供货商竞争力查询)                 |  ✅   | ![](http://www.plantuml.com/plantuml/png/RP51IyCm5CVl-HJFsneST8ANW6CJMncTNKYtWoSf6BJGTANB3YA2uCM37q3OD_2W7aGywBUnuiUmAKqJjTVtVk_t_YyPhMHSoBL8qPOyvuA3Y8iL4d4EQPBnH781bmYc9pDsuB70EuQz8M1C0up13w3dBmkUd6KGy0kGowepQqcC7JL8nspUpFc1AaGBIdyLffFG2nT7-nupSdI5wX1v5Ku5eCRp5rcjypnDUA6moBBhcBto-ltxxgRSl7pTFy40ojjdY7mySBTTrtNKJ3YUJsQXoMJjLl9AJAF_-nL9EnOsXbgHnJ9PPYePX6QD7Mvd5pxV7ihDH_duwjGOymuDrld6p30OWwEswKSWn3u7z_lzOHE242i1qfzBYFuXJRTd4sAkjdbzSHUlUwX3eWTKgogtwevvZPghZ7d4izErI7y0) |
| [Q21](#q21: 不能按时交货的供货商查询)         |  🟡   | Spark 逻辑执行计划如下，这里的问题是如果我们的模型是以 lineitem->(orders, supplier->nation) 的模型<br />![](http://www.plantuml.com/plantuml/png/jPL1Qnin48Nlyoj43Y4nn93rBJGbMqeXfRJGj6VZsdAhL2iPINiePVzx9RmUPaPUrxJaP8bzR-Pz8-tw8QPLI7rdmNzxrkkaudhbbcwLZ7U_GQd3-gV-DUuwdHVp-Hm6E7YsnYOTXAlJwbvTFBLVtpwgzdFxvh6z89PTyELtz2ZPQW1MKxswkQ79GDZBOsO0O3m-R7I8fOdVhqiFtEGHf3vEK0aloZFSkvfdL2ZHQW1M3776FUAGnyp0QQnnsYJTtPOkbWBPPUntkyCwwBKskxHP9Grbl_PTP_PRIKbgIgjwBQsI-Pmy0yX6XLBmu1ouI3hX-3YQa-rbwFDSbRfEtHnFi_Zdmr5g-dWM_t4yYvEaib4Dki1fS95qOe0FUfkUT6VUUxD_mqbtTKytizdbvMociYGlFtxwajTN62vBSNsTaxAo9K1zQ-virlnTsAYxOBx_I2nnwz9B92vbCGXDXc65fXesXgNNhNInHeSIDf8B5jbtEvxyRjCQ9ZgVN6_jLKI24LOynVhNzPt3akgiN0E896K0d0_l7fC8MnLZwyFosHktl2r1o1n43lfQ38SYnZdDxFHyaQd0CsN9LS-Xrh3B1ZmnYgWBfl6QQJm9H0t--EKMI0qYoOGdLFZ79fDAh6QQQa6f10PLE8Gj71mTg7e2f9aMZRK41yge3nwqs_IT_GC0) |
| [Q22](#q22: 全球销售机会查询)                 |  ❌   | Spark 的执行计划，似乎没有模型，这个如何加速？<br />![](http://www.plantuml.com/plantuml/png/fLB1JiCm3BtxAtoQjYgRclO0L81svS8lH6cMPHbTWX8NWIR-dROgNKQr4pVBVa_FxzdRI39GUsh0rqUj2AEIJlYmqo7Y6H2zK6saD_q5tv1YXhO_wP0FC2MaKbJBPgZV3zxfZZ3YzxQX6H2VCSSiMooo311ZMqSArfdbo6R86HOhV7d59IzPoLbHikA1bJdZzupVhP9zf3516qawxnNaXrbEAYkKiU-8gNFB1x_few_I459JV6RgIknV1XEnHkz9I4TM7BrrPxXiz9GUo7dI0xR2haJzQID5-8t_SRt-Spgu98V3U8J2oE5p3fz6cyqcIHxWUiUUcJvJ5Qmvq0mqQVAg7y5MkrttZJy0) |

# 延伸

## [SQL查询中 in 和 exists 的区别分析](https://www.jianshu.com/p/f212527d76ff)

### The key differences between IN & EXISTS Operator are 

|      | **IN Operator**                                              | **EXISTS Operator**                                          |
| :--- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1.   | IN can be used as a replacement for multiple OR operators.   | To determine if any values are returned or not, we use EXISTS. |
| 2.   | IN works faster than the EXISTS Operator when If the sub-query result is small. | If the sub-query result is larger, then EXISTS works faster than the IN Operator. |
| 3.   | In the IN-condition SQL Engine compares all the values in the IN Clause. | Once true is evaluated in the EXISTS condition then the SQL Engine will stop the process of further matching. |
| 4.   | To check against only a single column, IN operator can be used. | For checking against more than one single column, you can use the EXISTS Operator. |
| 5.   | The IN operator cannot compare anything with NULL values.    | The EXISTS clause can compare everything with NULLs.         |
| 6.   | A direct set of values can be given for comparison.          | Cannot compare directly the values, sub-query needs to be given. |

## [Spark 中支持的 7 种 Join 类型](https://mp.weixin.qq.com/s/YUdy6LvHPRoCsjUhF1NR-g)

https://jishuin.proginn.com/p/763bfbd5a8ff

目前 Apache Spark 3.0 版本中，一共支持以下七种 Join 类型：

|                  |                                                              |                                           |
| ---------------- | ------------------------------------------------------------ | ----------------------------------------- |
| INNER JOIN       | <img src="https://mmbiz.qpic.cn/mmbiz_png/0yBD9iarX0ntSquwAgvK1JVtVYqlhpqZe3icicyiaP7gh7D3Iic8Xd4RDZmalz6S8OWu7iaepBzKNFQ3qctUXIwxn7Qw/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1" style="zoom:50%;" /> |                                           |
| CROSS JOIN       |                                                              | 叉乘                                      |
| LEFT OUTER JOIN  | <img src="https://mmbiz.qpic.cn/mmbiz_png/0yBD9iarX0ntSquwAgvK1JVtVYqlhpqZehDtbvGgCKfFBnoAH669kia7BJjichup5GibAiaLgm3wp4g92JRyCXxgWxA/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1" style="zoom:50%;" /> |                                           |
| RIGHT OUTER JOIN | <img src="https://mmbiz.qpic.cn/mmbiz_png/0yBD9iarX0ntSquwAgvK1JVtVYqlhpqZebiboKicfXcRyT7F3Nwq7Niczib7g0VNmj6Dic6JT6l4Ws4pCcqFb7yezOKw/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1" style="zoom:50%;" /> |                                           |
| FULL OUTER JOIN  | <img src="https://mmbiz.qpic.cn/mmbiz_png/0yBD9iarX0ntSquwAgvK1JVtVYqlhpqZec6YRgROlUnvwBNXOyicM2EH6mH9ic2PtYOaEjel4xias1PtshrJeQ7NDg/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1" style="zoom:50%;" /> |                                           |
| LEFT SEMI JOIN   | <img src="https://mmbiz.qpic.cn/mmbiz_png/0yBD9iarX0ntSquwAgvK1JVtVYqlhpqZe3icicyiaP7gh7D3Iic8Xd4RDZmalz6S8OWu7iaepBzKNFQ3qctUXIwxn7Qw/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1" style="zoom:50%;" /> | 看图像 `inner join`，区别是左表数据不膨胀 |
| LEFT ANTI JOIN   | <img src="https://mmbiz.qpic.cn/mmbiz_png/0yBD9iarX0ntSquwAgvK1JVtVYqlhpqZeHPibaDx6pAicRo4iaiaYN2DgYkScFicsegfuZKbO96dmYk0RTxog3IFhlQg/640?wx_fmt=png&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1" style="zoom:50%;" /> | 看图像 `left  join`，区别是左表数据不膨胀 |

