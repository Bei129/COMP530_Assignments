# COMP530 A8 Report
Dec 15th, 2024

## Collaborator
- Tong Wu (tw68@rice.edu)
- Lingyi Xu (lx28@rice.edu)

## Queries Results

### 1.sql
```sql
|l.l_comment       |
|furiously ir      |
Found 1 records in all.
```

### 2.sql
```sql
|l.l_orderkey      |
|420               |
|547               |
|5895              |
|10917             |
|17701             |
|22182             |
|43045             |
|44741             |
|56866             |
|58052             |
|84003             |
|89059             |
|90912             |
|92198             |
|94439             |
|95075             |
|95845             |
|99392             |
|100481            |
|110279            |
|110530            |
|111425            |
|124290            |
|128865            |
|131525            |
|132993            |
|140418            |
|141381            |
|143524            |
|147301            |
|151942            |
|152967            |
|157156            |
|162691            |
|164965            |
|179780            |
|180483            |
|200388            |
|201413            |
|204320            |
|210112            |
|210885            |
|211201            |
|215395            |
|219460            |
|226567            |
|227842            |
|228165            |
|231782            |
|232705            |
Found 1057 records in all.
```


### 3.sql
```sql
[<MyDB_AggAtt0, int>, <MyDB_AggAtt1, double>, <MyDB_CntAtt, int>]
|agg.sum_0         |agg.avg_1         |
|292098            |3.620425          |
Found 1 records in all.
```

### 4.sql
```sql
[<MyDB_GroupAtt0, string>, <MyDB_AggAtt0, double>, <MyDB_CntAtt, int>]
|agg.avg_2         |att.temp3         |
|150611.604523     |priority: 5-LOW   |
|150129.831018     |priority: 4-NOT SP|
|150068.606146     |priority: 3-MEDIUM|
|150724.811169     |priority: 2-HIGH  |
|150454.466436     |priority: 1-URGENT|
Found 5 records in all.
```

### 5.sql
```sql
[<MyDB_GroupAtt0, string>, <MyDB_AggAtt0, double>, <MyDB_AggAtt1, double>, <MyDB_AggAtt2, double>, <MyDB_AggAtt3, double>, <MyDB_AggAtt4, double>, <MyDB_AggAtt5, double>, <MyDB_AggAtt6, double>, <MyDB_AggAtt7, int>, <MyDB_CntAtt, int>]
|att.temp12        |agg.sum_4         |agg.sum_5         |agg.sum_6         |agg.sum_7         |agg.avg_8         |agg.avg_9         |agg.avg_10        |agg.sum_11        |
|return flag was N |7826475.000000    |11731187444.120041|11144649274.750301|11590849811.731108|25.487758         |38203.874855      |0.049994          |307068            |
Found 1 records in all.
```

### 6.sql
```sql
[<MyDB_GroupAtt0, string>, <MyDB_AggAtt0, int>, <MyDB_CntAtt, int>]
|l.l_shipmode      |agg.sum_13        |
|TRUCK             |49645             |
|RAIL              |50059             |
Found 2 records in all.
```

### 7.sql
```sql
[<MyDB_GroupAtt0, int>, <MyDB_AggAtt0, double>, <MyDB_CntAtt, int>]
|ps.ps_partkey     |agg.avg_14        |
|1                 |665.290000        |
|2                 |646.880000        |
|5                 |219.830000        |
|7                 |763.980000        |
|8                 |916.910000        |
|9                 |188.020000        |
|10                |418.635000        |
|12                |498.395000        |
|13                |409.506667        |
|14                |891.445000        |
|15                |458.670000        |
|18                |367.633333        |
|19                |144.800000        |
|20                |675.540000        |
|23                |103.130000        |
|25                |694.350000        |
|27                |348.610000        |
|28                |204.860000        |
|29                |857.885000        |
|33                |929.050000        |
|34                |265.310000        |
|35                |540.180000        |
|37                |15.720000         |
|43                |493.190000        |
|44                |608.080000        |
|45                |919.630000        |
|46                |106.800000        |
|48                |611.160000        |
|53                |388.080000        |
|54                |686.510000        |
|56                |855.390000        |
|57                |311.370000        |
|58                |527.380000        |
|60                |92.640000         |
|61                |633.740000        |
|62                |620.080000        |
|63                |481.265000        |
|66                |785.750000        |
|70                |327.883333        |
|73                |514.810000        |
|75                |433.590000        |
|77                |875.830000        |
|78                |577.230000        |
|79                |891.180000        |
|81                |314.980000        |
|83                |385.120000        |
|84                |588.785000        |
|86                |158.010000        |
|87                |868.600000        |
|88                |821.430000        |
Found 119448 records in all.
```

### 8.sql

```sql
|n.n_name          |o.o_orderdate     |att.temp0         |
|JAPAN             |1998-07-01        |356.760000        |
|CANADA            |1998-05-21        |14381.941000      |
|UNITED KINGDOM    |1998-05-13        |12604.606200      |
|CANADA            |1998-06-04        |9443.007600       |
|JORDAN            |1998-06-03        |22514.833600      |
|GERMANY           |1998-05-12        |4808.073600       |
|SAUDI ARABIA      |1998-05-17        |3664.473600       |
|SAUDI ARABIA      |1998-06-18        |16261.240500      |
|JAPAN             |1998-06-16        |12532.540800      |
|MOZAMBIQUE        |1998-04-29        |18733.350200      |
|ARGENTINA         |1998-06-21        |46823.319000      |
|ARGENTINA         |1998-06-01        |46707.977400      |
|PERU              |1998-07-13        |37649.667600      |
|RUSSIA            |1998-07-28        |2471.833800       |
|RUSSIA            |1998-05-29        |38952.345000      |
|JORDAN            |1998-03-27        |9849.280000       |
|ARGENTINA         |1998-02-13        |11927.340000      |
|CANADA            |1998-05-11        |32337.270000      |
|INDONESIA         |1998-06-30        |59426.048000      |
|INDIA             |1998-03-15        |30081.290000      |
|FRANCE            |1998-07-12        |14830.246200      |
|INDIA             |1998-03-10        |-50.011200        |
|RUSSIA            |1998-06-24        |9578.646700       |
|ARGENTINA         |1998-05-06        |42779.520000      |
|CANADA            |1998-06-19        |10805.728000      |
|GERMANY           |1998-06-09        |56604.290200      |
|SAUDI ARABIA      |1998-06-24        |9854.460000       |
|RUSSIA            |1998-06-01        |6395.877000       |
|PERU              |1998-06-25        |2554.118400       |
|INDONESIA         |1998-06-02        |11974.710000      |
|INDONESIA         |1998-05-14        |22914.313200      |
|JAPAN             |1998-05-19        |16489.968600      |
|SAUDI ARABIA      |1998-04-14        |5764.113000       |
|ETHIOPIA          |1998-05-16        |24613.284000      |
|MOZAMBIQUE        |1998-07-10        |84926.880000      |
|INDONESIA         |1998-07-24        |39672.126000      |
|GERMANY           |1998-06-10        |10543.814400      |
|GERMANY           |1998-07-31        |12984.678000      |
|FRANCE            |1998-07-03        |28088.586000      |
|INDIA             |1998-04-24        |12730.116000      |
|ROMANIA           |1998-06-09        |16102.097500      |
|ARGENTINA         |1998-06-24        |27462.548000      |
|EGYPT             |1998-06-13        |1716.909400       |
|ARGENTINA         |1998-06-05        |37265.720000      |
|IRAQ              |1998-07-21        |-5125.590000      |
|ETHIOPIA          |1998-05-16        |265.783800        |
|CHINA             |1998-03-21        |6157.703200       |
|CANADA            |1998-04-16        |4043.482800       |
|JORDAN            |1998-05-06        |8547.331400       |
|IRAN              |1998-05-04        |28706.278500      |
Found 2347 records in all.
```


### 9.sql

```sql
[<MyDB_GroupAtt0, string>, <MyDB_AggAtt0, int>, <MyDB_CntAtt, int>]
|att.temp1         |agg.sum_0         |
|name: Supplier#000|1300              |
|name: Supplier#000|900               |
|name: Supplier#000|900               |
|name: Supplier#000|1025              |
|name: Supplier#000|1600              |
|name: Supplier#000|525               |
|name: Supplier#000|1000              |
|name: Supplier#000|1450              |
|name: Supplier#000|1200              |
|name: Supplier#000|1150              |
|name: Supplier#000|1600              |
|name: Supplier#000|1700              |
|name: Supplier#000|1300              |
|name: Supplier#000|950               |
|name: Supplier#000|1650              |
|name: Supplier#000|1200              |
|name: Supplier#000|1550              |
|name: Supplier#000|1075              |
|name: Supplier#000|1150              |
|name: Supplier#000|750               |
|name: Supplier#000|1400              |
|name: Supplier#000|1150              |
|name: Supplier#000|1500              |
|name: Supplier#000|700               |
|name: Supplier#000|1650              |
|name: Supplier#000|1500              |
|name: Supplier#000|850               |
|name: Supplier#000|1250              |
|name: Supplier#000|1375              |
|name: Supplier#000|900               |
|name: Supplier#000|1750              |
|name: Supplier#000|550               |
|name: Supplier#000|1050              |
|name: Supplier#000|1200              |
|name: Supplier#000|1950              |
|name: Supplier#000|900               |
|name: Supplier#000|1350              |
|name: Supplier#000|1250              |
|name: Supplier#000|450               |
|name: Supplier#000|700               |
|name: Supplier#000|1350              |
|name: Supplier#000|950               |
|name: Supplier#000|1625              |
|name: Supplier#000|1150              |
|name: Supplier#000|1000              |
|name: Supplier#000|625               |
|name: Supplier#000|1000              |
|name: Supplier#000|1500              |
|name: Supplier#000|1250              |
|name: Supplier#000|1625              |
Found 10000 records in all.
```

### 10.sql

```sql
[<MyDB_GroupAtt0, string>, <MyDB_CntAtt, int>]
|att.temp2         |
|date was 1995-02-2|
|date was 1995-01-1|
|date was 1995-03-1|
|date was 1995-01-1|
|date was 1995-02-0|
|date was 1995-02-0|
|date was 1995-04-2|
|date was 1995-01-2|
|date was 1995-01-3|
|date was 1995-02-1|
|date was 1995-01-2|
|date was 1995-04-1|
|date was 1995-01-1|
|date was 1995-04-0|
|date was 1995-02-2|
|date was 1995-02-0|
|date was 1995-01-2|
|date was 1995-01-1|
|date was 1995-01-2|
|date was 1995-01-1|
|date was 1995-01-1|
|date was 1995-02-0|
|date was 1995-01-0|
|date was 1995-04-1|
|date was 1995-02-1|
|date was 1995-02-1|
|date was 1995-02-0|
|date was 1995-05-1|
|date was 1995-01-0|
|date was 1995-02-2|
|date was 1995-04-1|
|date was 1995-01-2|
|date was 1995-03-2|
|date was 1995-01-0|
|date was 1995-01-1|
|date was 1995-01-2|
|date was 1995-02-0|
|date was 1995-03-2|
|date was 1995-01-0|
|date was 1995-03-0|
|date was 1995-02-1|
|date was 1995-01-2|
|date was 1995-03-0|
|date was 1995-03-1|
|date was 1995-02-1|
|date was 1995-02-2|
|date was 1995-02-1|
|date was 1995-05-0|
|date was 1995-03-0|
|date was 1995-01-1|
Found 161 records in all.
```

### 11.sql

```sql
[<MyDB_GroupAtt0, string>, <MyDB_GroupAtt1, string>, <MyDB_AggAtt0, int>, <MyDB_CntAtt, int>]
|c.c_name          |n.n_name          |agg.sum_0         |
|Customer#000000004|EGYPT             |2                 |
|Customer#000000007|CHINA             |6                 |
|Customer#000000010|ETHIOPIA          |2                 |
|Customer#000000011|UNITED KINGDOM    |4                 |
|Customer#000000013|CANADA            |5                 |
|Customer#000000016|IRAN              |3                 |
|Customer#000000019|CHINA             |3                 |
|Customer#000000022|CANADA            |5                 |
|Customer#000000023|CANADA            |2                 |
|Customer#000000025|JAPAN             |6                 |
|Customer#000000026|RUSSIA            |4                 |
|Customer#000000028|INDIA             |5                 |
|Customer#000000029|ALGERIA           |4                 |
|Customer#000000031|UNITED KINGDOM    |1                 |
|Customer#000000034|MOROCCO           |8                 |
|Customer#000000037|INDIA             |1                 |
|Customer#000000040|CANADA            |3                 |
|Customer#000000041|IRAN              |1                 |
|Customer#000000043|ROMANIA           |5                 |
|Customer#000000044|MOZAMBIQUE        |7                 |
|Customer#000000046|FRANCE            |1                 |
|Customer#000000047|BRAZIL            |1                 |
|Customer#000000049|IRAN              |2                 |
|Customer#000000055|IRAN              |3                 |
|Customer#000000061|PERU              |1                 |
|Customer#000000062|GERMANY           |3                 |
|Customer#000000064|CANADA            |8                 |
|Customer#000000065|UNITED KINGDOM    |1                 |
|Customer#000000067|INDONESIA         |2                 |
|Customer#000000068|JAPAN             |1                 |
|Customer#000000070|RUSSIA            |7                 |
|Customer#000000073|ALGERIA           |6                 |
|Customer#000000074|EGYPT             |3                 |
|Customer#000000076|ALGERIA           |6                 |
|Customer#000000079|MOROCCO           |1                 |
|Customer#000000080|ALGERIA           |2                 |
|Customer#000000082|CHINA             |2                 |
|Customer#000000085|ETHIOPIA          |1                 |
|Customer#000000086|ALGERIA           |4                 |
|Customer#000000088|MOZAMBIQUE        |1                 |
|Customer#000000091|INDIA             |3                 |
|Customer#000000092|BRAZIL            |7                 |
|Customer#000000094|INDONESIA         |6                 |
|Customer#000000097|PERU              |2                 |
|Customer#000000100|SAUDI ARABIA      |10                |
|Customer#000000103|INDONESIA         |5                 |
|Customer#000000106|ARGENTINA         |4                 |
|Customer#000000107|MOROCCO           |1                 |
|Customer#000000109|MOZAMBIQUE        |2                 |
|Customer#000000112|ROMANIA           |2                 |
Found 72172 records in all.
```

### 12.sql

```sql
[<MyDB_GroupAtt0, string>, <MyDB_GroupAtt1, string>, <MyDB_AggAtt0, int>, <MyDB_CntAtt, int>]
|c2.c_name         |p.p_name          |agg.sum_1         |
|Customer#000000091|thistle brown deep|1                 |
|Customer#000000295|burlywood aquamari|1                 |
|Customer#000000434|orange cream sprin|1                 |
|Customer#000000442|cream tan peru alm|1                 |
|Customer#000000472|moccasin rose sadd|1                 |
|Customer#000000586|cyan sky papaya mo|1                 |
|Customer#000000638|khaki orange seash|1                 |
|Customer#000000797|moccasin rose sadd|1                 |
|Customer#000000851|salmon olive lace |1                 |
|Customer#000000871|moccasin rose sadd|1                 |
|Customer#000000913|deep antique burni|1                 |
|Customer#000001076|metallic blanched |1                 |
|Customer#000001216|black peach light |1                 |
|Customer#000001247|khaki orange seash|1                 |
|Customer#000001264|goldenrod saddle c|1                 |
|Customer#000001265|forest lawn golden|1                 |
|Customer#000001291|peach cornsilk blu|1                 |
|Customer#000001352|cyan sky papaya mo|1                 |
|Customer#000001420|smoke aquamarine t|1                 |
|Customer#000001453|chiffon chartreuse|1                 |
|Customer#000001468|frosted sandy grey|1                 |
|Customer#000001562|aquamarine goldenr|1                 |
|Customer#000001820|goldenrod saddle c|1                 |
|Customer#000001852|blanched rosy fire|1                 |
|Customer#000001999|burlywood peru hot|1                 |
|Customer#000002086|red antique navajo|1                 |
|Customer#000002144|yellow ivory blanc|1                 |
|Customer#000002279|drab frosted linen|1                 |
|Customer#000002308|rose papaya smoke |1                 |
|Customer#000002315|maroon cornflower |1                 |
|Customer#000002515|red bisque khaki f|1                 |
|Customer#000002560|khaki orange seash|1                 |
|Customer#000002774|chiffon chartreuse|1                 |
|Customer#000003038|khaki orange seash|1                 |
|Customer#000003248|red bisque khaki f|1                 |
|Customer#000003355|honeydew pale brow|1                 |
|Customer#000003550|black peach light |1                 |
|Customer#000003650|misty lace goldenr|1                 |
|Customer#000003671|frosted medium min|1                 |
|Customer#000003854|peach cornsilk blu|1                 |
|Customer#000003874|lemon saddle mediu|1                 |
|Customer#000003889|lemon tomato antiq|1                 |
|Customer#000004174|forest violet lave|1                 |
|Customer#000004219|maroon cornflower |1                 |
|Customer#000004327|deep antique burni|1                 |
|Customer#000004328|cream purple pink |1                 |
|Customer#000004390|floral thistle fro|1                 |
|Customer#000004444|aquamarine goldenr|1                 |
|Customer#000004471|peach midnight sie|1                 |
|Customer#000004538|yellow ivory blanc|1                 |
Found 1581 records in all.
```