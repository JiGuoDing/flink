# TODOs

我们现在聚簇的这个逻辑，在现有 Flink 加 rsdb 的架构下面，会不会影响 **点查的顺序性**，能不能用 **聚簇的方式存二级表的明细**，以及了解是否可以实现，是改代码还是改接口。后面我了解新的架构，比如 merge sort 操作之后我们再一起判断一下是否可以进一步。

## 1. 聚簇逻辑对点查顺序性的影响

### 1.1 点查顺序性的定义

定义1. 当使用 RocksDB 作为状态后端（State Backend）时，“点查（Point Lookup）的顺序性” 通常指的是在处理数据流时，为了保证状态访问的正确性和一致性，Flink 如何管理对 RocksDB 中特定 Key 的访问顺序。

定义2. RocksDB 作为 LSM-Tree 结构，利用了有序性来优化查询。

- SST 内部的二分查找：RocksDB 的每个 SST 文件内部数据是严格按 Key 排序的。点查时先查索引块（indexblock），通过二分查找快速锁定目标数据所在的 datablock。
- 块缓存的预取：如果连续点查 Key 空间相近的数据，RocksDB 可能会将它们所在的整个 datablock 一起加载进内存。这时，后续的点查就变成了内存顺序访问，极大地提升了效率。

### 1.2 聚簇逻辑概述

非主键 Join 场景下，一个右流 Join 键可能对应多个左流记录 (一对多/多对多)，把这些左流记录聚簇存储 (逻辑/物理？)，这样在后续右流记录到达时，可以把所有相关的左流记录顺序读出，减少 I/O 次数。

## 2. 聚簇存二级表明细的可行性分析

Join Key (二级键) 层：仅存储 [Join Key | Pointer to Primary Key Layer]。

Primary Key (主键) 层：存储 Join Key 对应的所有主键条目，通过 PKey Group 将同一个 Join Key 对应的 Primary Key 聚簇存在一起。(改变 SSTable 结构？改变 datablock 组织方式？改变 Flink 里的读写接口逻辑？在 Flink 中维护 Join Key 层，而Primary Key 层仍由 RocksDB 负责存储)

非主键 Join 场景下，如果以 Join Key (二级键) 所对应的主键物理聚簇存储，则主键的有序性必然受到影响 (可以考虑在聚簇的范围内有序)，点查有序性不可避免会受到影响。

如何让一个 Join Key 对应的所有 Primary Key 相邻存储 (为了一次性读取)？

### 具体场景分析

左流：订单流 (订单 id 为主键)

右流：用户信息流 (用户 id 为主键)

查询目标：当右流到达一条数据后，从左流状态中找出所有与该用户相关的订单记录。

传统查询过程：

1. 右流到达用户 id 为 U123 的用户信息更新记录。
2. ? 此时并不知道哪些订单记录的用户 id 是 U123。(Flink 是否会自带一个二级索引？)
3. ? 从 RocksDB 中读取这条记录对应的所有订单记录，每读取一条订单记录就比较一条 (InputSideHasUniqueKey 场景，unique key 即订单 id 作为 RocksDB Key)。(具体执行过程待验证)
4. ? 从 RocksDB 中读取这条记录对应的所有订单记录，每读取一条订单记录就比较一条 (InputSideHasNoUniqueKey 场景，整个 Record 作为 RocksDB Key)。(具体执行过程待验证)

预期查询过程：

1. 右流到达用户 id 为 U123 的用户信息更新记录。
2. 从 Join Key 层查找 U123 对应的 Primary Key 层指针及偏移量 (PS-Tree)。
3. 验证订单 id 有效性后从 Primary Key 层顺序读取所有订单 id。(此时获取了所有用户 id 为 U123 的订单 id 列表)
4. 根据获取的订单 id 列表从 RocksDB 中读取对应订单记录。
