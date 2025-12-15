# gossip-lab 小实验说明

## 1. 实验目标

本目录提供 3 个基于 Java 的 Gossip 协议小实验，方便在学习理论后一键运行、观察协议在「心跳传播」「故障检测」「网络分区 / 脑裂」等场景下的行为。

### 1.1 代码位置

实验代码与脚本位于：

- 源码：`src/main/java/com/bigdatatheory/gossip/**`
- 脚本：`scripts/*.sh`

### 1.2 运行前置条件

- 已安装 Java 8 或以上版本
- 在仓库根目录下执行命令前，建议先 `git pull` 保持代码最新
- 所有脚本都会自动调用本目录下的 `build.sh`，使用 Maven 构建可执行 JAR，无需手动编译

### 1.3 快速一键体验

在仓库根目录下执行：

```bash
# 进入 gossip-lab 目录，后续三个实验都在此目录下运行
cd courses/other/gossip-lab

# 实验一：单进程心跳 Gossip 模拟
bash scripts/run-gossip-simulation.sh

# 实验二：alive/suspect/dead 故障检测模拟
bash scripts/run-membership-simulation.sh

# 实验三：网络分区 / 消息丢失导致的脑裂
bash scripts/run-partition-simulation.sh
```

---

## 2. 实验一：单进程心跳 Gossip 模拟（`run-gossip-simulation.sh`）

对应理论：Gossip 基本模型、心跳传播与收敛速度（参见讲义 `2.3 收敛性与复杂度分析`）。

### 2.1 实验目的

- 观察心跳在固定规模节点中的扩散过程
- 感受「随机对等节点传播」带来的快速收敛特性
- 验证理论上关于 `O(log N)` 级别收敛轮数的直观印象

### 2.2 运行方式

在 `courses/other/gossip-lab` 目录下执行：

```bash
# 5 个节点，模拟 20 轮 Gossip 心跳传播
bash scripts/run-gossip-simulation.sh 5 20
```

脚本参数与默认值如下：

| **位置** | **参数名**   | **含义**                           | **默认值** | **推荐值** |
| -------- | ------------ | ---------------------------------- | ---------- | ---------- |
| $1       | `NODE_COUNT` | 集群节点数                         | 5          | 5、10      |
| $2       | `ROUNDS`     | 模拟轮数（每轮每个节点 tick 一次） | 20         | 20、30     |

示例输出（取自 `GossipSimulationMain` 最后一轮统计）：

```text
# 每个节点看到的成员数量和最大心跳值（不同运行略有差异）
node-0 members=5 maxHeartbeat=20
node-1 members=5 maxHeartbeat=20
node-2 members=5 maxHeartbeat=20
node-3 members=5 maxHeartbeat=20
node-4 members=5 maxHeartbeat=20
```

### 2.3 推荐参数与预期现象

- 推荐配置一：`bash scripts/run-gossip-simulation.sh 5 20`
  - 预期现象：最终每个节点的 `members` 数基本等于节点总数 `5`，`maxHeartbeat` 接近轮数 `20`。
  - 含义：心跳经过多轮随机传播后，所有节点都「几乎」同步，体现 Gossip 的最终一致性特征。
- 推荐配置二：`bash scripts/run-gossip-simulation.sh 10 20`
  - 预期现象：在相同轮数下，`maxHeartbeat` 接近 `20`，但中间过程某些节点对远端节点的心跳更新会稍慢。
  - 含义：节点数增大，收敛仍然较快，但需要更多轮数才能达到「几乎全局同步」。

---

## 3. 实验二：alive/suspect/dead 故障检测模拟（`run-membership-simulation.sh`）

对应理论：SWIM 风格的故障检测、alive/suspect/dead 状态机（参见讲义 `3.1 基于心跳的成员管理` 与 `3.2 SWIM 协议`）。

### 3.1 实验目的

- 通过显式的 alive/suspect/dead 状态机，观察节点从正常到怀疑再到死亡的转换过程
- 理解 `suspectThreshold`、`deadThreshold` 两个超时参数如何影响故障检测的速度与误判风险

### 3.2 运行方式

在 `courses/other/gossip-lab` 目录下执行：

```bash
# 5 个节点，总共 20 轮，第 5 轮让 node-2 发生故障
# suspectThreshold=3，deadThreshold=6
bash scripts/run-membership-simulation.sh 5 20 2 5 3 6
```

脚本参数与默认值如下：

| 位置 | 参数名              | 含义                                          | 默认值 | 推荐值 |
| ---- | ------------------- | --------------------------------------------- | ------ | ------ |
| $1   | `NODE_COUNT`        | 集群节点数                                    | 5      | 5      |
| $2   | `ROUNDS`            | 模拟总轮数                                    | 20     | 20、30 |
| $3   | `FAILED_INDEX`      | 发生故障的节点索引，对应 `node-$FAILED_INDEX` | 2      | 2      |
| $4   | `FAILURE_ROUND`     | 触发故障的轮次                                | 5      | 5      |
| $5   | `SUSPECT_THRESHOLD` | 从最后一次心跳开始进入 suspect 的轮数         | 3      | 3      |
| $6   | `DEAD_THRESHOLD`    | 从最后一次心跳开始进入 dead 的轮数            | 6      | 6、8   |

示例输出结构（取自 `MembershipSimulationMain` 最后一轮视图）：

```text
# 说明行：标记了故障节点、轮数和阈值
failedNode=node-2 failureRound=5 totalRounds=20 suspectThreshold=3 deadThreshold=6

# 每行是一个节点眼中的成员列表及其状态
node-0 view: node-0=ALIVE node-1=ALIVE node-2=DEAD node-3=ALIVE node-4=ALIVE
node-1 view: node-0=ALIVE node-1=ALIVE node-2=DEAD node-3=ALIVE node-4=ALIVE
...
```

### 3.3 推荐参数与预期现象

- 推荐配置：`bash scripts/run-membership-simulation.sh 5 20 2 5 3 6`
  - 预期时间线（按轮数大致划分）：
    - 轮 `< 5`：所有节点都认为 `node-2` 为 `ALIVE`。
    - 轮 `≈ 5 + suspectThreshold`（约第 8 轮）开始：部分节点会把 `node-2` 标记为 `SUSPECT`。
    - 轮 `≥ 5 + deadThreshold`（约第 11 轮）后：大部分节点会把 `node-2` 标记为 `DEAD`。
  - 含义：通过调节 `suspectThreshold`、`deadThreshold` 可以权衡「检测速度」与「误判概率」——阈值越小，检测越快但越容易误判；阈值越大，检测越慢但更保守。

---

## 4. 实验三：网络分区 / 消息丢失导致的脑裂（`run-partition-simulation.sh`）

对应理论：网络分区、消息丢失、错误故障判断与脑裂（brain split）现象（参见讲义 `4.3 工程实践中的常见问题 - 误判故障与脑裂`）。

### 4.1 实验目的

- 在没有真正节点宕机的情况下，仅通过丢弃跨分区消息，观察两侧互相「误判死亡」的过程
- 直观感受网络分区对 Gossip 视图的一致性影响

### 4.2 运行方式

在 `courses/other/gossip-lab` 目录下执行：

```bash
# 6 个节点，总共 20 轮，从第 5 轮开始到第 20 轮存在网络分区
# suspectThreshold=3，deadThreshold=6，跨分区消息全部丢弃（1.0）
bash scripts/run-partition-simulation.sh 6 20 5 20 3 6 1.0
```

脚本参数与默认值如下：

| 位置 | 参数名                   | 含义                                   | 默认值 | 推荐值   |
| ---- | ------------------------ | -------------------------------------- | ------ | -------- |
| $1   | `NODE_COUNT`             | 集群节点数                             | 6      | 6        |
| $2   | `ROUNDS`                 | 模拟总轮数                             | 20     | 20、30   |
| $3   | `PARTITION_START`        | 网络分区开始轮数                       | 5      | 5        |
| $4   | `PARTITION_END`          | 网络分区结束轮数（包含）               | 20     | 20       |
| $5   | `SUSPECT_THRESHOLD`      | suspect 阈值                           | 3      | 3        |
| $6   | `DEAD_THRESHOLD`         | dead 阈值                              | 6      | 6        |
| $7   | `CROSS_DROP_PROBABILITY` | 跨分区消息丢弃概率，`1.0` 表示全部丢弃 | 1.0    | 0.5、1.0 |

示例输出结构（取自 `PartitionSimulationMain` 最后一轮视图）：

```text
# 顶部一行给出实验参数和快照轮次
partitioned simulation: nodeCount=6 totalRounds=20 partition=[5,20] suspectThreshold=3 deadThreshold=6 crossDropProbability=1.0 snapshotRound=20

# 两个分区各自认为对方「全部死亡」，形成脑裂
node-0 view: node-0=ALIVE node-1=ALIVE node-2=ALIVE node-3=DEAD node-4=DEAD node-5=DEAD
node-3 view: node-0=DEAD node-1=DEAD node-2=DEAD node-3=ALIVE node-4=ALIVE node-5=ALIVE
...
```

### 4.3 推荐参数与预期现象

- 推荐配置一：`bash scripts/run-partition-simulation.sh 6 20 5 20 3 6 1.0`
  - 预期现象：
    - 节点 `node-0` ~ `node-2` 形成一个分区，`node-3` ~ `node-5` 形成另一个分区。
    - 在最后几轮，各自分区内的节点都认为「自己分区内部 `ALIVE`，对方分区全部 `DEAD`」，典型脑裂。
- 推荐配置二：`bash scripts/run-partition-simulation.sh 6 20 5 20 3 6 0.5`
  - 预期现象：
    - 由于跨分区消息有一半概率仍然送达，一部分节点会对对方分区的状态在 `ALIVE` 和 `SUSPECT` / `DEAD` 之间摇摆。
  - 含义：通过调整 `crossDropProbability` 可以连续地从「完全连通」过渡到「完全断联」，观察 Gossip 在不同网络质量下的收敛与误判行为。

---

## 5. 理论学习与实验联动建议

- 先阅读 Gossip 协议的基础理论（`2. Gossip 核心原理与系统模型`），重点理解 **心跳传播** 与 **最终一致性**，再运行实验一，感受收敛速度与成员视图变化。
- 阅读 SWIM / 故障检测相关章节（`3.1 基于心跳的成员管理`、`3.2 SWIM 协议`）后，运行实验二，重点观察 **alive -> suspect -> dead** 状态出现的轮数与阈值之间的关系。
- 理解网络分区与脑裂（`4.3 工程实践中的常见问题`）后，运行实验三，通过调整 `CROSS_DROP_PROBABILITY` 和时长区间 `[PARTITION_START, PARTITION_END]`，尝试构造不同程度的 **误判与脑裂** 场景。
