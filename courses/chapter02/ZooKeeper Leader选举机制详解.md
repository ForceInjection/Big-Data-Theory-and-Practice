# ZooKeeper Leader 选举机制详解

## 摘要

Apache ZooKeeper 是一个开源的分布式协调服务，广泛应用于分布式系统中的配置管理、命名服务、分布式锁等场景。Leader 选举是 ZooKeeper 集群正常运行的核心机制，确保集群在任何时候都有且仅有一个 Leader 节点来协调事务处理。本文将深入分析 ZooKeeper 的 Leader 选举算法，通过三节点集群的具体场景，详细描述首次启动、集群重启、节点宕机等不同情况下的选举过程。

**关键词**：ZooKeeper、Leader 选举、分布式一致性、ZAB 协议、分布式系统

---

## 1. 引言

在分布式系统中，协调服务扮演着至关重要的角色。ZooKeeper 作为 Apache 基金会的顶级项目，为分布式应用提供了高可用的协调服务。其核心的 ZAB（ZooKeeper Atomic Broadcast）协议通过 Leader 选举机制确保集群的一致性和可用性。

Leader 选举是 ZooKeeper 集群管理的基础，它决定了哪个节点将承担协调者的角色，负责处理所有的写请求并将状态变更同步到其他节点。理解 Leader 选举的工作原理对于运维 ZooKeeper 集群、排查问题以及优化性能都具有重要意义。

---

## 2. ZooKeeper 集群架构基础

### 2.1 节点角色与职责

ZooKeeper 集群采用主从架构，包含三种节点角色：

| 节点角色 | 主要职责 | 投票权 | 数量限制 |
|----------|----------|--------|----------|
| **Leader** | 处理写请求、协调事务、维护集群状态 | 有 | 有且仅有 1 个 |
| **Follower** | 处理读请求、参与投票、同步数据 | 有 | 多个 |
| **Observer** | 处理读请求、同步数据、不参与投票 | 无 | 多个（可选） |

### 2.2 集群规模设计原则

ZooKeeper 集群通常采用奇数个节点，以确保在网络分区时能够形成过半数的仲裁：

| 集群规模 | 容错能力 | 最小仲裁数 | 适用场景 |
|----------|----------|------------|----------|
| **3 节点** | 容忍 1 个故障 | 2 | 开发测试环境 |
| **5 节点** | 容忍 2 个故障 | 3 | 生产环境 |
| **7 节点** | 容忍 3 个故障 | 4 | 高可用要求 |

### 2.3 投票格式与比较算法

#### 2.3.1 投票格式

ZooKeeper 使用四元组投票格式：`(leader, zxid, peerEpoch, electionEpoch)`

- **leader**：推荐的 Leader 节点 ID
- **zxid**：推荐节点的最新事务 ID  
- **peerEpoch**：推荐节点的 epoch 值
- **electionEpoch**：当前选举轮次

#### 2.3.2 比较算法

当节点收到其他节点的投票时，按以下优先级顺序进行比较：

1. **electionEpoch 比较**：
   - 如果接收到的 `electionEpoch` > 本地 `electionEpoch`，更新本地选举轮次并接受该投票
   - 如果接收到的 `electionEpoch` < 本地 `electionEpoch`，忽略该投票
   - 如果相等，继续下一步比较

2. **peerEpoch 比较**：
   - 数值越大优先级越高，反映节点的数据版本新旧
   - 如果不相等，选择 `peerEpoch` 更大的候选者
   - 如果相等，继续下一步比较

3. **zxid 比较**：
   - 64 位事务标识符，数值越大优先级越高
   - 确保选择拥有最新数据的节点作为 Leader
   - 如果不相等，选择 `zxid` 更大的候选者
   - 如果相等，继续下一步比较

4. **leader（myid）比较**：
   - 节点标识符，数值越大优先级越高
   - 用于打破前面所有条件都相等的平局情况
   - 选择 `leader`（即候选者的 myid）更大的节点

### 2.4 Epoch 概念详解

#### 2.4.1 electionEpoch（选举轮次）

- **定义**：选举过程中的逻辑时钟，标识当前是第几轮选举
- **作用**：防止不同轮次的投票相互干扰，确保选举过程的有序性
- **变化时机**：
  - 节点进入 LOOKING 状态时，`electionEpoch` 递增
  - 收到更高 `electionEpoch` 的投票时，更新本地值
- **生命周期**：仅在选举过程中有效，选举结束后重置

#### 2.4.2 peerEpoch（节点轮次）

- **定义**：节点记录的最后一个 Leader 的 epoch 值
- **作用**：反映节点参与的最新 Leader 任期，确保数据一致性
- **变化时机**：
  - 新 Leader 当选时，所有节点更新为新的 `peerEpoch`
  - 节点重启后从持久化存储中恢复
- **生命周期**：持久化保存，跨选举轮次保持

#### 2.4.3 两者关系与应用场景

**关系对比：**

- **层次不同**：`electionEpoch` 是选举层面的计数器，`peerEpoch` 是集群层面的版本号
- **作用域不同**：`electionEpoch` 仅在选举期间有效，`peerEpoch` 在整个集群运行期间有效
- **优先级**：比较投票时，先比较 `electionEpoch`（确保同一轮选举），再比较 `peerEpoch`（确保数据新旧）

**实际场景举例：**

```text
场景：集群运行中 Leader 故障，触发新选举

节点状态：
- 当前集群 peerEpoch = 5（上一个 Leader 的任期）
- 节点 A 启动选举：electionEpoch = 1
- 节点 B 收到投票后：electionEpoch = 1
- 选举完成后：新 Leader 的 peerEpoch = 6
```

### 2.5 投票比较示例

#### 2.5.1 基于 peerEpoch 的选择

投票 A: (leader=1, zxid=0x500000001, peerEpoch=4, electionEpoch=3)
投票 B: (leader=2, zxid=0x500000002, peerEpoch=5, electionEpoch=3)

**比较过程：**

1. electionEpoch: 3 == 3 (相等，继续比较)
2. peerEpoch: 4 < 5 (选择投票 B，因为节点 2 的 peerEpoch 更大)

**结果：选择节点 2 作为 Leader。**

#### 2.5.2 基于 zxid 的选择

投票 A: (leader=1, zxid=0x500000001, peerEpoch=5, electionEpoch=3)
投票 B: (leader=2, zxid=0x500000002, peerEpoch=5, electionEpoch=3)

**比较过程：**

1. electionEpoch: 3 == 3 (相等，继续比较)
2. peerEpoch: 5 == 5 (相等，继续比较)
3. zxid: 0x500000001 < 0x500000002 (选择投票 B，因为节点 2 的 zxid 更大)

**结果：选择节点 2 作为 Leader。**

#### 2.5.3 基于 myid 的平局决胜

投票 A: (leader=1, zxid=0x500000001, peerEpoch=5, electionEpoch=3)
投票 B: (leader=2, zxid=0x500000001, peerEpoch=5, electionEpoch=3)

**比较过程：**

1. electionEpoch: 3 == 3 (相等，继续比较)
2. peerEpoch: 5 == 5 (相等，继续比较)  
3. zxid: 0x500000001 == 0x500000001 (相等，继续比较)
4. leader: 1 < 2 (选择投票 B，因为节点 2 的 ID 更大)

**结果：选择节点 2 作为 Leader。**

### 2.6 配置参数

**关键配置参数：**

| 参数 | 默认值 | 说明 | 影响 |
|------|--------|------|------|
| `tickTime` | 2000ms | 心跳间隔 | 故障检测速度 |
| `initLimit` | 10 | 初始连接超时倍数 | 集群启动时间 |
| `syncLimit` | 5 | 同步超时倍数 | 数据一致性保证 |
| `electionAlg` | 3 | 选举算法 | 选举效率和可靠性 |

---

## 3. 三节点集群首次启动选举过程

### 3.1 场景设置

假设我们有一个三节点的 ZooKeeper 集群：

- **节点 A**：myid=1，IP=192.168.1.101
- **节点 B**：myid=2，IP=192.168.1.102  
- **节点 C**：myid=3，IP=192.168.1.103

所有节点初始状态：peerEpoch=0，zxid=0，electionEpoch=1，状态=LOOKING

### 3.2 启动顺序：A → B → C

#### 3.2.1 节点 A 启动（t=0s）

```text
节点 A 状态：LOOKING
初始投票：(leader=1, zxid=0, peerEpoch=0, electionEpoch=1)
行为：
1. 向自己投票：Vote(1, 0, 0, 1)
2. 尝试连接其他节点（B、C 未启动，连接失败）
3. 等待其他节点启动
```

**关键点**：单个节点无法形成过半数仲裁（1 < 2），继续等待。

#### 3.2.2 节点 B 启动（t=10s）

```text
节点 B 状态：LOOKING
初始投票：(leader=2, zxid=0, peerEpoch=0, electionEpoch=1)
投票交换过程：

步骤 1：B 向 A 发送投票
B → A: Vote(2, 0, 0, 1)

步骤 2：A 收到 B 的投票，比较决策因素
A 的投票：(1, 0, 0, 1)
B 的投票：(2, 0, 0, 1)
比较结果：electionEpoch 相同，peerEpoch 相同，zxid 相同，myid(2) > myid(1)
A 更新投票：Vote(2, 0, 0, 1)

步骤 3：A 向 B 发送更新后的投票
A → B: Vote(2, 0, 0, 1)

步骤 4：投票统计
节点 A：支持节点 B (2票中的1票)
节点 B：支持节点 B (2票中的2票)
```

**关键点**：虽然 B 获得了所有当前节点的支持，但仍未达到过半数仲裁（2 票 ≥ ⌊3/2⌋ + 1 = 2 票，实际上已经满足，但需要等待 C 节点确认）

#### 3.2.3 节点 C 启动（t=20s）

```text
节点 C 状态：LOOKING
初始投票：(leader=3, zxid=0, peerEpoch=0, electionEpoch=1)
投票交换过程：

步骤 1：C 向 A、B 发送投票
C → A: Vote(3, 0, 0, 1)
C → B: Vote(3, 0, 0, 1)

步骤 2：A、B 收到 C 的投票并比较
A 当前投票：(2, 0, 0, 1)
C 的投票：(3, 0, 0, 1)
比较结果：electionEpoch 相同，peerEpoch 相同，zxid 相同，myid(3) > myid(2)
A 更新投票为 Vote(3, 0, 0, 1)

B 当前投票：(2, 0, 0, 1)  
C 的投票：(3, 0, 0, 1)
比较结果：electionEpoch 相同，peerEpoch 相同，zxid 相同，myid(3) > myid(2)
B 更新投票为 Vote(3, 0, 0, 1)

步骤 3：A、B 向所有节点广播更新后的投票
A → B,C: Vote(3, 0, 0, 1)
B → A,C: Vote(3, 0, 0, 1)

步骤 4：最终投票统计
节点 A：支持节点 C
节点 B：支持节点 C  
节点 C：支持节点 C
总计：3票支持节点 C，达到过半数仲裁（3 ≥ 2）
```

#### 3.2.4 选举结果确认

```text
选举结果：
- Leader：节点 C (myid=3)
- Follower：节点 A (myid=1)、节点 B (myid=2)

状态转换：
节点 A：LOOKING → FOLLOWING
节点 B：LOOKING → FOLLOWING  
节点 C：LOOKING → LEADING

后续步骤：
1. 节点 C 开始 Leader 初始化
2. 节点 A、B 连接到 Leader C
3. 进入数据同步阶段
4. 集群开始正常服务
```

### 3.3 时序图总结

```text
时间轴    节点A(myid=1)         节点B(myid=2)         节点C(myid=3)
t=0s     [启动] LOOKING         [离线]                [离线]
         投票: (1,0,0,1)
         
t=10s    LOOKING               [启动] LOOKING         [离线]
         投票: (2,0,0,1)        投票: (2,0,0,1)
         
t=20s    LOOKING               LOOKING               [启动] LOOKING
         投票: (3,0,0,1)        投票: (3,0,0,1)       投票: (3,0,0,1)
         
t=25s    FOLLOWING             FOLLOWING             LEADING
         连接到C                连接到C                初始化Leader
```

---

## 4. 集群重启场景下的选举过程

### 4.1 场景设置

假设之前运行的三节点集群因维护需要全部重启，重启前的状态：

- **节点 A**：myid=1，peerEpoch=5，zxid=0x500000012
- **节点 B**：myid=2，peerEpoch=5，zxid=0x500000015  
- **节点 C**：myid=3，peerEpoch=5，zxid=0x500000010

**关键差异**：节点 B 拥有最新的事务 ID（zxid=0x500000015）

### 4.2 重启顺序：C → A → B

#### 4.2.1 节点 C 重启（t=0s）

- **节点 C 状态**：LOOKING
- **从磁盘恢复**：peerEpoch=5, zxid=0x500000010, myid=3
- **初始投票**：Vote(3, 0x500000010, 5, 1)
- **行为**：等待其他节点

#### 4.2.2 节点 A 重启（t=15s）

- **节点 A 状态**：LOOKING  
- **从磁盘恢复**：peerEpoch=5, zxid=0x500000012, myid=1
- **初始投票**：Vote(1, 0x500000012, 5, 1)

**投票交换：**

```text
A → C: Vote(1, 0x500000012, 5, 1)
C → A: Vote(3, 0x500000010, 5, 1)
```

**比较过程：**

```text
A 比较 C 的投票：
- electionEpoch 相同 (1 = 1)
- peerEpoch 相同 (5 = 5)
- zxid 比较：0x500000010 < 0x500000012
- A 的数据更新，保持投票 Vote(1, 0x500000012, 5, 1)

C 比较 A 的投票：
- electionEpoch 相同 (1 = 1)
- peerEpoch 相同 (5 = 5)  
- zxid 比较：0x500000012 > 0x500000010
- C 更新投票为 Vote(1, 0x500000012, 5, 1)
```

**当前投票状态：**

```text
节点 A：支持节点 A
节点 C：支持节点 A
```

#### 4.2.3 节点 B 重启（t=30s）

- **节点 B 状态**：LOOKING
- **从磁盘恢复**：peerEpoch=5, zxid=0x500000015, myid=2  
- **初始投票**：Vote(2, 0x500000015, 5, 1)

**投票交换：**

```text
B → A,C: Vote(2, 0x500000015, 5, 1)
A → B: Vote(1, 0x500000012, 5, 1)
C → B: Vote(1, 0x500000012, 5, 1)
```

**比较过程：**

```text
A 和 C 比较 B 的投票：
- electionEpoch 相同 (1 = 1)
- peerEpoch 相同 (5 = 5)
- zxid 比较：0x500000015 > 0x500000012  
- A 和 C 都更新投票为 Vote(2, 0x500000015, 5, 1)
```

**最终投票统计：**

```text
节点 A：支持节点 B
节点 B：支持节点 B
节点 C：支持节点 B
总计：3票支持节点 B，达到过半数仲裁
```

#### 4.2.4 选举结果

**选举结果：**

- Leader：节点 B (拥有最新的 zxid)
- Follower：节点 A、节点 C

**关键原因：**

- 虽然节点 C 的 myid 最大，但节点 B 拥有最新的事务数据，
- 根据选举规则，zxid 的优先级高于 myid。

### 4.3 时序图总结

```text
时间轴    节点A(myid=1)         节点B(myid=2)         节点C(myid=3)
t=0s     [离线]                [离线]                [重启] peerEpoch=5
                                                     LOOKING
                                                     投票: (3,0x500000010,5,1)
         
t=15s    [重启] peerEpoch=5     [离线]                LOOKING
         LOOKING                                     投票: (1,0x500000012,5,1)
         投票: (1,0x500000012,5,1)
         
t=30s    LOOKING               [重启] peerEpoch=5     LOOKING
         投票: (2,0x500000015,5,1)  LOOKING           投票: (2,0x500000015,5,1)
                               投票: (2,0x500000015,5,1)
         
t=35s    FOLLOWING             LEADING               FOLLOWING
         连接到B                初始化Leader           连接到B
```

---

## 4.4 选举时间配置说明

ZooKeeper 的选举时间受多个配置参数影响：

### 4.4.1 关键时间参数

| 参数名称 | 默认值 | 说明 | 影响 |
|----------|--------|------|------|
| **tickTime** | 2000ms | 基本时间单位，心跳间隔 | 故障检测速度 |
| **initLimit** | 10 | Follower 连接 Leader 的超时时间（tickTime 的倍数） | 初始同步时间 |
| **syncLimit** | 5 | Follower 与 Leader 同步的超时时间（tickTime 的倍数） | 数据同步时间 |
| **electionAlg** | 3 | 选举算法（FastLeaderElection） | 选举效率 |

### 4.4.2 选举时间估算

**故障检测时间**：

- 心跳超时：`tickTime * syncLimit = 2000ms * 5 = 10s`
- 实际检测：通常在 2-5 秒内完成

**选举过程时间**：

- 投票交换：1-3 秒（取决于网络延迟）
- 结果确认：1-2 秒
- 总计：3-8 秒

**完整恢复时间**：

- 故障检测 + 选举过程 + 数据同步：通常 10-20 秒

### 4.4.3 electionAlg 参数详解

ZooKeeper 支持多种选举算法，通过 `electionAlg` 参数配置：

| 算法编号 | 算法名称 | 状态 | 说明 |
|----------|----------|------|------|
| **0** | LeaderElection | 已废弃 | 原始选举算法，性能较差 |
| **1** | AuthFastLeaderElection | 已废弃 | 带认证的快速选举算法 |
| **2** | AuthFastLeaderElection | 已废弃 | UDP 版本的认证快速选举 |
| **3** | FastLeaderElection | **默认推荐** | 当前唯一支持的算法 |

**FastLeaderElection 算法特点**：

- **高效性**：使用 TCP 连接，减少网络开销
- **可靠性**：支持网络分区恢复
- **一致性**：严格按照 `(electionEpoch, peerEpoch, zxid, myid)` 优先级比较
- **兼容性**：自 ZooKeeper 3.6.0 起为唯一可用算法

**配置示例**：

```properties
# zoo.cfg 配置文件
tickTime=2000
initLimit=10
syncLimit=5
electionAlg=3  # 使用 FastLeaderElection 算法
```

---

## 5. 节点宕机场景下的重新选举

### 5.1 场景设置

运行中的集群状态：

- **Leader**：节点 C (myid=3)
- **Follower**：节点 A (myid=1)、节点 B (myid=2)
- **当前 epoch**：6
- **最新 zxid**：0x600000020

### 5.2 Leader 节点宕机

#### 5.2.1 故障检测（t=0s）

**故障发生**：节点 C (Leader) 突然宕机

**节点 A 和 B 的检测过程：**

1. 心跳超时检测（默认 tickTime * syncLimit）
2. 连接断开检测
3. 在 2-4 秒内检测到 Leader 不可达

**节点状态转换：**

- **节点 A**：FOLLOWING → LOOKING
- **节点 B**：FOLLOWING → LOOKING
- **节点 C**：LEADING → [离线]

#### 5.2.2 新一轮选举开始（t=5s）

**选举参数：**

- 新的 electionEpoch：1 (重新开始选举)
- 当前 peerEpoch：6 (保持不变)
- 节点 A：Vote(1, 0x600000020, 6, 1)
- 节点 B：Vote(2, 0x600000020, 6, 1)

**投票交换：**

- A → B: Vote(1, 0x600000020, 6, 1)  
- B → A: Vote(2, 0x600000020, 6, 1)

**比较过程**：A 比较 B 的投票：

- electionEpoch 相同 (1 = 1)
- peerEpoch 相同 (6 = 6)
- zxid 相同 (0x600000020 = 0x600000020)
- myid 比较：2 > 1，A 更新投票为 Vote(2, 0x600000020, 6, 1)

**最终投票：**

- 节点 A：支持节点 B
- 节点 B：支持节点 B  
- 总计：2 票支持节点 B，达到过半数仲裁（2 ≥ 2）

#### 5.2.3 选举完成（t=8s）

**选举结果：**

- 新 Leader：节点 B (myid=2)
- Follower：节点 A (myid=1)

**集群恢复：**

1. 节点 B 转换为 LEADING 状态
2. 节点 A 连接到新 Leader B
3. 数据同步确认
4. 集群恢复正常服务

选举耗时：约 3-8 秒（取决于网络延迟和配置）

### 5.3 时序图总结

Leader 宕机重新选举的完整时序：

```text
时间轴：Leader 宕机重新选举过程

t=0s    [正常运行] A(Follower) ← C(Leader) → B(Follower)
        |
t=1s    [故障发生] A(检测断开) ← C[宕机] → B(检测断开)
        |
t=1-5s  [故障检测] A(FOLLOWING→LOOKING) | B(FOLLOWING→LOOKING)
        |
t=5s    [开始选举] A: Vote(1,0x600000020,6,1) ↔ B: Vote(2,0x600000020,6,1)
        |           A 更新投票支持 B: Vote(2,0x600000020,6,1)
t=8s    [选举完成] A(FOLLOWING) → B(LEADING) | C[离线]
        |
t=60s   [原Leader恢复] A(Follower) → B(Leader) ← C(Follower,降级)
```

**关键时间点：**

- 故障检测：1-5秒
- 重新选举：5-8秒  
- 服务恢复：8秒后
- 原 Leader 恢复：60秒后自动降级为 Follower

### 5.4 原 Leader 节点恢复

#### 5.4.1 节点 C 重新上线（t=60s）

**节点 C 恢复过程：**

1. 从磁盘加载状态：peerEpoch=6, zxid=0x600000020
2. 尝试以 Leader 身份启动
3. 发现集群已有新 Leader (节点 B，peerEpoch=6)
4. 检查当前选举状态，发现自己已不是 Leader
5. 自动降级为 Follower 状态
6. 连接到当前 Leader B
7. 同步最新数据和状态

**最终集群状态：**

- Leader：节点 B (myid=2)  
- Follower：节点 A (myid=1)、节点 C (myid=3)

### 5.4 Follower 节点宕机

#### 5.4.1 单个 Follower 宕机

**场景：节点 A (Follower) 宕机**:

**影响分析：**

- 集群仍有 2 个节点运行（B 为 Leader，C 为 Follower）
- 满足过半数要求（2 ≥ 2）
- 不触发重新选举
- 集群继续正常服务

**节点 A 恢复后：**

1. 自动连接到当前 Leader B
2. 同步缺失的数据
3. 恢复为 Follower 状态

#### 5.4.2 多个 Follower 宕机

**场景：节点 A 和 C (两个 Follower) 同时宕机**:

**影响分析：**

- 仅剩 Leader B 运行
- 不满足过半数要求（1 < 2）
- Leader B 自动转换为 LOOKING 状态
- 集群停止服务，等待节点恢复

**恢复过程：**

1. 至少一个 Follower 恢复上线
2. 满足过半数要求后重新选举
3. 集群恢复正常服务

---

## 6. 总结

ZooKeeper 的 Leader 选举机制是其分布式协调能力的核心基础。通过本文的详细分析，我们可以得出以下关键结论：

### 6.1 核心特点

1. **强一致性保证**：通过过半数仲裁和 epoch 机制确保集群一致性
2. **自动故障恢复**：能够自动检测故障并重新选举 Leader
3. **数据完整性**：优先选择拥有最新数据的节点作为 Leader
4. **简单可靠**：算法逻辑清晰，实现相对简单

### 6.2 技术意义

ZooKeeper 的选举算法为分布式系统的协调服务提供了成熟可靠的解决方案。其设计思想和实现经验对于理解分布式一致性算法、构建高可用系统具有重要的参考价值。随着云原生技术的发展，ZooKeeper 的选举机制仍然是分布式系统设计中的重要组成部分。

---

## 参考文献

[1] Hunt, P., Konar, M., Junqueira, F. P., & Reed, B. (2010). ZooKeeper: Wait-free coordination for Internet-scale systems. *Proceedings of the 2010 USENIX Annual Technical Conference (USENIX ATC '10)*, 145-158. USENIX Association.

[2] Junqueira, F. P., Reed, B. C., & Serafini, M. (2011). Zab: High-performance broadcast for primary-backup systems. *Proceedings of the 2011 IEEE/IFIP 41st International Conference on Dependable Systems & Networks (DSN)*, 245-256. IEEE.

[3] Reed, B., & Junqueira, F. P. (2008). A simple totally ordered broadcast protocol. *Proceedings of the 2nd Workshop on Large-Scale Distributed Systems and Middleware*, 1-6. ACM.

[4] Lamport, L. (1998). The part-time parliament. *ACM Transactions on Computer Systems*, 16(2), 133-169.

[5] Ongaro, D., & Ousterhout, J. (2014). In search of an understandable consensus algorithm. *Proceedings of the 2014 USENIX Annual Technical Conference (USENIX ATC '14)*, 305-319. USENIX Association.

[6] Junqueira, F., & Reed, B. (2013). *ZooKeeper: Distributed Process Coordination*. O'Reilly Media.

[7] 倪超. (2015). *从 Paxos 到 ZooKeeper：分布式一致性原理与实践*. 电子工业出版社.

[8] Kleppmann, M. (2017). *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems*. O'Reilly Media.

[9] Apache Software Foundation. (2024). Apache ZooKeeper Documentation. Retrieved from <https://zookeeper.apache.org/doc/current/>

[10] Apache Software Foundation. (2024). ZooKeeper Administrator's Guide. Retrieved from <https://zookeeper.apache.org/doc/current/zookeeperAdmin.html>

[11] Apache Software Foundation. (2024). ZooKeeper Internals. Retrieved from <https://zookeeper.apache.org/doc/current/zookeeperInternals.html>

### 9.4 技术资源

[12] Apache Software Foundation. (2024). Apache ZooKeeper Wiki. Retrieved from <https://cwiki.apache.org/confluence/display/ZOOKEEPER/>

[13] Apache Software Foundation. (2024). ZooKeeper Source Code Repository. Retrieved from <https://github.com/apache/zookeeper>

[14] MIT EECS. (2024). 6.824: Distributed Systems Course Materials. Retrieved from <https://pdos.csail.mit.edu/6.824/>

---
