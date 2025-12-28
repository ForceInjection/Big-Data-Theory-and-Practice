# Watermark 水印：直观解释与示例

## 1. 背景与理论关联

- 本文作为课程补充材料，直观讲解 **Watermark（事件时间水印）** 如何在 **乱序数据** 下 **推动窗口进度与收敛**，并与 **Allowed Lateness（允许延迟）**、**Side Output（侧输出）** 协同 **处理迟到与超迟到事件**。
- **理论参考**：参阅 [从 ETL 到流式计算入门](./从ETL到流式计算入门.md#2312-时间语义处理时间-vs-事件时间)，涵盖事件时间、处理时间的形式化建模与水印机制的理论依据。

动机与风险概述

- **背景**：真实链路存在网络抖动、设备时钟误差、异步采集与多源合并（如 Kafka 多 Topic Join），事件到达顺序与发生顺序常不一致；批/流一体要求离线重跑与准实时监控口径一致。
- **需求**：
  - 明确窗口何时能安全关闭并产出结果（避免无限等待或过早关窗）。
  - 保持事件时间口径一致、可复算（与离线批处理结果一致）。
  - 控制状态保留与清理成本，避免状态膨胀。
  - 对迟到与超迟到事件有清晰可控的处理策略（更新/侧输出）。
- **若不使用水印的风险**：
  - 关窗不可判定（过早丢数据或长期不产出，导致 SLA 失效）。
  - 口径不一致（处理时间窗口受系统负载影响，难以复现，无法与事件时间口径对齐）。
  - 状态失控（缺少清理边界导致状态膨胀，影响稳定性与成本）。
  - 多源合并异常（无统一进度导致频繁重算或卡住，结果抖动）。
  - 更新不可控（迟到更新无规范，下游幂等与事务落地困难）。

## 2. 核心概念速览

- **事件时间**（Event Time）：事件在源系统发生的业务时间。
- **处理时间**（Processing Time）：事件在计算引擎实际处理的系统时间。
- **摄入时间**（Ingestion Time）：事件被采集并进入数据系统的时间。
- **水印**（Watermark）：计算引擎对「事件时间进度」的保守估计，常见定义为 `maxObservedEventTime - 延迟上界`。
- **允许延迟**（Allowed Lateness， AL）：窗口在被水印判定可关闭后，仍可接受迟到事件进行补充更新的额外时间。
- **侧输出**（Side Output）：超过允许延迟的迟到事件不再进入主窗口的结果通道，而是进入单独的输出以供审计或补偿处理。

## 3. 乱序事件与窗口收敛示例

**1. 场景设定**：

- 窗口大小 5 分钟，窗口 `W1 = [10:00:00, 10:05:00)`；乱序延迟上界 `L = 2 分钟`（水印策略）；允许延迟 `AL = 1 分钟`。
- 事件到达序列（括号内为事件时间）：
  - E01（事件时间 10:01:00）在 10:01:10 到达（正常）
  - E03（事件时间 10:03:00）在 10:04:20 到达（乱序：到达顺序与事件时间顺序不一致）
  - EX（事件时间 10:07:00）在 10:07:10 到达（后续事件：推进 Watermark 触发关窗）
  - E02（事件时间 10:02:00）在 10:07:20 到达（迟到：关窗后 AL 内补充）
  - 标记规则：`E01/E02/E03` 按事件时间升序编号；`EX` 表示用于演示的后续事件（事件时间在当前窗口结束之后，用于推进 Watermark 触发关窗）。
  - 简洁判定：
    **乱序** 指到达顺序与事件时间顺序不一致；
    **迟到** 指到达时 `Watermark ≥ 窗口结束时间` 且仍在 `AL` 内；
    **超迟到** 指超过 `AL`，进入 `Side Output`。
- **`maxEvt`**：当前已观测到的最大事件时间（按事件时间比较）。
- **`Watermark`**：系统对事件时间进度的保守估计，常取 `maxEvt - L`，只前进不后退。
- **`W1`**：窗口区间 `10:00:00-10:05:00`，采用左闭右开语义（含 10:00:00，不含 10:05:00）。
- **`AL`**：允许延迟时长（窗口初次关闭后，仍接受迟到事件更新的时间）。

**2. 水印推进与窗口行为规则**：

- **关闭规则**：当 `Watermark ≥ 窗口结束时间` 时，触发窗口初次计算与关闭。
- **更新规则**：在 `AL` 时间内到达的迟到事件，对已关闭窗口进行补充更新（可能产生更新/撤回与重算输出）。
- **清理规则**：当 `Watermark > 窗口结束时间 + AL` 时，窗口最终定版并清理状态；超迟到事件进入 `Side Output`。
- **多源合并**：多输入场景下，系统 Watermark 取各输入流 Watermark 的最小值，以确保安全推进。

**3. 事件时间最大值推进与水印（逐步演示）**：

1. 当前摄入时间 10:01:10，观测到 E01（事件时间 10:01:00）

   - `maxEvt = 10:01:00`，`Watermark = maxEvt - L = 10:01:00 - 2 分钟 = 9:59:00`
   - W1 (10:00:00-10:05:00) 尚未关闭（`Watermark < 10:05:00`）。

2. 当前摄入时间 10:04:20，观测到 E03（事件时间 10:03:00）

   - `maxEvt = 10:03:00`，`Watermark = 10:03:00 - 2 分钟 = 10:01:00`
   - W1 仍未关闭（`Watermark < 10:05:00`）。

3. 当前摄入时间 10:07:10，观测到 EX（事件时间 10:07:00）

   - `maxEvt = 10:07:00`，`Watermark = 10:07:00 - 2 分钟 = 10:05:00`
   - 触发 W1 初次计算（`Watermark ≥ 10:05:00`）；窗口保持开启，进入允许延迟期（AL = 1 分钟）。

4. 当前摄入时间 10:07:20，观测到 E02（事件时间 10:02:00）

   - `maxEvt 仍为 10:07:00`，`Watermark = 10:07:00 - 2 分钟 = 10:05:00`，仍处于 `AL` 内，W1 允许进行补充更新（更新聚合/重新输出，带上更新标识；下游需采用幂等/事务，避免重复影响）。

5. 后续摄入事件使 `Watermark > 10:05:00 + AL`（例如 `maxEvt` 升至 10:08:10 → `Watermark = 10:06:10 > 10:06:00 (= 10:05:00 + AL)`）
   - W1 最终定版并清理状态；此后到达的更迟事件进入 `Side Output`，不再影响主结果口径与报表一致性。

**提示**：

- **触发与关闭区别**：
  - **首次触发**：`Watermark ≥ 窗口结束时间` 时，触发窗口计算并输出结果，但**保留状态**。
  - **彻底关闭**：`Watermark ≥ 窗口结束时间 + AL` 时，**清理状态**并关闭窗口（后续数据入侧输出）。
- **Watermark 停滞风险**：Watermark 推进依赖**新数据的事件时间**，与**系统时间**无关。
  - **现象**：若数据源断流，Watermark 停止推进，导致窗口永远无法关闭（即使系统时间已过很久）。
  - **解决**：生产环境需配置 **空闲源策略（Idle Source Policy）**，在无数据时强制推进 Watermark。
- **数据一致性**：AL 期间的更新输出应带版本标识，配合下游的幂等写入或事务提交，避免重复计算导致的数据错误。

## 4. Flink 示例（事件时间 + 水印 + 允许延迟）

```scala
// 示例环境：Flink DataStream API（Scala）
// 目标：按用户 ID 与 5 分钟事件时间窗口聚合，允许 1 分钟延迟，超迟到事件走侧输出。

case class UserEvent(userId: String, eventTime: java.time.Instant, value: Long)

val watermarkStrategy =
  WatermarkStrategy
    .forBoundedOutOfOrderness(java.time.Duration.ofMinutes(2)) // 乱序延迟上界 L = 2m
    .withTimestampAssigner((e: UserEvent, _: Long) => e.eventTime.toEpochMilli)

val input: DataStream[UserEvent] = env
  .fromSource(kafkaSource, watermarkStrategy, "user-events") // 绑定水印策略

val lateTag = new OutputTag[UserEvent]("late-events") {}

val result = input
  .keyBy(_.userId)
  .window(TumblingEventTimeWindows.of(Time.minutes(5))) // 5 分钟滚动窗口
  .allowedLateness(Time.minutes(1))                      // 允许延迟 AL = 1m
  .sideOutputLateData(lateTag)                           // 超迟到走侧输出
  .reduce(
    (a, b) => UserEvent(a.userId, a.eventTime, a.value + b.value)
  )

val mainSink = result.addSink(/* 生产库 Sink：幂等或两阶段提交 */)
val lateSink = result.getSideOutput(lateTag).addSink(/* 侧输出：审计或补偿渠道 */)
```

要点说明：

- `forBoundedOutOfOrderness` 采用「有界乱序」水印策略，设置乱序延迟上界（通常通过历史时延分布估计）。
- `allowedLateness` 与 `sideOutputLateData` 协同，确保窗口结果既能在合理迟到范围内更新，又能对超迟到事件进行隔离与补偿处理。
- Sink 层需采用两阶段提交或幂等写，以实现端到端的精确一次语义。

## 5. Spark 示例（Structured Streaming 水印）

```scala
// 目标：在 Spark Structured Streaming 中基于事件时间窗口统计，
//       设置 watermark 与窗口，处理迟到数据。

val spark = SparkSession.builder().appName("WatermarkDemo").getOrCreate()
import spark.implicits._

val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "k1:9092,k2:9092")
  .option("subscribe", "user-events")
  .load()
  .selectExpr("CAST(value AS STRING)")
  .as[String]
  .map(parseToEvent) // 将 JSON 解析为包含 event_time 的行

val withWatermark = df
  .withWatermark("event_time", "2 minutes") // 乱序延迟上界 L = 2m

val agg = withWatermark
  .groupBy(
    window($"event_time", "5 minutes"),
    $"user_id"
  )
  .agg(sum($"value").as("sum_value"))

val query = agg.writeStream
  .outputMode("update")
  .option("checkpointLocation", "/chk/watermark-demo")
  .format("console")
  .start()
```

要点说明：

- `withWatermark` 声明事件时间列的水印延迟上界；当水印超过窗口结束时间时，窗口将被清理，减少状态占用。
- Spark 的迟到数据处理与 Flink 的 `allowedLateness` 机制有所差异，需结合具体业务场景验证窗口更新与数据保留策略。

## 6. 工程实践建议

- 观测与建模：通过延迟分布（P50/P95/P99）选择水印延迟上界；不同 Topic/业务键可采用差异化策略，且需按延迟分布动态调整。
- 口径一致性：使用事件时间语义定义所有窗口与指标；统一时区与时间精度（UTC、毫秒）。
- 延迟与成本：`allowedLateness` 越大，状态保留时间越长；需结合状态后端能力与成本优化。
- 容错与一致性：与检查点、两阶段提交/幂等写协同，确保端到端精确一次；侧输出纳入审计与补偿流程。
- 监控与告警：持续监控水印推进、状态大小、迟到率与重算率，动态调整参数。

## 7. 常见误区与对策

- 误区：水印越保守越安全。对策：过度保守将显著拖慢窗口收敛与结果产出；建议按延迟分布分层配置。
- 误区：允许延迟只是“再等一会”。对策：需要明确补充更新的语义与下游可见性，标注更新标识并做好幂等写。
- 误区：事件时间只在实时场景重要。对策：离线重跑与准实时分析同样应基于事件时间，保证口径一致与可复算性。

## 8. 参考文献

[1] Apache Flink. "Event Time and Watermarks." Flink Documentation. Accessed: Dec. 12, 2025. [Online]. Available: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/watermarks/

[2] Apache Spark. "Handling late data and watermarking." Spark Structured Streaming Programming Guide. Accessed: Dec. 12, 2025. [Online]. Available: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking

[3] Chandy, K. M., & Lamport, L. "Distributed Snapshots: Determining Global States of Distributed Systems." ACM Transactions on Computer Systems, vol. 3, no. 1, pp. 63–75, 1985.

> 说明：本文中的示例数据与时间设定为教学示例，旨在说明水印推进、窗口关闭与迟到处理的工程机制，并非真实生产指标。
