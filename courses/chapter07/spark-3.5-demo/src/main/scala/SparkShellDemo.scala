import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import java.io.File

object SparkShellDemo {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("SparkShellDemo")
      .config("spark.master", "local[2]")
      .getOrCreate()
    
    val sc = spark.sparkContext
    
    println("=== Spark Shell 快速体验 Demo ===")
    
    // 1. 创建和操作 RDD
    println("\n1. 创建和操作 RDD:")
    val numbers = sc.parallelize(1 to 100)
    println(s"分区数: ${numbers.getNumPartitions}")
    
    val evenNumbers = numbers.filter(_ % 2 == 0)
    val squares = evenNumbers.map(x => x * x)
    
    val result = squares.take(10)
    println(s"前10个偶数的平方: ${result.mkString(", ")}")
    
    val count = evenNumbers.count()
    val sum = evenNumbers.reduce(_ + _)
    println(s"偶数个数: $count, 偶数和: $sum")
    
    // 2. 文本处理实战
    println("\n2. 文本处理实战 (WordCount):")
    
    // 创建示例文本文件
    val sampleText = "Hello Spark Hello World\nSpark is awesome\nHello Big Data"
    val textFile = new File("data/sample.txt")
    val writer = new java.io.PrintWriter(textFile)
    writer.write(sampleText)
    writer.close()
    
    val textRDD = sc.textFile("data/sample.txt")
    
    val wordCounts = textRDD
      .flatMap(_.split("\\s+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    
    println("词频统计结果:")
    wordCounts.take(10).foreach(println)
    
    // 3. 数据缓存演示
    println("\n3. 数据缓存演示:")
    
    val expensiveRDD = sc.parallelize(1 to 1000)
      .map(x => {
        // 模拟耗时操作
        Thread.sleep(1)
        x * x
      })
    
    // 第一次计算
    val start1 = System.currentTimeMillis()
    val result1 = expensiveRDD.filter(_ > 500).count()
    val time1 = System.currentTimeMillis() - start1
    println(s"第一次计算耗时: ${time1}ms, 结果: $result1")
    
    // 缓存 RDD
    expensiveRDD.persist(StorageLevel.MEMORY_ONLY)
    expensiveRDD.count() // 触发缓存
    
    // 第二次计算
    val start2 = System.currentTimeMillis()
    val result2 = expensiveRDD.filter(_ > 800).count()
    val time2 = System.currentTimeMillis() - start2
    println(s"缓存后计算耗时: ${time2}ms, 结果: $result2")
    
    // 4. RDD 血缘关系查看
    println("\n4. RDD 血缘关系查看:")
    
    val complexRDD = sc.parallelize(1 to 100)
      .map(_ * 2)
      .filter(_ > 50)
      .map(_ + 1)
    
    println("RDD 血缘关系:")
    println(complexRDD.toDebugString)
    
    println("\n依赖关系:")
    complexRDD.dependencies.foreach(dep =>
      println(s"依赖类型: ${dep.getClass.getSimpleName}")
    )
    
    // 5. 存储级别演示
    println("\n5. 存储级别演示:")
    
    val criticalData = sc.parallelize(1 to 1000)
      .filter(_ % 100 == 0)
    
    criticalData.persist(StorageLevel.MEMORY_AND_DISK_2)
    println("使用 MEMORY_AND_DISK_2 存储级别")
    
    criticalData.unpersist()
    criticalData.persist(StorageLevel.OFF_HEAP)
    println("使用 OFF_HEAP 存储级别 (需要配置 spark.memory.offHeap.enabled=true)")
    
    println("\n=== Demo 完成 ===")
    
    spark.stop()
  }
}