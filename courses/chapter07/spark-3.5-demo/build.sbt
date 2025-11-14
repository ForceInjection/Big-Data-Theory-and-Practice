// 项目设置
name := "spark-3.5-demo"
version := "1.0"
scalaVersion := "2.13.12"

// Spark 依赖
val sparkVersion = "3.5.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

// 打包配置
Compile / packageBin / mainClass := Some("SparkShellDemo")
Compile / packageBin / packageOptions += Package.ManifestAttributes(
  "Main-Class" -> "SparkShellDemo"
)