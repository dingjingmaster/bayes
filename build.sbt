name := "regression"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
  "org.apache.spark" % "spark-core_2.11" % "2.4.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.4.1",
//  "com.huaban" % "jieba-analysis" % "1.0.2"
)
unmanagedJars in Compile ++= Seq(
  Attributed.blank[File](file(baseDirectory.value + "/lib/jieba-analysis-1.0.2.jar"))
)