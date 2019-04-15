name := "bayes"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq (
  "org.apache.spark" % "spark-core_2.11" % "2.4.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.4.1"
//  "org.apache.spark" % "spark-assembly_2.10" % "1.1.1"
)