package com.dingjingmaster.easou.cate3

import com.dingjingmaster.easou.cate3.Params.{HDFS_CATEGORY_BAYES_MODEL, HDFS_SVM}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object BayesTraining {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
                  .setAppName("category3_bayes_training")
                  .set("spark.executor.memory", "20g")
                  .set("spark.driver.memory", "6g")
                  .set("spark.cores.max", "30")
                  .set("spark.dynamicAllocation.enabled", "false")
                  .setMaster("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
    val sc = new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, HDFS_SVM)
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("精确度：" + accuracy.toString)

    model.save(sc, HDFS_CATEGORY_BAYES_MODEL)
  }
}
