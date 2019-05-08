package com.dingjingmaster.easou.cate3

import com.dingjingmaster.easou.cate3.Params.HDFS_SVM
import com.dingjingmaster.easou.cate3.Params.HDFS_CATEGORY_LR_MODEL

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}

object LRTraining {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
                  .setAppName("category3_lr_training")
                  .set("spark.executor.memory", "20g")
                  .set("spark.driver.memory", "4g")
                  .set("spark.cores.max", "30")
                  .setMaster("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
    val sc = new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, HDFS_SVM)
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1).cache()
    val classNum = data.map(x=>(x.label, 1)).reduceByKey((x, y) => x + y).count()
    println("\n\n\n共有分类：" + classNum.toString + "\n\n\n")
    val model = new LogisticRegressionWithLBFGS()
                  .setNumClasses(classNum.toInt)
                  .run(training)
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
    model.save(sc, HDFS_CATEGORY_LR_MODEL)
  }
}
