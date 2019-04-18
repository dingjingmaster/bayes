package com.dingjingmaster.easou.cate2.bayes
import scala.collection.Map

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

object Category2Train {
  def main(args: Array[String]): Unit = {
    val dateStr = "2019-04-15"
    val chapterPath = this.HDFS + "/rs/dingjing/item_norm/2019-01-28/chapter_info/"

    val conf = new SparkConf()
                  .setAppName("bayes_category2")
                  .set("spark.executor.memory", "10g")
                  .set("spark.driver.memory", "4g")
                  .set("spark.cores.max", "10")
                  .setMaster("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
    val sc = new SparkContext(conf)

    /* 读取物品数据集 */
    val itemInfoRDD = sc.textFile(HDFS_ITEMINFO)
                  .map(x => x.split("\\t"))
                  .filter(x => x.length >= 21)
                              /* gid, norm_name \t  norm_author \t tag1 \t tag2 \t fee_flag */
                  .map(x => (x(0), x(4) + "\t" + x(10) + "\t" + x(18) + "\t" + x(20) + "\t" + x(30)))
    /* 读取章节信息 */
    val chapterRDD = sc.textFile(chapterPath)
                  .map(x => x.split("\\t"))
                  .filter(x => x.length == 3)
                            /* gid, chapter */
                  .map(x => (x(0), x(2)))
    val allItemInfoRDDt = chapterRDD.join(itemInfoRDD)
    var allItemInfoRDD = allItemInfoRDDt.map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._1)
                  .map(x => x.split("\\t"))
                  .filter(x => x(5) == "1")
                            /* tag1.2, (name, author) */
                  .map(x => (x(3), (x(1), x(2))))
    allItemInfoRDD.map(x => x._1 + "\t" + x._2._1 + "\t" + x._2._2)
          .saveAsTextFile(HDFS_DINGJING + "/category2/" + dateStr + "/debug")
    /* 处理 tag 目标 */
    val targetG = sc.broadcast(allItemInfoRDD.map(x => x._1)
                  .map(detail_tag12)
                  .filter(x => x != "")
                  .map(x => (x, 1))
                  .reduceByKey((x, y) => x + y)
                  .map(x => x._1)
                  .zipWithIndex()
                  .map(x => (x._1, x._2.toString))
                  .collectAsMap())
    allItemInfoRDD = allItemInfoRDD.map(x => (detail_tag12(x._1), (x._2._1, x._2._2)))
                  .filter(x => x._1 != "")
                  .map(x => norm_target(x, targetG.value))
    targetG.unpersist()
    /* 处理书名 */
    val normNameG = sc.broadcast(allItemInfoRDD.map(x => (x._2._1, 1))
                    .reduceByKey((x, y) => x + y)
                    .map(x => x._1)
                    .zipWithIndex()
                    .map(x => (x._1, x._2.toString))
                    .collectAsMap())
    allItemInfoRDD = allItemInfoRDD.map(x => detail_name(x, normNameG.value))
    normNameG.unpersist()
    /* 处理作者名 */
    val authorG = sc.broadcast(allItemInfoRDD.map(x => (x._2._2, 1))
                    .reduceByKey((x, y) => x + y)
                    .map(x => x._1)
                    .zipWithIndex()
                    .map(x => (x._1, x._2.toString))
                    .collectAsMap())
    allItemInfoRDD = allItemInfoRDD.map(x => detail_author(x, authorG.value))
    authorG.unpersist()

    val finallyRDD = allItemInfoRDD


    finallyRDD.map(x => x._1 + " 1:" + x._2._1 + " 2:" + x._2._2)
                    .repartition(1)
                    .saveAsTextFile(HDFS_DINGJING + "/category2/" + dateStr + "/dataSet.svm")
    val dataRDD = MLUtils.loadLibSVMFile(sc, HDFS_DINGJING + "/category2/" + dateStr + "/dataSet.svm")
    val Array(training, test) = dataRDD.randomSplit(Array(0.6, 0.4))
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = test.map(x => (model.predict(x.features), x.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    model.save(sc, HDFS_DINGJING + "/category2/" + dateStr + "/model")

    print("\n\n\n------------------------------------------------------\n\n\n")
    print("准确率: " + accuracy.toString)
    sc.parallelize(accuracy.toString).repartition(1).
                    saveAsTextFile(HDFS_DINGJING + "/category2/" + dateStr + "/accuracy")
    print("\n\n\n------------------------------------------------------\n\n\n")
  }

  def detail_tag12(x: String): String = {
    var tg = ""
    val arr = x.split("\\,")
    if (arr.length >= 2) {
      tg = arr(1)
    }
    return tg
  }

  def norm_target(x: Tuple2[String, Tuple2[String,String]], mp: Map[String, String]):
                        Tuple2[String, Tuple2[String,String]] = {
    var tag = "-1"
    if (mp.contains(x._1)) {
      tag = mp(x._1)
    }
    return (tag, (x._2._1, x._2._2))
  }

  def detail_name(x: Tuple2[String, Tuple2[String,String]], mp: Map[String, String]):
                        Tuple2[String, Tuple2[String,String]] = {
    var name = "-1"
    if (mp.contains(x._2._1)) {
      name = mp(x._2._1)
    }
    return (x._1, (name, x._2._2))
  }

  def detail_author(x: Tuple2[String, Tuple2[String, String]], mp: Map[String, String]):
                          Tuple2[String, Tuple2[String, String]] = {
    var author = "-1"
    if (mp.contains(x._2._2)) {
      author = mp(x._2._2)
    }
    return (x._1, (x._2._1, author))
  }

  private val HDFS = "hdfs://10.26.26.145:8020/"
  private val HDFS_DINGJING = "hdfs://10.26.26.145:8020/rs/dingjing/"
  private val HDFS_ITEMINFO = "hdfs://10.26.26.145:8020/rs/iteminfo/current/"
}
