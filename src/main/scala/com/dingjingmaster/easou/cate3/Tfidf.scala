package com.dingjingmaster.easou.cate3

import scala.collection.Map
import com.dingjingmaster.easou.cate3.Params.HDFS
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.SparseVector

object Tfidf {
  def main(args: Array[String]): Unit = {
    val trainPath = HDFS + "/rs/dingjing/category3/word/"
    val tfidfPath = HDFS + "/rs/dingjing/category3/tfidf/"
    val svmPath = HDFS + "/rs/dingjing/category3/svm/"

    val ss = SparkSession.builder()
                      .appName("category3_vector")
                      .config("spark.executor.memory", "10g")
                      .config("spark.driver.memory", "4g")
                      .config("spark.cores.max", "30")
                      .master("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
                      .getOrCreate()
    import ss.implicits._
    val wordDF = ss.sparkContext.textFile(trainPath)
                      .map(x => x.split("\t"))
                      .filter(x => (x(1) == "1") && (x(2) != ""))
                      .map(x => (x(0), x(2), x(3)))
                      .map(x => DataTrans(x._1, x._2, x._3.replace("{]", " ")))
                      .toDF("gid", "cate", "sentence")
    val tokenizerDF = new Tokenizer().setInputCol("sentence").setOutputCol("words").transform(wordDF)
    val hashTFDF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(600000).transform(tokenizerDF)
    val featureDF = new IDF().setInputCol("rawFeatures").setOutputCol("features").fit(hashTFDF).transform(hashTFDF)
    val saveRDD = featureDF.rdd.map(save_vector)
    val cateG = ss.sparkContext.broadcast(
      saveRDD.map(x => (x._2, 1)).reduceByKey((x, y) => x + y).map(_._1).zipWithIndex().map(x => (x._1.toString, x._2.toString)).collectAsMap()
    )
    saveRDD.map(x => change_tag(x, cateG.value)).saveAsTextFile(svmPath)
    saveRDD.map(x => x._1 + "\t" + x._2 + "\t" + x._3 + "\t" + x._4.toString()).saveAsTextFile(tfidfPath)
  }
  def change_tag(x: Tuple4[String, String, String, Any], mp: Map[String, String]) = {
    val cate = x._2
    val feature = x._4.asInstanceOf[SparseVector]
    var tag = "9999"
    var values = ""
    if (mp.contains(cate)) {
      tag = mp(cate)
    }
    val index = feature.indices
    val value = feature.values
    for (i <- 0 until index.length) {
      values += "" + (index(i) + 1).toString + ":" + value(i).toString + " "
    }
    values = values.trim

    tag + " " + values
  }
  def save_vector(x: Row): Tuple4[String, String, String, Any] = {
    val gid = x.getString(0)
    val cate = x.getString(1)
    val sentence = x.getString(2)
    val feature = x.get(5)

    (gid, cate, sentence, feature)
  }

  case class DataTrans(gid: String, cate3: String, words:String)
}
