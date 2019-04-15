package com.dingjingmaster.easou.cate2.bayes

import org.apache.spark.{SparkConf, SparkContext}

//import com.huaban.analysis.jieba.JiebaSegmenter
//import com.huaban.analysis.jieba.JiebaSegmenter.SegMode

object Category2Train {
  def main(args: Array[String]): Unit = {
    val dateStr = "2019-04-15"
    val chapterPath = this.HDFS + "/rs/dingjing/item_norm/2019-01-28/chapter_info/"

    val conf = new SparkConf()
                  .setAppName("bayes_category2")
                  .setMaster("local")
    val sc = new SparkContext(conf)

    /* 读取物品数据集 */
    val itemInfoRDD = sc.textFile(HDFS_ITEMINFO)
                  .map(x => x.split("\\t"))
                  .filter(x => x.length >= 21)
                  .map(x => (x(0), x(4) + "\t" + x(10) + "\t" + x(18) + "\t" + x(20) + "\t" + x(30)))
    /* 读取章节信息 */
    val chapterRDD = sc.textFile(chapterPath)
                  .map(x => x.split("\\t"))
                  .filter(x => x.length == 3)
                  .map(x => (x(0), x(2)))
    val allItemInfoRDDt = chapterRDD.join(itemInfoRDD)
    val allItemInfoRDD = allItemInfoRDDt.map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._1)
    allItemInfoRDD.saveAsTextFile(HDFS_DINGJING + "category2/" + dateStr + "/base_info/")
    //val allItemInfoRDD = sc.textFile(HDFS_DINGJING + "category2/" + "/base_info/")
    /* 处理书名 */
    val normNameRDD = allItemInfoRDD.map(x => x.split("\\t"))
                    .map(x => (x(1), 1))
                    .reduceByKey((x, y) => x + y)
                    .map(x => x._1 + "\t" + x._2)
                    .zipWithIndex()
                    .map(x => x._2 + "\t" + x._1)
    normNameRDD.saveAsTextFile(HDFS_DINGJING + "category2/" + "/name/")
    /* 处理作者名 */
    val AuthorRDD = allItemInfoRDD.map(x => x.split("\\t"))
                    .map(x => (x(2), 1))
                    .reduceByKey((x, y) => x + y)
                    .map(x => x._1 + "\t" + x._2)
                    .zipWithIndex()
                    .map(x => x._2 + "\t" + x._1)
    AuthorRDD.saveAsTextFile(HDFS_DINGJING + "category2/" + "/author/")
    /* 处理tag */
    val tagRDD = allItemInfoRDD.map(x => x.split("\\t"))
                    .map(x => x(4))
                    .flatMap(x => x.split(","))
                    .map(x => (x, 1))
                    .reduceByKey((x, y) => x + y)
                    .map(x => x._1 + "\t" + x._2)
                    .zipWithIndex()
                    .map(x => x._2 + "\t" + x._1)
    tagRDD.saveAsTextFile(HDFS_DINGJING + "category2/" + "/tag/")
    /* 处理一级分类 */
    val category1RDD = allItemInfoRDD.map(x => x.split("\\t"))
                    .map(x => x(3).split(","))
                    .filter(x => x.length >= 1)
                    .map(x => (x(0), 1))
                    .reduceByKey((x, y) => x + y)
                    .map(x => x._1 + "\t" + x._2)
                    .zipWithIndex()
                    .map(x => x._2 + "\t" + x._1)
    category1RDD.saveAsTextFile(HDFS_DINGJING + "category2/" + "/cate1/")
    /* 处理章节信息 */

    /* 准备训练集和测试集 */
  }

  private val HDFS = "hdfs://10.26.26.145:8020/"
  private val HDFS_DINGJING = "hdfs://10.26.26.145:8020/rs/dingjing/"
  private val HDFS_ITEMINFO = "hdfs://10.26.26.145:8020/rs/iteminfo/current/"
}
