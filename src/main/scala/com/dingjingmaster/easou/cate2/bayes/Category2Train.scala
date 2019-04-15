package com.dingjingmaster.easou.cate2.bayes

import org.apache.spark.{SparkConf, SparkContext}

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
                  .map(x => (x(0), x(4) + "\t" + x(10) + "\t" + x(18) + "\t" + x(20)))
    /* 读取章节信息 */
    val chapterRDD = sc.textFile(chapterPath)
                  .map(x => x.split("\\t"))
                  .filter(x => x.length == 3)
                  .map(x => (x(0), x(2)))
    val allItemInfoRDDt = chapterRDD.join(itemInfoRDD)
    val allItemInfoRDD = allItemInfoRDDt.map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._1)
    allItemInfoRDD.saveAsTextFile(HDFS_DINGJING + "category2/base_info/")
  }

  private val HDFS = "hdfs://10.26.26.145:8020/"
  private val HDFS_DINGJING = "hdfs://10.26.26.145:8020/rs/dingjing/"
  private val HDFS_ITEMINFO = "hdfs://10.26.26.145:8020/rs/iteminfo/current/"
}
