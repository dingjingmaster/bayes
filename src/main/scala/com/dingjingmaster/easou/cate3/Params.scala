package com.dingjingmaster.easou.cate3

object Params {
  val HDFS = "hdfs://10.26.26.145:8020/"
  val HDFS_DINGJING = HDFS + "/rs/dingjing/"
  val HDFS_ITEMINFO = HDFS + "/rs/iteminfo/current/"
  val HDFS_CATE3BASE = HDFS_DINGJING + "/category3/"
  val HDFS_CHAPTER = HDFS_DINGJING + "/item_norm/2019-01-28/chapter_info/"
  val HDFS_SVM = HDFS_CATE3BASE + "/svm/"
  val HDFS_CATEGORY_LR_MODEL = HDFS_CATE3BASE + "/lr_model/"
  val HDFS_CATEGORY_BAYES_MODEL = HDFS_CATE3BASE + "/bayes_model/"
}
