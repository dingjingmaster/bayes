package com.dingjingmaster.ml.bayes.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

/**
  *  数据集信息
  *  根据人口普查数据预测年收入是否超过5万美元/年。
  *  第一列：年龄（连续）
  *  第二列：工作类型（离散）Private、 Self - emp - not - inc、 Self - emp - inc、 Federal - gov、 Local - gov
  *                          State - gov、 Without - pay、 Never - worked
  *  第三列：一个州内数据观察代表的人数（连续）
  *  第四列：学历（离散）Bachelors、 Some-college、 11th、 HS-grad、 Prof-school、 Assoc-acdm、 Assoc-voc
  *                          9th、 7th-8th、 12th、 Masters、 1st-4th、 10th、 Doctorate、 5th-6th、 Preschool
  *  第五列：education-num（连续）
  *  第六列：婚姻状况（离散）Married - civ - spouse、 Divorced、 Never - married、 Separated、 Widowed
  *                          Married - spouse - absent、 Married - AF - spouse
  *  第七列：职业（离散）Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty,
  *                      Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving,
  *                      Priv-house-serv, Protective-serv, Armed-Forces.
  *  第八列：家庭关系（离散）Wife、 Own - child、  Husband、  Not - in - family、 Other - relative、  Unmarried
  *  第九咧：种族（离散）White、  Asian - Pac - Islander、  Amer - Indian - Eskimo、  Other、 Black
  *  第十咧：性别（离散）Female、 Male.
  *  十一列：capital-gain（连续）
  *  十二列：capital-loss（连续）
  *  十三咧：每周工作时长（连续）
  *  十四列：祖国（离散）United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc),
  *                      India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica,
  *                      Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti,
  *                      Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador,
  *                      Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
  *  十五列：年收入是否超过 50K（离散） >=50K <50k
  *
  *  数据集来源： http://archive.ics.uci.edu/ml/datasets/Adult
  */
object Bayes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
                .setAppName("bayes")
                .setMaster("local")
    val sc = new SparkContext(conf)

    // 读取数据集
    sc.textFile(this.HDFS_PATH + "/dataset/adult.data")
                .filter(x => x != "")
                .map(transToSVM)
                .filter(x => x != "")
                .saveAsTextFile(this.HDFS_PATH + "/bayes/adult.svm")

    val dataRDD = MLUtils.loadLibSVMFile(sc, this.HDFS_PATH + "/bayes/adult.svm")

    val Array(training, test) = dataRDD.randomSplit(Array(0.1, 0.9))
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = test.map(x => (model.predict(x.features), x.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    model.save(sc, this.HDFS_PATH + "bayes/model/naiveBayesModel")

    print("\n\n\n------------------------------------------------------\n\n\n")
    print("准确率: " + accuracy.toString)
    print("\n\n\n------------------------------------------------------\n\n\n")

    //val bayesModel = NaiveBayesModel.load(sc, "model/naiveBayesModel")
  }

  def transToSVM(str: String): String = {
    val x = str.split(",")
    if (x.length != 15) {
      return ""
    }

    var target = ""               // 分类结果
    var workClasst = ""           // 第二列
    var eduactiont = ""           // 第四列
    var maritalStatust = ""       // 第六列
    var occupationt = ""          // 第七列
    var relationshipt = ""        // 第八列
    var racet = ""                // 第九咧
    var sext = ""                 // 第十咧
    var nativeCountryt = ""       // 第十四列

    var tmp = x(14).trim()
    if (tmp == "<=50K"){target = "1"}else{target = "0"}
    tmp = x(1).trim()
    if(workClass.contains(tmp)){workClasst = workClass(tmp)}else{workClasst = "0"}
    tmp = x(3).trim()
    if(education.contains(tmp)){eduactiont = education(tmp)}else{eduactiont = "0"}
    tmp = x(5).trim()
    if(maritalStatus.contains(tmp)){maritalStatust = maritalStatus(tmp)}else{maritalStatust = "0"}
    tmp = x(6).trim()
    if(occupation.contains(tmp)){occupationt = occupation(tmp)}else{occupationt = "0"}
    tmp = x(7).trim()
    if(relationship.contains(tmp)){relationshipt = relationship(tmp)}else{relationshipt = "0"}
    tmp = x(8).trim()
    if(race.contains(tmp)){racet = race(tmp)}else{racet = "0"}
    tmp = x(9).trim()
    if(sex.contains(tmp)){sext = sex(tmp)}else{sext = "0"}
    tmp = x(13).trim()
    if(nativeCountry.contains(tmp)){nativeCountryt = nativeCountry(tmp)}else{nativeCountryt = "0"}

    return target + " 1:" + x(0) + " 2:" + workClasst + " 3:" + x(2).trim() +
            " 4:" + eduactiont + " 5:" + x(4).trim() + " 6:" + maritalStatust +
            " 7:" + occupationt + " 8:" + relationshipt + " 9:" + racet +
            " 10:" + sext + " 11:" + x(10).trim() + " 12:" + x(11).trim() + " 13:" + x(12).trim() +
            " 14:" + nativeCountryt
  }

  private val workClass = Map[String, String] (
    "Private" -> "1", "Self-emp-not-inc" -> "2",
    "Self-emp-inc" -> "3", "Federal-gov" -> "4",
    "Local-gov" -> "5", "State-gov" -> "6",
    "Without-pay" -> "7", "Never-worked" -> "8"
  )

  private val education = Map[String, String] (
    "Bachelors" -> "1",
    "Some-college" -> "2",
    "11th" -> "3",
    "HS-grad" -> "4",
    "Prof-school" -> "5",
    "Assoc-acdm" -> "6",
    "Assoc-voc" -> "7",
    "9th" -> "8",
    "7th-8th" -> "9",
    "12th" -> "10",
    "Masters" -> "11",
    "1st-4th" -> "12",
    "10th" -> "13",
    "Doctorate" -> "14",
    "5th-6th" -> "15",
    "Preschool" -> "16"
  )

  private val maritalStatus = Map[String, String] (
    "Married-civ-spouse" -> "1",
      "Divorced" -> "2",
      "Never-married" -> "3",
      "Separated" -> "4",
      "Widowed" -> "5",
      "Married-spouse-absent" -> "6",
      "Married-AF-spouse" -> "7"
  )

  private val occupation = Map[String, String] (
    "Tech-support" -> "1",
     "Craft-repair" -> "2",
      "Other-service" -> "3",
      "Sales" -> "4",
      "Exec-managerial" -> "5",
      "Prof-specialty" -> "6",
      "Handlers-cleaners" -> "7",
      "Machine-op-inspct" -> "8",
      "Adm-clerical" -> "9",
      "Farming-fishing" -> "10",
      "Transport-moving" -> "11",
      "Priv-house-serv" -> "12",
      "Protective-serv" -> "13",
      "Armed-Forces" -> "14"
  )

  private val relationship = Map[String, String] (
    "Wife" -> "1",
      "Own-child" -> "2",
      "Husband" -> "3",
      "Not-in-family" -> "4",
      "Other-relative" -> "5",
      "Unmarried" -> "6"
  )

  private val race = Map[String, String] (
    "White" -> "1",
    "Asian-Pac-Islander" -> "2",
    "Amer-Indian-Eskimo" -> "3",
    "Other" -> "4",
    "Black" -> "5"
  )

  private val sex = Map[String, String] (
    "Female" -> "1", "Male" -> "2"
  )

  private val nativeCountry = Map[String, String] (
    "United-States" -> "1",
    "Cambodia" -> "2",
    "England" -> "3",
    "Puerto-Rico" -> "4",
    "Canada" -> "5",
    "Germany" -> "6",
    "Outlying-US(Guam-USVI-etc)" -> "7",
    "India" -> "8",
    "Japan" -> "9",
    "Greece" -> "10",
    "South" -> "11",
    "China" -> "12",
    "Cuba" -> "13",
    "Iran" -> "14",
    "Honduras" -> "15",
    "Philippines" -> "16",
    "Italy" -> "17",
    "Poland" -> "18",
    "Jamaica" -> "19",
    "Vietnam" -> "20",
    "Mexico" -> "21",
    "Portugal" -> "22",
    "Ireland" -> "23",
    "France" -> "24",
    "Dominican-Republic" -> "25",
    "Laos" -> "26",
    "Ecuador" -> "27",
    "Taiwan" -> "28",
    "Haiti" -> "29",
    "Columbia" -> "30",
    "Hungary" -> "31",
    "Guatemala" -> "32",
    "Nicaragua" -> "33",
    "Scotland" -> "34",
    "Thailand" -> "35",
    "Yugoslavia" -> "36",
    "El-Salvador" -> "37",
    "Trinadad&Tobago" -> "38",
    "Peru" -> "39",
    "Hong" -> "40",
    "Holand-Netherlands" -> "41"
  )

  private val HDFS_PATH = "hdfs://10.26.26.145:8020/rs/dingjing/"
}
