package jyb

import org.apache.spark
import spark.sql
import sql.SparkSession
import sql.DataFrame
import sql.functions._
import spark.rdd.RDD

object test {

  def main(args: Array[String]): Unit = {
    val cfgDir = args(0)
    val seed0 = args(1).toLong
    val maxIter = args(2).toInt
    val param = new Param(cfgDir)
    val model = FMModel(param, 20)
      .setCheckDir("/checkPoint/app_with_sementic/FML")
      .setUserBlocks(128)
      .setBatchSize(50000)

    val ss = SparkSession.builder()
      .appName("FM-Rank")
      .getOrCreate()
    val sc = ss.sparkContext

    val dataDir = "/app_with_sementic/" +
      "dataset/divide_by_seed42/train"
    val data = sc.textFile(dataDir)
      .repartition(1024).map(_.split('|'))
      .map(ps => Usage(ps(1).toInt, ps(2).toInt))
    val option1 = data.map(p => (p.u, 1))
      .reduceByKey(_ + _).filter(_._2 >= 10)
    println("[INFO] %d users remains while using ge 10 apps".format(option1.count()))
    val option2 = data.map(p => (p.i, 1))
      .reduceByKey(_ + _).filter(_._2 >= 10)
    println("[INFO] %d users remains while being used ge 10 users".format(option2.count()))
    val valid = data.map(p => (p.u, p))
      .join(option1).map(_._2._1)
      .map(p => (p.i, p))
      .join(option2).map(_._2._1)
    println("[INFO] before v.s. after filtering, %d <-> %d".format(data.count(), valid.count()))

    val rng = new util.Random(seed0)
    for (cv <- 0 until 1) {
      val parts =
        valid.randomSplit(Array(80, 20), rng.nextLong() ^ cv)
      val train = parts(0)
      val test = parts(1)
      val testDF = getTestDF(ss, train, test)
      val (matW, matH) =
        model.train(train, testDF, rng.nextLong(), maxIter)
      val loss = model.eval(testDF, matW, matH)
      println(s"Final test rank loss $loss")
    }
  }

  def getTestDF(ss: SparkSession,
                train: RDD[Usage],
                test: RDD[Usage]):
  DataFrame = {
    import ss.implicits._
    val sc = ss.sparkContext
    val items = train.map(_.i)
      .distinct().collect()
    val iBD = sc.broadcast(items)
    val trainDF = train.map(p => (p.u, p.i))
      .aggregateByKey(Array[Int]())(_ :+ _, _ ++ _)
      .toDF("u", "is").as("T1")
      .persist()
    val testDF = test.map(p => (p.u, p.i))
      .aggregateByKey(Array[Int]())(_ :+ _, _ ++ _)
      .toDF("u", "is").as("T2")
      .persist()
    val joint = trainDF.join(testDF, trainDF("u") === testDF("u"))
      .select("T1.u", "T1.is", "T2.is")
      .toDF("u", "is", "js")
      .as("J1").persist()
    joint.count()
    trainDF.unpersist()
    testDF.unpersist()
    val result = joint.as[(Int, Array[Int], Array[Int])]
      .rdd.map{case (u, is, js) =>
        val trainSet = is.toSet
        val testSet = js.toSet.diff(trainSet)
        val positive = iBD.value
          .filter(i => testSet.contains(i))
        val negative = iBD.value
          .filter(i => !trainSet.contains(i))
          .filter(i => !testSet.contains(i))
      (u, positive, negative)
    }.toDF("u", "pos", "neg")
      .as("Test").persist()
    result.count()
    joint.unpersist()
    // check off
    val testDir =
      "/checkPoint/app_with_sementic/FML/test"
    deleteIfExists(sc, testDir)
    result.as[(Int, Array[Int], Array[Int])].rdd
      .filter(_._2.nonEmpty)
      .map{case (u, pos, neg) =>
        val uStr = u.toString
        val posStr = pos.mkString(",")
        val negStr = neg.mkString(",")
        s"$uStr|$posStr|$negStr"
      }.repartition(128)
      .saveAsTextFile(testDir)
    sc.textFile(testDir).map(_.split('|'))
      .map{ps =>
        val u = ps(0).toInt
        val pos =
          ps(1).split(',').map(_.toInt)
        val neg =
          ps(2).split(',').map(_.toInt)
        (u, pos, neg)
      }.toDF("u", "pos", "neg")
  }

}
