package jyb

import org.apache.spark
import spark.sql
import sql.SparkSession
import sql.DataFrame
import spark.rdd.RDD
import java.nio.file.Paths

object test {

  def main(args: Array[String]): Unit = {
    val cfgDir = args(0)
    val seed0 = args(1).toLong
    val maxIter = args(2).toInt
    val checkDir = args(3)
    val param = new Param(cfgDir)
    val model = FMModel(param, 20)
      .setCheckDir(checkDir)
      .setUserBlocks(128)
      .setBatchSize(param.getBatchSize)

    val ss = SparkSession.builder()
      .appName("FM-Rank")
      .getOrCreate()
    val sc = ss.sparkContext

    val dataDir = args(4)
    // "/app_with_sementic/" +
    //   "dataset/divide_by_seed42/train"
    val data = sc.textFile(dataDir)
      .repartition(1024).map(_.split('|'))
      .map(ps => Usage(ps(1).toInt, ps(2).toInt))
    val option1 = data.map(p => (p.u, 1))
      .reduceByKey(_ + _).filter(_._2 >= 10)
    println("[INFO] %d users remains while using ge 10 apps".format(option1.count()))
    val option2 = data.map(p => (p.i, 1))
      .reduceByKey(_ + _).filter(_._2 >= 10)
    println("[INFO] %d users remains while being used ge 10 users".format(option2.count()))
    val remains = data.map(p => (p.u, p))
      .join(option1).map(_._2._1)
      .map(p => (p.i, p))
      .join(option2).map(_._2._1)
    println("[INFO] before v.s. after filtering, %d <-> %d".format(data.count(), remains.count()))

    val rng = new util.Random(seed0)
    for (cv <- 0 until 1) {
      val parts =
        remains.randomSplit(Array(80, 20), rng.nextLong() ^ cv)
      val train = parts(0)
      val valid = parts(1)
      val validDF = getTestDF(ss, train, valid, checkDir)
      val (matW, matH) =
        model.train(train, validDF, rng.nextLong(), maxIter)
      val loss = model.eval(validDF, matW, matH)
      println(s"Final test rank loss $loss")
    }
  }

  def getTestDF(ss: SparkSession, train: RDD[Usage],
                valid: RDD[Usage], checkDir: String):
  DataFrame = {
    import ss.implicits._
    val sc = ss.sparkContext
    val trainDF = train.map(p => (p.u, p.i))
      .aggregateByKey(Array[Int]())(_ :+ _, _ ++ _)
      .toDF("u", "is").as("T1")
      .persist()
    val validDF = valid.map(p => (p.u, p.i))
      .aggregateByKey(Array[Int]())(_ :+ _, _ ++ _)
      .toDF("u", "is").as("T2")
      .persist()
    val joint = trainDF.join(validDF, trainDF("u") === validDF("u"))
      .select("T1.u", "T1.is", "T2.is")
      .toDF("u", "is", "js")
      .as("J1").persist()
    joint.count()
    trainDF.unpersist()
    validDF.unpersist()
    val result = joint.as[(Int, Array[Int], Array[Int])]
      .rdd.map{case (u, is, js) =>
        val trainSet = is.toSet
        val validSet =
          js.filter(j => !trainSet.contains(j))
      (u, is, validSet)
    }.toDF("u", "pos0", "pos1")
      .as("Valid").persist()
    result.count()
    joint.unpersist()
    // check off
    val validDir =
      Paths.get(checkDir, "valid").toString
    deleteIfExists(sc, validDir)
    result.as[(Int, Array[Int], Array[Int])].rdd
      .filter(_._3.nonEmpty)
      .map{case (u, pos0, pos1) =>
        val uStr = u.toString
        val pos0Str = pos0.mkString(",")
        val pos1Str = pos1.mkString(",")
        s"$uStr|$pos0Str|$pos1Str"
      }.repartition(128)
      .saveAsTextFile(validDir)
    sc.textFile(validDir).map(_.split('|'))
      .map{ps =>
        val u = ps(0).toInt
        val pos =
          ps(1).split(',').map(_.toInt)
        val neg =
          ps(2).split(',').map(_.toInt)
        (u, pos, neg)
      }.toDF("u", "pos0", "pos1")
  }

}
