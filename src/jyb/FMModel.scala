package jyb

import org.apache.spark
import spark.rdd.RDD
import spark.storage.StorageLevel
import spark.Partitioner
import spark.{HashPartitioner => FMPart}

import spark.sql
import sql.{SparkSession, DataFrame}
import sql.functions._
import scala.util.Random

case class FMModel(param: Param, batchNum: Int) {

  // load parameters
  private val alpha = param.getAlpha
  private val drop = param.getDropRate
  private val rank = param.getRank
  private val eta = param.getEta
  private val a = param.getA
  private val z = param.getZ
  private val l = param.getL
  private val epsilon = 0.00001

  private var numUserBlocks = 32
  def setUserBlocks(x: Int):
  this.type = {
    this.numUserBlocks = x
    this
  }

  private var batchSize = 100
  def setBatchSize(x: Int):
  this.type = {
    batchSize = x
    this
  }

  private var intermediateStorage =
    StorageLevel.MEMORY_AND_DISK
  private var finalStorage =
    StorageLevel.MEMORY_AND_DISK
  def setStorageLevel(inter: StorageLevel, last: StorageLevel):
  this.type = {
    this.intermediateStorage = inter
    this.finalStorage = last
    this
  }

  private var checkDir =
    "/checkPoint/fml"
  def setCheckDir(dir: String):
  this.type = {
    this.checkDir = dir
    this
  }

  // fml training
  def train(R: RDD[Usage], test: DataFrame,
            seed: Long, maxIter: Int):
  (RDD[(Int, Factor)], Map[Int, Factor]) = {
    val ss = test.sparkSession
    val sc = ss.sparkContext
    val userPart = new FMPart(numUserBlocks)
    // rid off dependency
    val (matY, items) = process(R)
    // part Y by users
    val partMatY = partUsage(matY, userPart)
      .persist(intermediateStorage)
    // get so-called inblocks
    val userInBlocks = makeBlock(partMatY, userPart)
      .persist(intermediateStorage)
    // random generator
    val seedGen = new Random(seed)
    var userFactors =
      initialize(userInBlocks, rank, seedGen.nextLong())
    var itemFactors =
      initialize(items, rank, seedGen.nextLong())
    var userSumGrads =
      userFactors.mapValues{fs => fs.map(_ => 0.0)}
    var itemSumGrads =
      itemFactors.map{case (k, _) => (k, 0.0)}
    // training with drop-out
    for (step <- 0 until maxIter){
      // get loss
      val trainLoss =
        getDistanceLoss(userInBlocks, items, userFactors, itemFactors)
      val userWithFactor =
        userInBlocks.join(userFactors).mapValues{
          case (block, factors) =>
            block.srcIds.view.zip(factors)
        }.flatMap(_._2).setName(s"$step-W")
          .persist(intermediateStorage)
      val testLoss =
        getRankLoss(test, userWithFactor, itemFactors)
      userWithFactor.unpersist()
      println(s"[$step] train-loss is $trainLoss, test rank loss is $testLoss")
      // batch-gd
      for (iter <- 0 until batchNum){
        val seed0 = seedGen.nextLong() | iter ^ step
        val preUserFactors =
          userFactors.setName(s"$iter-W")
            .persist(intermediateStorage)
        // get gradient
        val (userGrads, itemGrads) =
          learnGradient(userInBlocks, preUserFactors, itemFactors, items, seed0)
        // adagrad
        val preUserSumGrads =
          userSumGrads.setName(s"$iter-SGW")
            .persist(intermediateStorage)
        userSumGrads =
          getAdaGrads(preUserSumGrads, userGrads)
        itemSumGrads =
          itemGrads.foldLeft(itemSumGrads){
            case (dict, (k, gk)) =>
              dict + (k -> (dict(k) + norm2(gk)))
          }
        // update by gradient with AdaGrad method
        userFactors =
          updatedFactor(preUserFactors, userSumGrads, userGrads)
        itemFactors =
          itemGrads.foldLeft(itemFactors){
            case (dict, (i, ghi)) =>
              val _eta =
                div(eta, math.sqrt(itemSumGrads(i)))
              val newHi = plus(dict(i), ghi, -_eta)
              dict + (i -> clip(newHi, l))
          }
        if (iter % 5 == 0){
          userFactors =
            checkFactors(userFactors, checkDir, "userFactors")
          userSumGrads =
            checkSumGrads(userSumGrads, checkDir, "userGrads")
        }
        preUserSumGrads.unpersist()
        preUserFactors.unpersist()
      }
      userFactors = 
        checkFactors(userFactors, checkDir, "userFactors")
    }
    val userWithFactor =
      userInBlocks.join(userFactors).mapValues{
        case (block, factors) =>
          block.srcIds.view.zip(factors)
      }.flatMap(_._2)
    userInBlocks.unpersist()
    partMatY.unpersist()
    (userWithFactor, itemFactors)
  }

  def getRankLoss(test: DataFrame,
                  matW: RDD[(Int, Array[Double])],
                  matH: Map[Int, Array[Double]]):
  Double = {
    // assume test: Row(u, pos0, pos1)
    // pos0: train-used items
    // pos1: test-used items
    val ss = test.sparkSession
    val sc = ss.sparkContext
    import ss.implicits._
    val rDF = test.toDF("u", "pos0", "pos1")
      .as("R").persist(intermediateStorage)
    val wDF = matW.toDF("u", "wu")
      .as("W").persist(intermediateStorage)
    val t1 = rDF.join(wDF, rDF("u") === wDF("u"))
      .select("R.pos0", "R.pos1", "W.wu")
      .toDF("pos0", "pos1", "wu")
      .as("T1").persist(intermediateStorage)
    t1.count()
    rDF.unpersist()
    wDF.unpersist()
    val hBD = sc.broadcast(matH)
    val rankLoss =
      (pos0: Array[Int], pos1: Array[Int], wu: Array[Double]) => {
        val used = (pos0 ++ pos1).toSet
        val negWithScore = hBD.value.toArray
          .filter{case (j, _) => !used.contains(j)}
          .map{case (_, hj) => distance2(wu, hj)}
        val sz = pos1.length
        val DG = pos1.map{i =>
          val hi = hBD.value(i)
          val dui = distance2(wu, hi)
          negWithScore.count(_ <= dui)
        }.map{r =>
          math.log(2.0) / math.log(r + 2.0)
        }.sum
        div(DG, sz)
      }
    val loss =
      t1.as[(Array[Int], Array[Int], Array[Double])].rdd
        .map{case (pos0, pos1, wu) => rankLoss(pos0, pos1, wu)}
        .mean()
    t1.unpersist()
    loss
  }

  def getDistanceLoss(matR: RDD[(Int, InBlock)], items: Array[Int],
                      matW: RDD[(Int, Array[Factor])], matH: Map[Int, Factor]):
  Double = {
    val sc = matR.sparkContext
    val iBD = sc.broadcast(items)
    val errors = matR.join(matW).mapValues{
      case (block, factors) =>
        val srcIds = block.srcIds
        val dstPtrs = block.dstPtrs
        val dstIndices = block.dstIndices
        val errorBuilder =
          collection.mutable.ArrayBuilder.make[Double]
        for (u <- srcIds.indices){
          val wu = factors(u)
          val pu =
            getIndices(dstIndices, dstPtrs(u), dstPtrs(u+1))
          val puSet = pu.toSet
          val nu =
            iBD.value.filter(j => !puSet.contains(j))
          pu.foreach{i =>
            val (yui, cui) = getDistance(1)
            val loss = cui * pow2(yui - distance2(wu, matH(i)))
            errorBuilder += loss
          }
          nu.foreach{j =>
            val (yui, cui) = getDistance(0)
            val loss = cui * pow2(yui - distance2(wu, matH(j)))
            errorBuilder += loss
          }
        }
        errorBuilder.result()
    }.flatMap(_._2)
    errors.mean()
  }

  private def getGaussian(rng: Random,
                          sigma: Double,
                          mu: Double):
  Double =
    rng.nextGaussian() * sigma + mu


  private def initialize(keys: Array[Int],
                         d: Int, seed: Long):
  Map[Int, Factor] = {
    val sz = keys.length
    val rng = new Random(seed ^ sz)
    keys.map{k =>
      val factor =
        Array.fill(d)(getGaussian(rng, 0.03, 0.08))
      (k, factor)
    }.toMap
  }

  private def initialize(keys: RDD[(Int, InBlock)],
                         d: Int, seed: Long):
  RDD[(Int, Array[Factor])] = {
    val sz = keys.count()
    val rng = new Random(seed ^ sz)
    keys.mapPartitions({ps =>
      ps.map{
        case (id, block) =>
          val sz = block.size
          val factors = Array.fill(sz){
            Array.fill(d)(getGaussian(rng, 0.03, 0.08))
          }
          (id, factors)
      }
    }, true)
  }

  private def makeBlock(R: RDD[(Int, Usages)],
                        part: Partitioner):
  RDD[(Int, InBlock)] = {
    R.mapPartitions({items =>
      items.map{case (id, us) =>
        val builder = InBlockBuilder()
        builder.add(us)
        (id, builder.build())
      }
    }, preservesPartitioning = true)
      .setName("InBlock")
  }

  private def partUsage(R: RDD[Usage],
                        part: Partitioner):
  RDD[(Int, Usages)] = {
    val numPartitions = part.numPartitions
    R.mapPartitions{ps =>
      val builders =
        Array.fill(numPartitions)(UsagesBuilder())
      ps.flatMap{r =>
        val blockId = part.getPartition(r.u)
        val builder = builders(blockId)
        builder.add(r)
        if (builder.size >= 2048){
          builders(blockId) = UsagesBuilder()
          Iterator.single((blockId, builder.build()))
        }else Iterator.empty
      } ++ {
        builders.view.zipWithIndex
          .filter(_._1.size > 0).map{
          case (block, idx) =>
            (idx, block.build())
        }
      }
    }.groupByKey(numPartitions).mapValues{ps =>
      val builder = UsagesBuilder()
      ps.foreach(builder.merge)
      builder.build()
    }.setName("Usages")
  }

  def eval(test: DataFrame,
           matW: RDD[(Int, Factor)],
           matH: Map[Int, Factor]):
  Double =
    getRankLoss(test, matW, matH)

  def updatedFactor(factors: RDD[(Int, Array[Factor])],
                    sumGrads: RDD[(Int, Array[Double])],
                    grads: RDD[(Int, Array[Factor])]):
  RDD[(Int, Array[Factor])] = {
    val delta = sumGrads.join(grads).mapValues{
      case (sgs, gs) =>
        sgs.zip(gs).map{case (sg, g) =>
          weighted(g, div(eta, math.sqrt(sg)))
        }
    }.setName("Delta").persist(intermediateStorage)
    val newFactors = factors.join(delta).mapValues{
      case (fs, ds) =>
        fs.zip(ds).map{
          case (f, d) =>
            clip(plus(f, d, -1.0), l)
        }
    }
    newFactors.count()
    delta.unpersist()
    newFactors
  }

  def getAdaGrads(sumGrads: RDD[(Int, Array[Double])],
                  grads: RDD[(Int, Array[Factor])]):
  RDD[(Int, Array[Double])] = {
    sumGrads.join(grads).mapValues{
      case (sgs, gs) =>
        sgs.zip(gs).map{case (sg, g) =>
          sg + norm2(g)
        }
    }
  }

  def learnGradient(srcInBlock: RDD[(Int, InBlock)],
                    srcFactors: RDD[(Int, Array[Factor])],
                    matH: Map[Int, Factor],
                    items: Array[Int], seed0: Long):
  (RDD[(Int, Array[Factor])], Array[(Int, Factor)]) = {
    val sc = srcInBlock.sparkContext
    val iSZ = items.length
    val iBD = sc.broadcast(items)
    val hBD = sc.broadcast(matH)   
    val rng = new util.Random(seed0)
    val joint = srcInBlock.join(srcFactors).mapValues{
      case (block, factors) =>
        val dstPtrs = block.dstPtrs
        val dstIndices = block.dstIndices
        val batches = 
          getBatches(dstPtrs, dstIndices, iBD.value, batchSize, rng.nextLong())
        val gu = factors.map(f => f.map(_ => 0.0))
        val giBuilder =
          collection.mutable.ArrayBuilder.make[(Int, Factor)]
        batches.foreach{case (u, i, rui) =>
            val wu = factors(u)
            val hi = hBD.value(i)
            val (yui, cui) = getDistance(rui)
            val dui = plus(wu, hi, -1.0)
            val fui = dropOut(dui, drop, rng.nextLong())
            val scale = -4 * cui * (yui - norm2(fui))
            gu(u) = plus(gu(u), dui, div(scale, batchSize))
            val elem = (i, weighted(dui, div(-scale, batchSize)))
            giBuilder += elem
        }
        (gu, giBuilder.result())

        /*
        var guBuilder =
          collection.mutable.ArrayBuilder.make[Factor]
        val giBuilder =
          collection.mutable.ArrayBuilder.make[(Int, Factor)]
        for (u <- srcIds.indices){
          val wu = factors(u)
          val pu = getIndices(dstIndices, dstPtrs(u), dstPtrs(u+1))
          val puSet = pu.toSet
          val nu = iBD.value.filter(j => !puSet.contains(j))
          val yu =
            pu.map(i => (i, getDistance(1))) ++
              nu.map(j => (j, getDistance(1)))
          val delta = yu.map{
            case (i, (yui, cui)) =>
              val hi = hBD.value(i)
              val dui = plus(wu, hi, -1.0)
              val fui = dropOut(dui, drop, rng.nextLong())
              val scale = -4 * cui * (yui - norm2(fui))
              (i, weighted(fui, scale))
          }
          var builder =
            Array.fill(rank)(0.0)
          delta.foreach{
            case (i, dui) =>
              builder = plus(builder, dui)
              giBuilder += ((i, weighted(dui, -1.0)))
          }
          guBuilder += builder
        }
        (guBuilder.result(), giBuilder.result())
        */
    }
    val gu = joint.mapValues(_._1)
    val gi = joint.map(_._2._2).flatMap(p => p)
      .reduceByKey(plus).collect()
    (gu, gi)
  }


  private def process(positive: RDD[Usage]):
  (RDD[Usage], Array[Int]) = {
    val items =
      positive.map(_.i)
        .distinct().collect()
    val Y0 = checkOff[Usage](positive, checkDir, "Usage")
      .map(_.split(',')).map(buildUsage)
    (Y0, items)
  }

  private def getDistance(rui: Int):
  (Double, Double) = {
    val yui = this.a * (1 - rui) + this.z * rui
    val cui = 1.0 + alpha * rui
    (yui, cui)
  }

}
