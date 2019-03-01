package jyb

import java.io.Serializable
import java.nio.file.{Files, Paths}

import scala.xml.XML

class Param extends Serializable {

  private var a = 2.0
  private var z = 0.0
  private var alpha = 1.0
  private var l = 1.0
  private var eta = 0.05
  private var rank = 10
  private var dropRate = 0.05

  def this(dir: String) {
    this()
    require(dirExist(dir), "dir invalid!")
    val xmlReader =
      XML.loadFile(dir)
    this.a =
      getParam(xmlReader, "a").toDouble
    this.z =
      getParam(xmlReader, "z").toDouble
    this.alpha =
      getParam(xmlReader, "alpha").toDouble
    this.l =
      getParam(xmlReader, "l").toDouble
    this.rank =
      getParam(xmlReader, "rank").toInt
    this.eta =
      getParam(xmlReader, "eta").toDouble
    this.dropRate =
      getParam(xmlReader, "drop").toDouble
  }

  def getA: Double = this.a
  def getZ: Double = this.z
  def getL: Double = this.l
  def getEta: Double = this.eta
  def getRank: Int = this.rank
  def getAlpha: Double = this.alpha
  def getDropRate: Double = this.dropRate

  private def getParam(reader: xml.Elem, x: String):
  String = {
    (reader \\ x).text
  }

  private def dirExist(dir: String):
  Boolean = {
    val path = Paths.get(dir)
    Files.exists(path)
  }

}
