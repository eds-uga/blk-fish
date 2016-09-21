package BlkFish

import java.io.FileNotFoundException

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by brad on 9/20/16.
  */


object Preprocess {

  def removeMemPath(data: RDD[(String, String)]): RDD[(String, Array[String])] = {
    try {
      data.map({
        (kv: (String, String)) =>
          (
            kv._1.replaceAll("(.*\\/)", "").replaceAll("(\\.\\w+)+", ""),
            kv._2.split(" ")
              .filter(num => num.length <= 2)
            )
      })
    } catch {
      case _ => println("Error at removeMemPath")
        Driver.sc.stop()
        return null
    }
  }

  def byteCount(data: RDD[(String, Array[String])]): RDD[(String, Map[String, Double])] = {
    try {
      data.map({
        doc =>
          (
            doc._1,
            doc._2.foldLeft(Map.empty[String, Double]) {
              (acc: Map[String, Double], word: String) =>
                acc + (word -> (acc.getOrElse(word, 0.0) + 1.0))
            }
            )
      })
    } catch {
      case _ => println("Error at byteCount")
        Driver.sc.stop()
        return null
    }
  }

  def bytesToInt(data: RDD[(String,Map[String,Double])]):RDD[(String,Map[Int,Double])]={
    try {
      data.map({
        doc =>
          (
            doc._1,
            doc._2.map(
              (byte: (String, Double)) =>
                try {
                  (Integer.parseInt(byte._1, 16), byte._2)
                } catch {
                  case e: NumberFormatException => (257, byte._2)
                }
            )
            )
      })
    }catch {
      case _ => println("Error at bytesToInt")
        Driver.sc.stop()
        return null
    }
  }

  def toLabeledPoints(data: RDD[(String, Map[Int, Double])]): RDD[LabeledPoint] = {
    try {
      data.map({
        (point: (String, Map[Int, Double])) =>
          LabeledPoint(
            10.0,
            Vectors.sparse(
              258,
              point._2.toSeq
            )
          )
      })
    }catch {
      case _ => println("Error at toLabeledPoints")
        Driver.sc.stop()
        return null
    }
  }

  def toLabeledPoints(data: RDD[(String, Map[Int, Double])], cats:Map[String,String]): RDD[LabeledPoint] = {
    try {
      data.map({
        (point: (String, Map[Int, Double])) =>
          LabeledPoint(
            cats.get(point._1).head.toDouble,
            Vectors.sparse(
              258,
              point._2.toSeq
            )
          )
      })
    }catch {
      case _ => println("Error at toLabeledPoints")
        Driver.sc.stop()
        return null
    }
  }

  def categoriesToMap(data:RDD[(String,String)]):Map[String,String]={
    data.toLocalIterator.foldLeft(Map.empty[String, String]) {
      (acc, word) =>
        acc + (word._1->word._2)
    }
  }

  def saveLabeledPoint(data: RDD[LabeledPoint], fileName: String): Unit = {
    try {
      data.saveAsObjectFile(fileName)
    } catch {
      case e: FileNotFoundException => println(s"Could not find file for saving $fileName")
      case _ => println(s"An unknown error occurred at saving $fileName")
    }
  }

}
