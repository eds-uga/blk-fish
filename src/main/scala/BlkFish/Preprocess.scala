package BlkFish

import java.io.FileNotFoundException
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  *
  */
object Preprocess {

  /**
    * Used to remove the path in the file name as this is not used in the classifications file.
    * Memory address are also removed from the file and a array of bytes is created.
    * @param data RDD with (file path,contents of file)
    * @return RDD of (file name, array of bytes in hexadecimal)
    */
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

  /**
    * Counts the number of unique bytes in each array.
    * @param data RDD (file name, array of bytes)
    * @return RDD (file name, map[unique byte, counts of unique byte])
    */
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

  /**
    * Converts the string name of the byte to an integer.
    * @param data RDD (file name, map[unique byte, counts of unique byte])
    * @return RDD (file name, map[integer of unique byte, counts of unique byte])
    */
  def bytesToInt(data: RDD[(String, Map[String, Double])]): RDD[(String, Map[Int, Double])] = {
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
    } catch {
      case _ => println("Error at bytesToInt")
        Driver.sc.stop()
        return null
    }
  }

  /**
    * Creates a LabeledPoint data structure from and RDD of type RDD[(String, Map[Int, Double])]
    * @param data RDD (file name, map[integer of unique byte, counts of unique byte])
    * @return LabeledPoint RDD of each instance file and all counts of unique bytes with 10 as unknow
    */
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
    } catch {
      case _ => println("Error at toLabeledPoints")
        Driver.sc.stop()
        return null
    }
  }

  /**
    * Creates a LabeledPoint data structure from and RDD of type RDD[(String, Map[Int, Double])]
    * but also adding in know classifications for each instance. This is giving in the form of a map
    * to each instance and its category
    * @param data RDD (file name, map[integer of unique byte, counts of unique byte])
    * @param cats Map [instance name, category]
    * @return LabeledPoint RDD of each instance file and all counts of unique bytes and correct classification
    */
  def toLabeledPoints(data: RDD[(String, Map[Int, Double])], cats: Map[String, String]): RDD[LabeledPoint] = {
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
    } catch {
      case _ => println("Error at toLabeledPoints")
        Driver.sc.stop()
        return null
    }
  }

  /**
    *
    * @param data
    * @return
    */
  def categoriesToMap(data: RDD[(String, String)]): Map[String, String] = {
    data.toLocalIterator.foldLeft(Map.empty[String, String]) {
      (acc, word) =>
        acc + (word._1 -> word._2)
    }
  }

  /**
    * Saves and RDD of LabeledPoints to be used for training
    * @param data Preprocessed data in the LabeledPoint data structure
    * @param fileName String name of where to place the saved object file
    */
  def saveLabeledPoint(data: RDD[LabeledPoint], fileName: String): Unit = {
    try {
      data.saveAsObjectFile(fileName)
    } catch {
      case e: FileNotFoundException => println(s"Could not find file for saving $fileName")
      case _ => println(s"An unknown error occurred at saving $fileName")
    }
  }

}
