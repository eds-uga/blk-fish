/**
  * Created by bradford_bazemore on 9/4/16.
  */

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object Driver {
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("test-preprocess")
    val sc = new SparkContext(sparkConf)

    val filesCats: RDD[(String, String)] = sc.textFile("gs://project2-csci8360/data/trainLabels.csv").map(line => (line.split(",")(0).replace("\"", ""), line.split(",")(1)))
    val trainData: RDD[(String, String)] = sc.wholeTextFiles("gs://project2-csci8360/data/train/*.bytes")

    val bytes: RDD[(String, Array[String])] = trainData.map({
      kv =>
        (
          kv._1,
          kv._2.split(" ")
            .filter(num => num.length <= 2)
          )
    })

    val byteCounts = bytes.map({
      doc =>
        (
          doc._1,
          doc._2.foldLeft(Map.empty[String, Int]) {
            (count, word) =>
              count + (word -> (count.getOrElse(word, 0) + 1))
          }
          )
    })

    println(byteCounts.count())

  }
}

