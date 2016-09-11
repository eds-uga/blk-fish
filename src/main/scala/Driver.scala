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
    //val bytes:ListBuffer[scala.collection.Map[String,Int]]=ListBuffer()

/*    filesCats.toLocalIterator.foreach({
      kv =>
        try {
          val temp = sc.textFile("gs://project2-csci8360/data/train/" + kv._1 + ".bytes")
          val words = temp.flatMap(word => word.split(" "))
          val nomem = words.filter(nums => nums.length <= 2)
          val bytecount = nomem.map(byte => (byte, 1)).reduceByKey(_ + _)
          bytes.append(bytecount.collectAsMap())
        }catch{
          case e: InvalidInputException=>println("ERROR: Missing file")
        }
    })*/

    val bytes: RDD[Array[String]] = trainData.map({
      kv=>
        kv._2.split(" ")
        .filter(num=>num.length<=2)
    })

    val byteCounts: RDD[Map[String, Int]] = bytes.map({
      doc=>
        doc.foldLeft(Map.empty[String, Int]){
            (count, word) =>
              count + (word -> (count.getOrElse(word, 0) + 1))
          }
    })

    byteCounts.toLocalIterator.foreach(m=>for((k,v) <- m) printf("key: %s, value: %s\n", k, v))

  }
}

