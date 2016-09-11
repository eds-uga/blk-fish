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

    val filesCats = sc.textFile("gs://project2-csci8360/data/trainLabels.csv").map(line => (line.split(",")(0).replace("\"", ""), line.split(",")(1)))
    //val trainData = sc.wholeTextFiles("gs://project2-csci8360/data/train/*.bytes")
    val bytes:ListBuffer[scala.collection.Map[String,Int]]=ListBuffer()

    filesCats.toLocalIterator.foreach({
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
    })

    bytes.foreach(m=>m.foreach(x => print ("|"+x._1 + "-->" + x._2 +"|")))

  }
}

