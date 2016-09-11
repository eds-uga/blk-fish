/**
  * Created by bradford_bazemore on 9/4/16.
  */

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

object Driver {
  def main(args:Array[String])={
    val sparkConf = new SparkConf().setAppName("test-preprocess")
    val sc = new SparkContext(sparkConf)

    val filesCats=sc.textFile("gs://project2-csci8360/data/trainLabels.csv").map(line=>(line.split(",")(0).replace("\"",""),line.split(",")(1)))
    preproc(sc,filesCats)
  }

  def preproc(sc:SparkContext,fileCats:RDD[(String,String)])={
    fileCats.toLocalIterator.foreach({
      kv=>
        try {
          val temp = sc.textFile("gs://project2-csci8360/data/train/01azqd4InC7m9JpocGv5.bytes")
          val words = temp.flatMap(word => word.split(" "))
          val nomem = words.filter(nums => nums.length <= 2)
          val bytecount = nomem.map(byte => (byte, 1)).reduceByKey(_ + _)
          bytecount.collect().foreach(println)
        }
    })
  }
}
