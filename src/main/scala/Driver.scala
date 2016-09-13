/**
  * Created by bradford_bazemore on 9/4/16.
  */

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest


object Driver {
  def main(args: Array[String]) = {

    //"gs://project2-csci8360/data/trainLabels.csv"
    //"gs://project2-csci8360/data/train/*.bytes"

    //"/media/brad/BackUps/ms_mal_data/trainLabels.csv"
    //"/media/brad/BackUps/ms_mal_data/*.bytes"

    val numClasses = 9
    val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
    val numTrees = 10
    val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
    val costFunction = "entropy" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
    val maxDepth = 5
    val maxBins = 32



    val sparkConf = new SparkConf().setAppName("test-preprocess")
    val sc = new SparkContext(sparkConf)

    val filesMeow: RDD[(String, String)] = sc.textFile("gs://project2-csci8360/data/trainLabels2.csv").map(line => (line.split(",")(0).replace("\"", ""), line.split(",")(1)))
    val filesCats: Map[String, String] = filesMeow.toLocalIterator.foldLeft(Map.empty[String, String]) {
      (acc, word) =>
        acc + (word._1->word._2)
    }




    val trainDataBytes: RDD[(String, String)] = sc.wholeTextFiles("gs://project2-csci8360/data/trainBytes/*.bytes")

    val bytes: RDD[(String, Array[String])] = trainDataBytes.map({
      (kv: (String, String)) =>
        (
          kv._1.replaceAll("(.*\\/)","").replaceAll("(\\.\\w+)+",""),
          kv._2.split(" ")
            .filter(num => num.length <= 2)
          )
    })

    val bytesS: RDD[(String, Map[String, Double])] = bytes.map({
      doc =>
        (
          doc._1,
          doc._2.foldLeft(Map.empty[String, Double]) {
            (acc: Map[String, Double], word: String) =>
              acc + (word -> (acc.getOrElse(word, 0.0) + 1.0))
          }
          )
    })

    val byteCounts: RDD[(String, Map[Int, Double])] = bytesS.map({
      doc =>
        (
          doc._1,
          doc._2.map(
            (byte: (String, Double)) =>
              try {
                (Integer.parseInt(byte._1, 16), byte._2)
              }catch{
                case e : NumberFormatException => (257,byte._2)
              }
            )
          )
    })

    val trainPoints: RDD[LabeledPoint] = byteCounts.map({
      (point: (String, Map[Int, Double])) =>
        LabeledPoint(
          filesCats.get(point._1).head.toDouble,
          Vectors.sparse(
            257,
            point._2.keySet.toArray,
            point._2.values.toArray
          )
        )
    })

    trainPoints.saveAsObjectFile("gs://project2-csci8360/data/objs/trainBytesPoints.obj")

/*    val testDataBytes: RDD[(String, String)] = sc.wholeTextFiles("gs://project2-csci8360/data/testBytes/*.bytes")

    val testBytes: RDD[(String, Array[String])] = testDataBytes.map({
      (kv: (String, String)) =>
        (
          kv._1.replaceAll("(.*\\/)","").replaceAll("(\\.\\w+)+",""),
          kv._2.split(" ")
            .filter(num => num.length <= 2)
          )
    })

    val testBytesS: RDD[(String, Map[String, Double])] = testBytes.map({
      doc =>
        (
          doc._1,
          doc._2.foldLeft(Map.empty[String, Double]) {
            (acc: Map[String, Double], word: String) =>
              acc + (word -> (acc.getOrElse(word, 0.0) + 1.0))
          }
          )
    })

    val testByteCounts: RDD[(String, Map[Int, Double])] = testBytesS.map({
      doc =>
        (
          doc._1,
          doc._2.map(
            (byte: (String, Double)) =>
              try {
                (Integer.parseInt(byte._1, 16), byte._2)
              }catch{
                case e : NumberFormatException => (257,byte._2)
              }
          )
          )
    })

    val testPoints: RDD[LabeledPoint] = testByteCounts.map({
      (point: (String, Map[Int, Double])) =>
        LabeledPoint(
          filesCats.get(point._1).head.toDouble,
          Vectors.sparse(
            257,
            point._2.keySet.toArray,
            point._2.values.toArray
          )
        )
    }) */

    testPoints.saveAsObjectFile("gs://project2-csci8360/data/objs/testBytesPoints.obj")*/

/*
    //training
    val model = RandomForest.trainClassifier(trainPoints, numClasses, categoricalFeatureInfo, numTrees, featureSubsetStrategy, costFunction, maxDepth, maxBins)

    //testing
    val labelAndPreds = testPoints.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //output results
    val accuracy = labelAndPreds.filter(pair => pair._1 == pair._2).count().toDouble/labelAndPreds.count().toDouble
    println(accuracy)*/

  }
}


//  \/\w+(\.\w+)+\n