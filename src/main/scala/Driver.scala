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


    val numClasses = 9
    val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
    val numTrees = 10
    val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
    val costFunction = "entropy" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
    val maxDepth = 5
    val maxBins = 32



    val sparkConf = new SparkConf().setAppName("test-preprocess")
    val sc = new SparkContext(sparkConf)

    val filesMeow: RDD[(String, String)] = sc.textFile("gs://project2-csci8360/data/trainLabels.csv").map(line => (line.split(",")(0).replace("\"", ""), line.split(",")(1)))
    val filesCats: Map[String, String] = filesMeow.toLocalIterator.foldLeft(Map.empty[String, String]) {
      (acc, word) =>
        acc + (word._1->word._2)
    }

    val trainDataBytes: RDD[(String, String)] = sc.wholeTextFiles("gs://project2-csci8360/data/train/*.bytes")
    //val trainDataAsm = sc.wholeTextFiles("gs://project2-csci8360/data/train/*.asm")

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
            point._2.size,
            point._2.keySet.toArray,
            point._2.values.toArray
          )
        )
    })



    val splits = trainPoints.randomSplit(Array(.75,.25))
    val (trainingData, testingData) = (splits(0), splits(1))

    //training
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeatureInfo, numTrees, featureSubsetStrategy, costFunction, maxDepth, maxBins)

    //testing
    val labelAndPreds = testingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //output results
    val correct = labelAndPreds.filter(x => x._1 == x._2)
    println("correct: " + correct.count)
    val incorrect = labelAndPreds.filter(x => x._1 != x._2)
    println("incorrect: " + incorrect.count)
    val percentage = correct.count.toDouble/(correct.count.toDouble + incorrect.count.toDouble)
    println("PERCENTAGE: " + percentage)

/*    val dlls: RDD[(String, Array[String])] = trainDataAsm.map({
      kv =>
      (
        kv._1,
        kv._2.split(" ")
          .filter(word=>word.contains(".dll")|word.contains(".DLL"))
          .filter(word=>word.matches("\\w+\\.(dll)|(DLL)"))
        )
    })*/

  }
}


//  \/\w+(\.\w+)+\n