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

    //Define the parentmeters to be used in the Random Forest
    val numClasses = 9
    val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
    val numTrees = 32
    val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
    val costFunction = "entropy" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
    val maxDepth = 8
    val maxBins = 200
    val seed = scala.util.Random.nextInt()

    //Initialize SparkContext
    val sparkConf = new SparkConf().setAppName("test-preprocess")
    val sc = new SparkContext(sparkConf)

    //Load in the already pre-processed training data (pre-processed same way as the testing data is about the be)
    val trainingData = sc.objectFile[LabeledPoint]("gs://project2-csci8360/data/objs/issue8fix")


    //Pre-process the testing data
    val testDataBytes: RDD[(String, String)] = sc.wholeTextFiles("gs://project2-csci8360/data/testBytes/*.bytes")
    //val trainDataAsm = sc.wholeTextFiles("gs://project2-csci8360/data/train/*.asm")

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
          10.0,
          Vectors.sparse(
            258,
            point._2.toSeq
          )
        )
    })

    //Training the Random Forest Model
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeatureInfo, numTrees, featureSubsetStrategy, costFunction, maxDepth, maxBins)

    //Testing the trained model against the pre-processed testing data
    val predictions = testPoints.map { point => model.predict(point.features)}

    //Formatting the classifier output
    val formattedPreds = predictions.map(pred => (pred.toInt)+1)

    //Saving the output to a txt file
    formattedPreds.saveAsTextFile("gs://project2-csci8360/data/testOutput/")
  }
}


//  \/\w+(\.\w+)+\n