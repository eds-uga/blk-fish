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
    //"gs://project2-csci8360/data/objs/issue8fix"

    //"/media/brad/BackUps/ms_mal_data/trainLabels.csv"
    //"/media/brad/BackUps/ms_mal_data/*.bytes"

    val numClasses = 9
    val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
    val numTrees = 32
    val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
    val costFunction = "entropy" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
    val maxDepth = 6
    val maxBins = 200
    val seed = scala.util.Random.nextInt()


    val sparkConf = new SparkConf().setAppName("test-preprocess")
    val sc = new SparkContext(sparkConf)





    val splits = trainPoints.randomSplit(Array(.99,.01))
    val (trainingData, testingData) = (splits(0), splits(1))

    //training
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeatureInfo, numTrees, featureSubsetStrategy, costFunction, maxDepth, maxBins, seed)

    //testing
    val labelAndPreds = testingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val accuracy = labelAndPreds.filter(pair => pair._1 == pair._2).count().toDouble/labelAndPreds.count().toDouble
    println(accuracy)


  }
}


//  \/\w+(\.\w+)+\n