package BlkFish

/**
  * Created by bradford_bazemore on 9/4/16.
  */

import com.typesafe.config.ConfigFactory
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import Preprocess._

object Driver {

  val sparkConf = new SparkConf().setAppName("test-preprocess")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) = {

    val conf = ConfigFactory.load()

    //Define the parameters to be used in the Random Forest
    val numClasses = 9
    val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
    val numTrees = 32
    val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
    val costFunction = "entropy" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
    val maxDepth = 8
    val maxBins = 200
    val seed = scala.util.Random.nextInt()

    //Load in the already pre-processed training data (pre-processed same way as the testing data is about the be)
    val trainingData = sc.objectFile[LabeledPoint]("gs://project2-csci8360/data/objs/issue8fix")


    //Pre-process the testing data
    val testDataBytes: RDD[(String, String)] = sc.wholeTextFiles("gs://project2-csci8360/testing/*.bytes")

    val testData = toLabeledPoints(bytesToInt(byteCount(removeMemPath(testDataBytes))))

    //Training the Random Forest Model
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeatureInfo, numTrees, featureSubsetStrategy, costFunction, maxDepth, maxBins)

    //Testing the trained model against the pre-processed testing data
    val predictions = testData.map { point => model.predict(point.features)}

    //Formatting the classifier output
    val formattedPreds = predictions.map(pred => (pred.toInt)+1)

    //Saving the output to a txt file
    formattedPreds.saveAsTextFile("gs://project2-csci8360/methods-test/")
  }
}


//  \/\w+(\.\w+)+\n