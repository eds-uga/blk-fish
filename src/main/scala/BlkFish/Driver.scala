package BlkFish

/**
  * Created by bradford_bazemore on 9/4/16.
  */

import com.typesafe.config.ConfigFactory
import org.apache.spark._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import Preprocess._

object Driver {

  val sparkConf = new SparkConf().setAppName("BlkFish")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) = {

    val conf = ConfigFactory.load()

    val categoricalFeatureInfo = Map[Int, Int]()

    val trainData = sc.objectFile[LabeledPoint](conf.getString("ml.path.trainData"))
    val testDataBytes: RDD[(String, String)] = sc.wholeTextFiles(conf.getString("ml.path.testData"))
    val testLabeledPoints = toLabeledPoints(bytesToInt(byteCount(removeMemPath(testDataBytes))))

    //Training the Random Forest Model
    val model = RandomForest.trainClassifier(
      trainData,
      conf.getInt("ml.algo.numberOfClasses"),
      categoricalFeatureInfo,
      conf.getInt("ml.algo.numberOfTrees"),
      conf.getString("ml.algo.featureSubsetStrategy"),
      conf.getString("ml.algo.costFunction"),
      conf.getInt("ml.algo.maxDepth"),
      conf.getInt("ml.algo.maxBins")
    )

    //Testing the trained model against the pre-processed testing data
    val predictions = testLabeledPoints.map { point => model.predict(point.features)}

    //Formatting the classifier output
    val formattedPredictions = predictions.map(pred => (pred.toInt)+1)

    //Saving the output to a txt file
    formattedPredictions.saveAsTextFile(conf.getString("ml.path.predictionsOutput"))
  }
}


//  \/\w+(\.\w+)+\n