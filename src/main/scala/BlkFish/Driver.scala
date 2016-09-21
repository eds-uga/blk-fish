package BlkFish

import org.apache.spark._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import Preprocess._

/**
  *
  */
object Driver {

  val sparkConf = new SparkConf().setAppName("BlkFish")
  val sc = new SparkContext(sparkConf)

  /**
    * Main method used to run spark. If used in the play framework it will be the controller
    * @param args Commandline arguments to pass into the application
    */
  def main(args: Array[String]) = {

    val conf = ConfigFactory.load()

    val categoricalFeatureInfo = Map[Int, Int]()

    val trainData = sc.objectFile[LabeledPoint](conf.getString("ml.path.trainData"))
    val testDataBytes: RDD[(String, String)] = sc.wholeTextFiles(conf.getString("ml.path.testData"))
    val testLabeledPoints = toLabeledPoints(bytesToInt(byteCount(removeMemPath(testDataBytes))))

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

    val predictions = testLabeledPoints.map { point => model.predict(point.features) }

    val formattedPredictions = predictions.map(pred => (pred.toInt) + 1)

    formattedPredictions.saveAsTextFile(conf.getString("ml.path.predictionsOutput"))
  }
}