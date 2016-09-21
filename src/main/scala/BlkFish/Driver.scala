package BlkFish

import java.io.FileNotFoundException
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.mapred._
import Preprocess._

/**
  *
  */
object Driver {

  val sparkConf = new SparkConf().setAppName("BlkFish")
  val sc = new SparkContext(sparkConf)

  /**
    * Main method used to run spark. If used in the play framework it will be the controller
    *
    * @param args Commandline arguments to pass into the application
    */
  def main(args: Array[String]) = {

    val conf = ConfigFactory.load()

    val categoricalFeatureInfo = Map[Int, Int]()

    val trainData = try {
      sc.objectFile[LabeledPoint](conf.getString("ml.path.trainData"))
    } catch {
      case ex: FileNotFoundException => println(s"ERROR: Could not find ${conf.getString("ml.path.trainData")}")
        println("Aborting...")
        sc.stop()
        null
    }

    val testDataBytes: RDD[(String, String)] = try {
      sc.wholeTextFiles(conf.getString("ml.path.testData"))
    } catch {
      case ex: FileNotFoundException => println(s"ERROR: Could not find ${conf.getString("ml.path.testData")}")
        println("Aborting...")
        sc.stop()
        null
    }

    val testLabeledPoints = toLabeledPoints(
      bytesToInt(
        byteCount(
          removeMemPath(
            testDataBytes
          )
        )
      )
    )

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

    val formattedPredictions = predictions.map(predication => predication.toInt + 1)

    try {
      formattedPredictions.saveAsTextFile(conf.getString("ml.path.predictionsOutput"))
    } catch {

      case ex: FileAlreadyExistsException => println("Prediction file already exists attempting to save with count append")
        try {
          formattedPredictions.saveAsTextFile(conf.getString("ml.path.predictionsOutput") + Calendar.getInstance.getTimeInMillis.toString)
        } catch {
          case ex: FileAlreadyExistsException => println("Failed to save with appended file name.")
            println("File will not be saved")
          case _ => println("Unknown error at save predictions")
        }

      case _ => println("Unknown error at save predictions")
    }

  }
}