import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
//import scala.util.


/**
  * Created by Brent on 9/8/2016.
  */
object MLAlgorithms {

  val numClasses = 9
  val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
  val numTrees = 10
  val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
  val infoGainStrategy = "gini" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
  val maxDepth = 4
  val maxBins = 100
  val seed = scala.util.Random.nextLong()


  def main(args:Array[String]) = {

    val sparkConf = new SparkConf().setAppName("test")
    val sc = new SparkContext(sparkConf)

    val data = MLUtils.loadLibSVMFile(sc, "blk-fish/small_train_MLUtil.txt")

  }



}
