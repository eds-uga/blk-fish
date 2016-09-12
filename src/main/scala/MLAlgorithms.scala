import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Brent on 9/8/2016.
  */
object MLAlgorithms {

  val numClasses = 9
  val categoricalFeatureInfo = Map[Int, Int]()//Can be used to make certain features (e.g .dll) categorical, for now not used
  val numTrees = 10
  val featureSubsetStrategy = "auto" //Will use sqrt strategy for numTrees > 1
  val costFunction = "gini" //Other option entropy, gini better for continuous, entropy better for categorical. (though very little difference, and gini is faster)
  val maxDepth = 4
  val maxBins = 32
  //val seed = scala.util.Random.nextLong()


  def main(args:Array[String]) = {

    val sparkConf = new SparkConf().setAppName("test")
    val sc = new SparkContext(sparkConf)
    //data processing
    val data = MLUtils.loadLibSVMFile(sc, "SVM.txt")
    val splits = data.randomSplit(Array(.75,.25))
    val (trainingData, testingData) = (splits(0), splits(1))

    //training
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeatureInfo, numTrees, featureSubsetStrategy, costFunction, maxDepth, maxBins)

    //testing
    val labelAndPreds = testingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //output results
    labelAndPreds.foreach(println)
    val correct = labelAndPreds.filter(x => x._1 == x._2)
    println("correct: " + correct.count)
    val incorrect = labelAndPreds.filter(x => x._1 != x._2)
    println("incorrect: " + incorrect.count)
    val percentage = correct.count.toDouble/(correct.count.toDouble + incorrect.count.toDouble)
    println("PERCENTAGE: " + percentage)


  }



}
