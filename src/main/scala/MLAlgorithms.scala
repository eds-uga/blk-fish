import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{Algo, BoostingStrategy, QuantileStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.sql.catalyst.plans.logical.Sort


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

  //Gradient Boosted parameters not shared
  val minInfoGain =   0.00001
  val numIterations = 100
  val learningRate =   .05
  val validationTotal =   .001

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

    labelAndPreds.foreach(println)

    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.treeStrategy.setMaxBins(maxBins)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.treeStrategy.setNumClasses(numClasses)
    boostingStrategy.treeStrategy.setMinInfoGain(minInfoGain)
    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(categoricalFeatureInfo)
    boostingStrategy.setNumIterations(numIterations)
    boostingStrategy.setLearningRate(learningRate)
    boostingStrategy.setValidationTol(validationTotal)


    val modelGB = GradientBoostedTrees.train(trainingData, boostingStrategy)

    val labelAndPredsGB = testingData.map { point =>
      val prediction = modelGB.predict(point.features)
      (point.label, prediction)
    }

    //output results
    labelAndPredsGB.foreach(println)

  }



}
