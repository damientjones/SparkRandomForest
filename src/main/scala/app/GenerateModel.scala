package app

import dao.{ContainerVectorsDao, ContainerDataDao}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import utility.classes.CreateSparkContext

class GenerateModel extends MlLibHelper {

  def main {
    val predictionData = getAndSaveData
    val labelIndexer = new StringIndexer()
      .setInputCol(scanLabel)
      .setOutputCol(scanLabelIndex)
      .fit(predictionData)
    val featureIndexer = new VectorIndexer()
      .setInputCol(scanDataEncodedColumn)
      .setOutputCol(scanEncodedIndex)
      // Treats features with more than 4 categories as a continuous feature
      .setMaxCategories(32)
      .fit(predictionData)
    val rf = new RandomForestClassifier()
      .setLabelCol(scanLabel)
      .setFeaturesCol(scanEncodedIndex)
      // number of trees, more is better to limit variance
      .setNumTrees(100)
      // faster than entropy for classification (no log to compute)
      .setImpurity(impurity)
      // maximum number of bins for splits of continuous features
      .setMaxBins(10)
      // size of tree, more is better to limit bias
      .setMaxDepth(10)
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf))

    // Create the model
    val model = pipeline.fit(predictionData)

    val predictions = model.transform(predictionData)

    // Show the output
    predictions.select(prediction, scanLabel, scanDataEncodedColumn).show(20)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(scanLabel)
      .setPredictionCol(prediction)
      .setMetricName(precision)
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    model
  }
}
