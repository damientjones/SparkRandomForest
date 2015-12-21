package app

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}

class GenerateModel extends MlLibHelper {

  def main {
    val predictionData = getAndSaveData
    val labelIndexer = new StringIndexer()
      .setInputCol(getLabel(predictColumn))
      .setOutputCol(getIndex(predictColumn))
      .fit(predictionData)
    val featureIndexer = new VectorIndexer()
      .setInputCol(predictionFeatures)
      .setOutputCol(getIndex(predictionFeatures))
      // Treats features with more than 4 categories as a continuous feature
      .setMaxCategories(maxCategories)
      .fit(predictionData)
    val rf = new RandomForestClassifier()
      .setLabelCol(getLabel(predictColumn))
      .setFeaturesCol(getIndex(predictionFeatures))
      // number of trees, more is better to limit variance
      .setNumTrees(numTrees)
      // faster than entropy for classification (no log to compute)
      .setImpurity(impurity)
      // maximum number of bins for splits of continuous features
      .setMaxBins(maxBins)
      // size of tree, more is better to limit bias
      .setMaxDepth(maxDepth)
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf))

    // Create the model
    val model = pipeline.fit(predictionData)

    val predictions = model.transform(predictionData)

    // Show the output
    predictions.select(prediction, getLabel(predictColumn), predictionFeatures).show(20)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(getLabel(predictColumn))
      .setPredictionCol(prediction)
      .setMetricName(precision)
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    model
  }
}
