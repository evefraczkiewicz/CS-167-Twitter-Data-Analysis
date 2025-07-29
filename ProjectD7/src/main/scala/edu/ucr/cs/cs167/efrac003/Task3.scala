//package edu.ucr.cs.cs167.jlow004
package edu.ucr.cs.cs167.efrac003

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}


object Task3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Topic Prediction").getOrCreate()

    // Load JSON data
    val data = spark.read.json("tweets_topic.json")

    // Preprocessing steps
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(1000)
    val indexer = new StringIndexer()
      .setInputCol("topic")
      .setOutputCol("label")
      .setHandleInvalid("keep")

    // Classifier
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

    // Pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, indexer, lr))

    // Split data into training and test sets
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train the model
    val model = pipeline.fit(trainingData)

    // Make predictions
    val predictions = model.transform(testData)

    // Select example rows to display
    predictions.select("id", "text", "topic", "probability", "prediction").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")

    // Compute precision and recall
    val precision = evaluator.setMetricName("precisionByLabel").evaluate(predictions)
    val recall = evaluator.setMetricName("recallByLabel").evaluate(predictions)
    println(s"Precision = $precision")
    println(s"Recall = $recall")

    spark.stop()
  }
}
