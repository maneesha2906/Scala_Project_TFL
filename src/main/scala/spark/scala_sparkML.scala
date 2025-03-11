import org.apache.spark.sql.SparkSession

object TFLUndergroundPredictor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TfL Underground Predictive Analysis")
      .enableHiveSupport()
      .getOrCreate()

    // Load preprocessed TfL data from Hive
    val data = spark.sql("SELECT * FROM tfl_underground_data1")

    // Feature Engineering
    val assembler = new VectorAssembler()
      .setInputCols(Array("time_of_day", "station_id", "day_of_week", "passenger_count"))
      .setOutputCol("features")

    // Train-Test Split
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    // Train ML Model
    val rf = new RandomForestRegressor()
      .setLabelCol("delay_time")  // Predicting delays
      .setFeaturesCol("features")

    // Create a pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, rf))
    val model = pipeline.fit(trainingData)

    // Save model to HDFS for deployment
    model.write.overwrite().save("hdfs://namenode:9000/models/tfl_underground_rf_model")

    // Evaluate Model
    val predictions = model.transform(testData)
    val evaluator = new RegressionEvaluator()
      .setLabelCol("delay_time")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE): $rmse")

    spark.stop()
  }
}
