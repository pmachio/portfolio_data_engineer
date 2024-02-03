package org.uam.masterbigdata


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder

object ClassifierHelper extends Schemas{
  /**Generamos datos a partir de unos trayectos originales con una sensibilidad de unos 111metros para la latitud y longitud
   * de inicio y fin.
   * Se crean valores aleatorios entre 0 y 1 y se multiplican por 0.001 para que al sumarlos o restarlos a la latitud y
   * longitud originales no superen los 111 metros
   *
   * http://wiki.gis.com/wiki/index.php/Decimal_degrees */
  def generateData(originalDataFrame:DataFrame): DataFrame = {
    originalDataFrame.withColumn("new_col", explode(array((1 until 201).map(lit): _*)))
      .drop("new_col")
      .withColumn("start_location_latitude",
        when(rand() > 0.5, col("start_location_latitude") + lit(rand() * 0.001))
          .otherwise(col("start_location_latitude") - lit(rand() * 0.001))
      )
      .withColumn("start_location_longitude"
        , when(rand() > 0.5, col("start_location_longitude") + lit(rand() * 0.001))
          .otherwise(col("start_location_longitude") - lit(rand() * 0.001))
      )
      .withColumn("end_location_latitude"
        , when(rand() > 0.5, col("end_location_latitude") + lit(rand() * 0.001))
          .otherwise(col("end_location_latitude") - lit(rand() * 0.001))
      )
      .withColumn("end_location_longitude"
        , when(rand() > 0.5, col("end_location_longitude") + lit(rand() * 0.001))
          .otherwise(col("end_location_longitude") - lit(rand() * 0.001))
      )
  }

  //Crear el pipeline y guardarlo.
  def journeysClassification_LogReg(path:String, journeysToLearnFrom:DataFrame):Unit = {
    val stringIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("label_ind")
    .setHandleInvalid("keep")

    val vectorAssembler:VectorAssembler = new VectorAssembler()
    .setInputCols(Array("start_location_latitude", "start_location_longitude", "end_location_latitude", "end_location_longitude"))
    .setOutputCol("features")

    val dataML_split = journeysToLearnFrom.randomSplit(Array(0.7, 0.3))
    println(s"Total train data: ${dataML_split(0).count()}")
    println(s"Total test data: ${dataML_split(1).count()}")

    val logisticRegression = new LogisticRegression()
    .setFeaturesCol("features")
    .setLabelCol("label_ind")
    //.setRegParam(0.01)

  /**
   * Pipeline
   * */
    val pipeline = new Pipeline()
    pipeline.setStages(Array(stringIndexer, vectorAssembler, logisticRegression))

    /**
     * Ajuste de hiperparámetros
     * */
      val paramGrid = new ParamGridBuilder()
        .addGrid(logisticRegression.regParam, Array(0.1, 0.01, 0.001))
        .addGrid(logisticRegression.maxIter, Array(10, 20, 30))
        .build()

    val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label_ind")

    val crossValidator = new CrossValidator()
      .setEvaluator(evaluator)
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val crossValidator_tr = crossValidator.fit(dataML_split(0))

    //Métricas de cross validation y los parámetros empleados
    //crossValidator_tr.avgMetrics.foreach(println)
    //crossValidator_tr.getEstimatorParamMaps.foreach(println)

    //evaluación de los datos de test
    val bestPipelineModel = crossValidator_tr.bestModel.asInstanceOf[PipelineModel]
    val pred_bestPipelineModel = bestPipelineModel.transform(dataML_split(1))


    val cv_evaluator = crossValidator_tr.getEvaluator
    println(s"Test - F1 ${cv_evaluator.evaluate(pred_bestPipelineModel)}")
    evaluator.setMetricName("weightedPrecision")
    println(s"Test - Precision ${cv_evaluator.evaluate(pred_bestPipelineModel)}")
    evaluator.setMetricName("weightedRecall")
    println(s"Test - Recall ${cv_evaluator.evaluate(pred_bestPipelineModel)}")
    evaluator.setMetricName("accuracy")
    println(s"Test - accuracy ${cv_evaluator.evaluate(pred_bestPipelineModel)}")


    bestPipelineModel.write.overwrite().save(path)
  }

  //recuperar el pipeline
}
