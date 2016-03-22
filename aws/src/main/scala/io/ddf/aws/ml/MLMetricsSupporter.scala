package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.exception.DDFException
import io.ddf.jdbc.content.{Representations, SqlArrayResult}
import io.ddf.ml.RocMetric


class MLMetricsSupporter(ddf: DDF) extends io.ddf.ml.AMLMetricsSupporter(ddf) {

  //we depend on aws ML Supporter. This metrics supporter cannot function independently
  val mlSupporter = ddf.getMLSupporter.asInstanceOf[MLSupporter]
  val mlHelper = mlSupporter.getAwsMLHelper
  val awsHelper = mlSupporter.getAwsHelper

  @throws(classOf[DDFException])
  override def r2score(meanYTrue: Double): Double = {
    throw new DDFException("Cannot get r2Score", new UnsupportedOperationException())
  }

  @throws(classOf[DDFException])
  override def residuals: DDF = {
    throw new DDFException("Cannot get residuals", new UnsupportedOperationException())
  }

  @throws(classOf[DDFException])
  override def roc(alpha_length: Int): RocMetric = {
    val predictDDFAsSql = this.getDDF().getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val matrix = Array.ofDim[Double](alpha_length, 3)
    for (count <- 1 to alpha_length) {
      val threshold = count * 1.0 / alpha_length
      var truePositive = 0
      var falsePositive = 0
      var trueNegative = 0
      var falseNegative = 0
      for (row <- predictDDFAsSql.indices) {
        val oldVal = (List(predictDDFAsSql(row)(0)) collect { case i: java.lang.Number => i.intValue() }).sum
        val newVal = predictDDFAsSql(row)(1).asInstanceOf[Int]
        val score = predictDDFAsSql(row)(2).asInstanceOf[Double]
        if (oldVal == 1 && (score > threshold || score == threshold)) truePositive = truePositive + 1
        else if (oldVal == 1 && score < threshold) falseNegative = falseNegative + 1
        else if (oldVal == 0 && (score > threshold || score == threshold)) falsePositive = falsePositive + 1
        else trueNegative = trueNegative + 1
      }
      matrix(count - 1)(0) = threshold
      matrix(count - 1)(1) = truePositive * 1.0 / (truePositive + falseNegative)
      matrix(count - 1)(2) = falsePositive * 1.0 / (falsePositive + trueNegative)
    }
    val rocMetric = new RocMetric(matrix.toArray, 0.0)
    rocMetric.computeAUC()
    rocMetric
  }

  @throws(classOf[DDFException])
  override def rmse(testDDF: DDF, implicitPref: Boolean): Double = {
    val model = ddf.ML.train("REGRESSION")
    val awsModel = model.getRawModel.asInstanceOf[AwsModel]
    val sql = awsHelper.selectSql(testDDF.getTableName)
    val dataSourceId = mlHelper.createDataSourceFromRedShift(ddf.getSchema, sql, awsModel.getMLModelType)
    mlHelper.getEvaluationMetrics(dataSourceId, awsModel.getModelId, awsModel.getMLModelType.toString)
  }
}
