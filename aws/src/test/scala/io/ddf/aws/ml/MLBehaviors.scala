package io.ddf.aws.ml

import io.ddf.{Factor, DDF}
import io.ddf.aws.AWSLoader
import io.ddf.content.Schema
import io.ddf.jdbc.content.{SqlArrayResult, Representations}
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.ml.IModel
import org.scalatest.FlatSpec
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

trait MLBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithRegression(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "do regression model computation" in {
      val ddf: DDF = airlineDDF
      val model: IModel = ddf.ML.train("REGRESSION")
      val prediction = ddf.ML.applyModel(model)
      val predictDDFAsSql = prediction.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
        .asInstanceOf[SqlArrayResult].result
      predictDDFAsSql foreach (row => println(row map (cell => cell.toString) mkString (",")))
      assert(prediction.getNumColumns > 0)
    }
  }

  def ddfWithModelParameters(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "do regression model computation with parameters" in {
      val ddf: DDF = airlineDDF
      val map: java.util.Map[String, String] = new java.util.HashMap[String, String]()
      map.put("sgd.l1RegularizationAmount", "1.0E-08")
      //map.put("sgd.l2RegularizationAmount", "1.0E-08")
      // Use one of the 2 regularization parameters
      map.put("sgd.maxPasses", "10")
      map.put("sgd.maxMLModelSizeInBytes", "33554432")
      val model: IModel = ddf.ML.train("REGRESSION", map)
      val prediction = ddf.ML.applyModel(model)
      val predictDDFAsSql = prediction.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
        .asInstanceOf[SqlArrayResult].result
      predictDDFAsSql foreach (row => println(row map (cell => cell.toString) mkString (",")))
      assert(prediction.getNumColumns > 0)
    }
  }

  def ddfWithBinary(implicit l: Loader): Unit = {
    val mtcarDDF = l.loadMtCarsDDF().sql2ddf("SELECT mpg ,cyl , disp , hp, drat , wt, qsec, vs FROM ddf://adatao/mtcars")

    it should "do binary model computation" in {
      print(mtcarDDF.getUri)
      val ddf: DDF = mtcarDDF
      val model: IModel = ddf.ML.train("BINARY")
      val prediction = ddf.ML.applyModel(model)
      val predictDDFAsSql = prediction.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
        .asInstanceOf[SqlArrayResult].result
      predictDDFAsSql foreach (row => println(row map (cell => cell.toString) mkString (",")))
      assert(prediction.getNumColumns > 0)
    }

  }

  def ddfWithMulticlass(implicit l: Loader): Unit = {

    val mtcarDDF = l.loadMtCarsDDF()

    it should "do multiclass model computation" in {
      val ddf: DDF = mtcarDDF
      val model: IModel = ddf.ML.train("MULTICLASS")
      val prediction = ddf.ML.applyModel(model)
      val predictDDFAsSql = prediction.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
        .asInstanceOf[SqlArrayResult].result
      predictDDFAsSql foreach (row => println(row map (cell => cell.toString) mkString (",")))
      assert(prediction.getNumColumns > 0)
    }
  }

  def ddfWithCrossValidation(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()

    it should "do CVRandom computation" in {
      val ddf: DDF = airlineDDF
      val crossValidationRandom = ddf.ML.CVRandom(2, 0.5, 3L).asScala.toSeq
      crossValidationRandom foreach {
        crossValidationSet => println("test DDF size (" + crossValidationSet.getTestSet.getNumRows + ","
          + crossValidationSet.getTestSet.getNumColumns + ")" + " train DDF size (" + crossValidationSet.getTrainSet
          .getNumRows + "," + crossValidationSet.getTrainSet.getNumColumns + ")")
      }
      assert(crossValidationRandom.size == 2)
    }

    it should "do CVKFold computation" in {
      val ddf: DDF = airlineDDF
      val crossValidationRandom = ddf.ML.CVKFold(2, 3L).asScala.toSeq
      crossValidationRandom foreach {
        crossValidationSet => println("test DDF size (" + crossValidationSet.getTestSet.getNumRows + ","
          + crossValidationSet.getTestSet.getNumColumns + ")" + " train DDF size (" + crossValidationSet.getTrainSet
          .getNumRows + "," + crossValidationSet.getTrainSet.getNumColumns + ")")
      }
      assert(crossValidationRandom.size == 2)
    }
  }

  def ddfWithConfusionMatrix(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()

    it should "do confusion matrix evaluation" in {
      val ddf: DDF = airlineDDF
      val model: IModel = ddf.ML.train("REGRESSION")
      val confusionMatrix = ddf.ML.getConfusionMatrix(model, 1.2)
      confusionMatrix foreach (row => println(row.mkString(",")))
      assert(confusionMatrix.nonEmpty)
    }
  }

  def ddfWithMetrics(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()
    val mtcarsDDF: DDF = l.loadMtCarsDDF().sql2ddf("SELECT mpg ,cyl , disp , hp, drat , wt, qsec, vs FROM ddf://adatao/mtcars")

    it should "do rmse evaluation" in {
      val ddf: DDF = airlineDDF
      val rmse = ddf.getMLMetricsSupporter.rmse(ddf, true)
      assert(rmse > 0)
    }

    it should "do roc computation" in {
      val ddf: DDF = mtcarsDDF
      val rocMetric = ddf.getMLMetricsSupporter.roc(10000)
      rocMetric.pred foreach (row => println(row.mkString(",")))
      assert(!(rocMetric.auc < 0))
    }
  }

  def ddfWithPrediction(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()
    val mtcarsDDF: DDF = l.loadMtCarsDDF()

    it should "do prediction for regression model" in {
      val ddf: DDF = airlineDDF
      val linearRegressionModel: LinearRegression = ddf.ML.train("REGRESSION").getRawModel.asInstanceOf[LinearRegression]
      val predictedValue = linearRegressionModel.predict(Seq(2008, 4, 3, 4, 1644, 1510, 1845, 1725, "WN", 1333, "N334SW", 121, 135,
        107, 80, 94, "IND", "MCO", 828, 6, 8, 0, 0, 0, 8, 0, 0, 0))
      assert(predictedValue.isInstanceOf[Float])
    }

    it should "do prediction for binary model" in {
      val ddf: DDF = mtcarsDDF.sql2ddf("SELECT mpg ,cyl , disp , hp, drat , wt, qsec, vs FROM ddf://adatao/mtcars")
      val binaryClassificationModel: BinaryClassification = ddf.ML.train("BINARY").getRawModel.asInstanceOf[BinaryClassification]
      val predictedLabel = binaryClassificationModel.predict(Seq(21.0, 6, 160.0, 110, 3.90, 2.620, 16.46))
      assert(predictedLabel == "0" || predictedLabel == "1")
    }

    it should "do prediction for multiclass model" in {
      val ddf: DDF = mtcarsDDF
      val targetColumn = ddf.getColumnNames.asScala.last
      ddf.setAsFactor(targetColumn)
      val levels = ddf.getSchemaHandler.computeLevelCounts(targetColumn)
      val multiclassClassificationModel: MultiClassClassification = ddf.ML.train("MULTICLASS").getRawModel
        .asInstanceOf[MultiClassClassification]
      val predictedLabel = multiclassClassificationModel.predict(Seq(21.0, 6, 160.0, 110, 3.90, 2.620, 16.46, 0, 1, 4))
      assert(levels exists (level => level equals predictedLabel))
    }
  }

}
