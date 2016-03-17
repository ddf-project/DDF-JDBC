package io.ddf.jdbc.analytics

import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter
import io.ddf.content.Schema.ColumnClass
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.types.AggregateTypes.AggregateFunction
import org.scalatest.FlatSpec

trait AnalyticsBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithAggregationHandler(implicit l: Loader) = {
    val ddf = l.loadAirlineDDF().sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from ddf://adatao/airline")

    it should "calculate simple aggregates" in {
      val res1 = ddf.aggregate("year, month, min(depdelay), max(arrdelay)")
      res1.size() should be(13)
      val aggregateResult = ddf.aggregate("year, month, avg(depdelay), stddev(arrdelay)")
      val result: Array[Double] = aggregateResult.get("2010\t3")
      result.length should be(2)

      val colAggregate = ddf.getAggregationHandler.aggregateOnColumn(AggregateFunction.MAX, "YEAR")
      colAggregate should be(2010)
    }

    it should "group data" in {
      val l1 = Lists.newArrayList("year", "month")
      val l2 = Lists.newArrayList("m=avg(depdelay)")
      val l3 = Lists.newArrayList("m= stddev(arrdelay)")
      val l4 = Lists.newArrayList("dayofweek")
      ddf.groupBy(l1, l2).getNumRows should be(13)
      ddf.groupBy(l4, l3).getNumRows should be(1)

      ddf.groupBy(l1).agg(l2).getNumRows() should be(13)
      ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = count(*)")).getNumRows should be(3)
      ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = count(1)")).getNumRows should be(3)
      ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = count(dayofweek)")).getNumRows should be(3)
      ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = avg(arrdelay)")).getNumRows should be(3)
    }
  }

  def ddfWithBinningHandler(implicit l: Loader): Unit = {
    it should "bin the ddf" in {
      val ddf = l.loadAirlineDDF()
      val newDDF: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true)

      newDDF.getSchemaHandler.getColumn("dayofweek").getColumnClass should be(ColumnClass.FACTOR)

      (newDDF.getSchemaHandler.getColumn("dayofweek").getOptionalFactor.getLevels.size >0) should be(true)

      val ddf1: DDF = ddf.binning("month", "custom", 0, Array[Double](2, 4, 6, 8), true, true)

      ddf1.getSchemaHandler.getColumn("month").getColumnClass should be(ColumnClass.FACTOR)
      // {'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
      (ddf1.getSchemaHandler.getColumn("month").getOptionalFactor.getLevelMap.get("[2,4]")>0) should be(true)

    }
  }

  def ddfWithStatisticsHandler(implicit l: Loader): Unit = {
    val ddf = l.loadAirlineNADDF()

    it should "calculate summary and simple summary" in {

      val summaries = ddf.getSummary
      summaries.head.max() should be(2010)

      val randomSummary = summaries(6)
      randomSummary.variance() should be(260747.76 +- 1.0)

    }
    it should "calculate vector mean" in {
      ddf.getVectorMean("YEAR") should not be null
    }

    it should "calculate vector variance" in {
      val variance = ddf.getVectorVariance("YEAR")
      variance.length should be(2)
    }

    it should "calculate vector cor" in {
      val cor = ddf.getVectorCor("YEAR", "MONTH")
      println(cor)
      cor should not be null
    }

    it should "calculate vector covariance" in {
      val cov = ddf.getVectorCovariance("YEAR", "MONTH")
      println(cov)
      cov should not be null
    }

    it should "calculate vector quantiles" in {
      val pArray: Array[java.lang.Double] = Array(0.3, 0.5, 0.7)
      val expectedQuantiles: Array[java.lang.Double] = Array(801.0, 1416.0, 1644.0)
      val quantiles: Array[java.lang.Double] = ddf.getVectorQuantiles("deptime", pArray)
      quantiles should equal(expectedQuantiles)
    }

    it should "calculate vector histogram" in {
      val bins: java.util.List[AStatisticsSupporter.HistogramBin] = ddf.getVectorHistogram("arrdelay", 5)
      bins.size should be(5)
      val first = bins.get(0)
      first.getX should be(-24)
      first.getY should be(10.0)
    }
  }

}
