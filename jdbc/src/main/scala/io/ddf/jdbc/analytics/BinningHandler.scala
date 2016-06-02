package io.ddf.jdbc.analytics

import java.{lang, util}

import io.ddf.DDF
import io.ddf.analytics.ABinningHandler.BinningType
import io.ddf.analytics.AStatisticsSupporter
import io.ddf.analytics.AStatisticsSupporter.HistogramBin
import io.ddf.content.Schema.Column
import io.ddf.exception.DDFException
import io.ddf.jdbc.analytics.StatsUtils.Histogram
import io.ddf.jdbc.content.{Representations, SqlArrayResult}
import java.text.DecimalFormat
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

class BinningHandler(ddf: DDF) extends io.ddf.analytics.ABinningHandler(ddf) {

  override def getVectorHistogram(column: String, numBins: Int): util.List[HistogramBin] = {
    val columnData = getDoubleColumn(ddf, column).get

    val max = columnData.max
    val min = columnData.min

    // Scala's built-in range has issues. See #SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps - 1, 1).map(s => min + (s * span) / steps) :+ max
    }

    // Compute the minimum and the maximum
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty DataSet or DataSet containing +/-infinity or NaN")
    }
    val range = if (min != max) {
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, numBins)
    } else {
      List(min, min)
    }

    val bins: util.List[HistogramBin] = new util.ArrayList[HistogramBin]()
    val hist = new Histogram(range)
    columnData.foreach{ in =>
      hist.add(in)
    }
    val histograms = hist.getValue
    histograms.foreach { entry =>
      val bin: AStatisticsSupporter.HistogramBin = new AStatisticsSupporter.HistogramBin
      bin.setX(entry._1)
      bin.setY(entry._2.toDouble)
      bins.add(bin)
    }
    bins
  }

  override def getVectorApproxHistogram(column: String, numBins: Int): util.List[HistogramBin] = getVectorHistogram(column, numBins)

  def getDoubleColumn(ddf: DDF, columnName: String): Option[List[Double]] = {
    val schema = ddf.getSchema
    val column: Column = schema.getColumn(columnName)
    column.isNumeric match {
      case true =>
        val data = ddf.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult].result
        val colIndex = ddf.getSchema.getColumnIndex(columnName)
        val colData = data.map {
          x =>
            val elem = x(colIndex)
            val mayBeDouble = Try(elem.toString.trim.toDouble)
            mayBeDouble.getOrElse(0.0)
        }
        Option(colData)
      case false => Option.empty[List[Double]]
    }
  }

  val MAX_LEVEL_SIZE = Integer.parseInt(System.getProperty("factor.max.level.size", "1024"))

}
