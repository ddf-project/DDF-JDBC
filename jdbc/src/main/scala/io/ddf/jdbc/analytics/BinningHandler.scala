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


  override def binningImpl(column: String, binningTypeString: String, numBins: Int, inputBreaks: Array[Double], includeLowest: Boolean,
                           right: Boolean): DDF = {

    val colMeta = ddf.getColumn(column)

    val binningType = BinningType.get(binningTypeString)

    breaks = inputBreaks

    binningType match {
      case BinningType.CUSTOM ⇒
        if (breaks == null) throw new DDFException("Please enter valid break points")
        if (breaks.sorted.deep != breaks.deep) throw new DDFException("Please enter increasing breaks")
      case BinningType.EQUAlFREQ ⇒ breaks = {
        if (numBins < 2) throw new DDFException("Number of bins cannot be smaller than 2")
        getQuantilesFromNumBins(colMeta.getName, numBins)
      }
      case BinningType.EQUALINTERVAL ⇒ breaks = {
        if (numBins < 2) throw new DDFException("Number of bins cannot be smaller than 2")
        getIntervalsFromNumBins(colMeta.getName, numBins)
      }
      case _ ⇒ throw new DDFException(String.format("Binning type %s is not supported", binningTypeString))
    }
    //    mLog.info("breaks = " + breaks.mkString(", "))

    var intervals = createIntervals(breaks, includeLowest, right)

    val newDDF= ddf.sql2ddf(createTransformSqlCmd(column, intervals, includeLowest, right))

    //remove single quote in intervals
    intervals = intervals.map(x ⇒ x.replace("'", ""))
    val levels = intervals.map{i => i.asInstanceOf[AnyRef]}.toList.asJava
    newDDF.getSchemaHandler.setAsFactor(column).setLevels(levels)
    newDDF
  }

  def createIntervals(breaks: Array[Double], includeLowest: Boolean, right: Boolean): Array[String] = {
    val decimalPlaces: Int = 2
    val formatter = new DecimalFormat("#." + Iterator.fill(decimalPlaces)("#").mkString(""))
    var intervals: Array[String] = null
    intervals = (0 to breaks.length - 2).map {
      i ⇒
        if (right)
          "'(%s,%s]'".format(formatter.format(breaks(i)), formatter.format(breaks(i + 1)))
        else
          "'[%s,%s)'".format(formatter.format(breaks(i)), formatter.format(breaks(i + 1)))
    }.toArray
    if (includeLowest) {
      if (right)
        intervals(0) = "'[%s,%s]'".format(formatter.format(breaks(0)), formatter.format(breaks(1)))
      else
        intervals(intervals.length - 1) = "'[%s,%s]'".format(formatter.format(breaks(breaks.length - 2)), formatter.format(breaks(breaks.length - 1)))
    }
    mLog.info("interval labels = {}", intervals)
    intervals
  }

  def createTransformSqlCmd(columnName: String, intervals: Array[String], includeLowest: Boolean, right: Boolean): String = {
    val sqlCmd = "SELECT %s FROM %s".format(ddf.getSchemaHandler.getColumns.map {
      column ⇒
        if (!columnName.equals(column.getName)) {
          column.getName
        }
        else {
          val b = breaks.map(_.asInstanceOf[Object])
          val col = column.getName
          val caseLowest = if (right) {
            if (includeLowest)
              String.format("when ((%s >= %s) and (%s <= %s)) then %s ", col, b(0), col, b(1), intervals(0))
            else
              String.format("when ((%s > %s) and (%s <= %s)) then %s ", col, b(0), col, b(1), intervals(0))
          }
          else {
            String.format("when ((%s >= %s) and (%s < %s)) then %s ", col, b(0), col, b(1), intervals(0))
          }

          // all the immediate breaks
          val cases = (1 to breaks.length - 3).map {
            i ⇒
              if (right)
                String.format("when ((%s > %s) and (%s <= %s)) then %s ", col, b(i), col, b(i + 1), intervals(i))
              else
                String.format("when ((%s >= %s) and (%s < %s)) then %s ", col, b(i), col, b(i + 1), intervals(i))
          }.mkString(" ")

          val caseHighest = if (right) {
            String.format("when ((%s > %s) and (%s <= %s)) then %s ", col, b(b.length - 2), col, b(b.length - 1), intervals(intervals.length - 1))
          }
          else {
            if (includeLowest)
              String.format("when ((%s >= %s) and (%s <= %s)) then %s ", col, b(b.length - 2), col, b(b.length - 1), intervals(intervals.length - 1))
            else
              String.format("when ((%s >= %s) and (%s < %s)) then %s ", col, b(b.length - 2), col, b(b.length - 1), intervals(intervals.length - 1))
          }

          // the full case expression under select
          "case " + caseLowest + cases + caseHighest + " else null end as " + col
        }
    }.mkString(", "), ddf.getUri)
    mLog.info("Transform sql = {}", sqlCmd)

    sqlCmd
  }

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

  def getIntervalsFromNumBins(colName: String, bins: Int): Array[Double] = {
    val cmd = "SELECT min(%s), max(%s) FROM %s".format(colName, colName, ddf.getUri)
    val res: Array[Double] = ddf.sql(cmd, "").getRows.get(0).split("\t").map(x ⇒ x.toDouble)
    val (min, max) = (res(0), res(1))
    val eachInterval = (max - min) / bins
    val probs: Array[Double] = Array.fill[Double](bins + 1)(0)
    var i = 0
    while (i < bins + 1) {
      probs(i) = min + i * eachInterval
      i += 1
    }
    probs(bins) = max
    probs
  }

  def getQuantilesFromNumBins(colName: String, bins: Int): Array[Double] = {
    val eachInterval = 1.0 / bins
    val probs: Array[Double] = Array.fill[Double](bins - 1)(0.0)
    var i = 0
    while (i < bins - 1) {
      probs(i) = (i + 1) * eachInterval
      i += 1
    }
    getQuantiles(colName, probs)
  }

  //override in implementation
  protected def getQuantiles(colName: String, pArray: Array[Double]): Array[Double] = {
    ddf.getStatisticsSupporter.getVectorQuantiles(colName, pArray.map {
      i =>
        val value: lang.Double = i
        value
    }).map(_.doubleValue())
  }

  val MAX_LEVEL_SIZE = Integer.parseInt(System.getProperty("factor.max.level.size", "1024"))

}
