package io.ddf.jdbc.content

import java.io.File
import java.util

import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.content.Schema.Column
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.{DDF, DDFManager}
import org.scalatest.FlatSpec

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


trait ContentBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithAddressing(implicit l: Loader): Unit = {
    val ddf = l.loadAirlineDDF()

    it should "load data from file" in {
      ddf.getNamespace should be("adatao")
      ddf.getColumnNames should have size 29

    }

    it should "load data from file using loadTable" in {
      val filePath = getClass.getResource("/airline.csv").getPath
      val ddf = l.jdbcDDFManager.loadTable(filePath, ",")
      ddf.getNamespace should be("adatao")
      ddf.getColumnNames should have size 29

    }

    it should "be addressable via URI" in {
      ddf.getUri should be("ddf://" + ddf.getNamespace + "/" + ddf.getName)
      l.jdbcDDFManager.getDDFByURI("ddf://" + ddf.getNamespace + "/" + ddf.getName) should be(ddf)
    }
  }

  def ddfWithMetaDataHandler(implicit l: Loader): Unit = {
    it should "get number of rows" in {
      val ddf = l.loadAirlineDDF()
      ddf.getNumRows should be(31)
    }
  }

  def ddfWithPersistenceHandler(implicit l: Loader): Unit = {
    it should "hold namespaces correctly" in {
      val manager: DDFManager = DDFManager.get(l.engine,l.jdbcDDFManager.getDataSourceDescriptor)
      val ddf: DDF = manager.newDDF

      val namespaces = ddf.getPersistenceHandler.listNamespaces

      namespaces should not be null
      for (namespace <- namespaces.asScala) {
        val ddfs = ddf.getPersistenceHandler.listItems(namespace)
        ddfs should not be null
      }

    }

    ignore should "persist and unpersist a jdbc DDF" in {
      val manager: DDFManager = DDFManager.get(l.engine,l.jdbcDDFManager.getDataSourceDescriptor)
      val ddf: DDF = manager.newDDF
      val uri: PersistenceUri = ddf.persist
      uri.getEngine should be(l.engine.toString)
      new File(uri.getPath).exists() should be(true)
      ddf.unpersist()
    }
  }

  def ddfWithSchemaHandler(implicit l: Loader): Unit = {
    val manager = l.jdbcDDFManager
    val ddf = l.loadAirlineDDF()
    it should "get schema" in {
      ddf.getSchema should not be null
    }

    it should "get columns" in {
      val columns: util.List[Column] = ddf.getSchema.getColumns
      columns should not be null
      columns.length should be(29)
    }

    it should "get columns for sql2ddf create table" in {
      val ddf = l.loadAirlineDDF()
      val columns = ddf.getSchema.getColumns
      columns should not be null
      columns.length should be(29)
      columns.head.getName.toUpperCase should be("YEAR")
    }

    it should "test get factors on DDF" in {
      val ddf = l.loadMtCarsDDF()
      val schemaHandler = ddf.getSchemaHandler
      val columnNames = Array("vs", "am", "gear", "carb")
      columnNames.foreach {
        col => schemaHandler.setAsFactor(col)
      }
      val result = schemaHandler.computeLevelCounts(columnNames)
      val columns = columnNames.map {
        col => schemaHandler.getColumn(col)
      }
      val factorMap = schemaHandler.computeLevelCounts(columnNames)

      assert(factorMap.get("vs").get("1") === 14)
      assert(factorMap.get("vs").get("0") === 18)
      assert(factorMap.get("am").get("1") === 13)
      assert(factorMap.get("gear").get("4") === 12)

      assert(factorMap.get("gear").get("3") === 15)
      assert(factorMap.get("gear").get("5") === 5)
      assert(factorMap.get("carb").get("1") === 7)
      assert(factorMap.get("carb").get("2") === 10)
    }


    it should "test get factors" in {
      val ddf = manager.sql2ddf("select * from ddf://adatao/mtcars")
      val schemaHandler = ddf.getSchemaHandler
      val columnNames = Array("vs", "am", "gear", "carb")
      columnNames.foreach {
        col => schemaHandler.setAsFactor(col)
      }
      val result = schemaHandler.computeLevelCounts(columnNames)
      val columns = columnNames.map {
        col => schemaHandler.getColumn(col)
      }
      val factorMap = schemaHandler.computeLevelCounts(columnNames)

      assert(factorMap.get("vs").get("1") === 14)
      assert(factorMap.get("vs").get("0") === 18)
      assert(factorMap.get("am").get("1") === 13)
      assert(factorMap.get("gear").get("4") === 12)

      assert(factorMap.get("gear").get("3") === 15)
      assert(factorMap.get("gear").get("5") === 5)
      assert(factorMap.get("carb").get("1") === 7)
      assert(factorMap.get("carb").get("2") === 10)
    }

    it should "test NA handling" in {
      val ddf = l.loadAirlineNADDF()
      val schemaHandler = ddf.getSchemaHandler
      val columnNames = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler.getColumnName(idx)
      }
      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      val cols = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      val factorMap = schemaHandler.computeLevelCounts(columnNames)
      val levels = schemaHandler.computeFactorLevels(cols(0).getName)
      assert(levels.contains("2008"))
      assert(levels.contains("2010"))
      assert(factorMap.get(columnNames(3)).get("MCO") === 3.0)
      assert(factorMap.get(columnNames(3)).get("TPA") === 3.0)
      assert(factorMap.get(columnNames(3)).get("JAX") === 1.0)
      assert(factorMap.get(columnNames(3)).get("LAS") === 3.0)
      assert(factorMap.get(columnNames(3)).get("BWI") === 10.0)
      assert(factorMap.get(columnNames(5)).get("0") === 9.0)
      assert(factorMap.get(columnNames(4)).get("3") === 1.0)
    }
  }

  def ddfWithViewHandler(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "project after remove columns " in {
      val ddf = airlineDDF
      val columns: java.util.List[String] = new java.util.ArrayList()
      columns.add("newYear")
      columns.add("newMonth")
      columns.add("newDeptime")

      val transformList = new util.ArrayList[String]()
      transformList.add("newYear=year")
      transformList.add("newMonth=month")
      transformList.add("newDeptime=deptime")
      val transformDDF = ddf.transform(transformList)
      transformDDF.getNumColumns should be (32)

      val newddf1: DDF = transformDDF.VIEWS.removeColumn("newYear")
      newddf1.getNumColumns should be(31)
      val newddf2: DDF = transformDDF.VIEWS.removeColumns("newYear", "newDeptime")
      newddf2.getNumColumns should be(30)
      val newddf3: DDF = transformDDF.VIEWS.removeColumns(columns)
      newddf3.getNumColumns should be(29)
    }
  }
}
