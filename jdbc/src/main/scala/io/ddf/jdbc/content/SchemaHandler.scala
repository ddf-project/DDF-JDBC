package io.ddf.jdbc.content

import java.lang.{Integer => JInt}
import java.util
import java.util.{HashMap => JHMap, List => JList, Map => JMap}

import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnType
import io.ddf.datasource.SQLDataSourceDescriptor
import io.ddf.exception.DDFException
import io.ddf.{DDF, Factor}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import java.util.{List => JList}

class SchemaHandler(ddf: DDF) extends io.ddf.content.SchemaHandler(ddf: DDF) {

  @throws(classOf[DDFException])
  override def computeLevelCounts(columnNames: Array[String]): JMap[String, JMap[String, Integer]] = {
    val columnIndexes: util.List[Integer] = new util.ArrayList[Integer]
    val columnTypes: util.List[Schema.ColumnType] = new util.ArrayList[Schema.ColumnType]
    import scala.collection.JavaConversions._
    for (col <- this.getColumns) {
      if (col.getColumnClass eq Schema.ColumnClass.FACTOR) {
        this.setAsFactor(col.getName)
        columnIndexes.add(this.getColumnIndex(col.getName))
        columnTypes.add(col.getType)
      }
    }
    //loop through all factors and compute factor

    val listLevelCountsWithName: JMap[String, JMap[String, Integer]] = new JHMap[String, JMap[String, Integer]]
    columnIndexes.foreach(colIndex => {
      val col = this.getColumn(this.getColumnName(colIndex))

      val quotedColName = "\"" + col.getName() + "\""
      val command = s"select ${quotedColName}, count(${quotedColName}) from " +
        s"{1} group by ${quotedColName}"

      val sqlResult = this.getManager.sql(command, new SQLDataSourceDescriptor(command, null, null, null, this.getDDF.getUUID.toString))
      //JMap[String, Integer]
      var result = sqlResult.getRows()
      val levelCounts: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]()
      for (item <- result) {
        if (item.split("\t").length > 1) {
          levelCounts.put(item.split("\t")(0), Integer.parseInt(item.split("\t")(1)))
        }
        else {
          //todo log this properly
          this.mLog.debug("exception parsing item")
        }
        this.mLog.debug(item)
      }
      listLevelCountsWithName.put(col.getName, levelCounts)
    })
    listLevelCountsWithName
  }

  override protected def computeFactorLevels(columnName: String): JList[AnyRef] = {
    val countDistinctCommand = s"select count(distinct $columnName) from {1}"
    val result1 = this.getManager.sql(countDistinctCommand, new SQLDataSourceDescriptor(countDistinctCommand, null, null, null, this.getDDF.getUUID.toString))
    val countDistinct = result1.getRows.get(0).toLong
    if(countDistinct > Factor.getMaxLevelCounts) {
      throw new DDFException(s"Number of distinct values in column $columnName is $countDistinct larger than MAX_LEVELS_COUNTS = ${Factor.getMaxLevelCounts}")
    }
    val command = s"select $columnName from {1} group by $columnName"
    val result = this.getManager.sql(command, new SQLDataSourceDescriptor(command, null, null, null, this.getDDF.getUUID.toString))
    val rows = result.getRows
    rows.map{
      r => r.asInstanceOf[AnyRef]
    }.asJava
  }
}

