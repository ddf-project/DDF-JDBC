package io.ddf.jdbc.etl

import java.util

import io.ddf.DDF
import io.ddf.etl.IHandleJoins
import io.ddf.etl.Types.JoinType
import io.ddf.exception.DDFException
import io.ddf.misc.ADDFFunctionalGroupHandler

import scala.collection.JavaConversions._

class JoinHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleJoins {

  @throws(classOf[DDFException])
  override def join(anotherDDF: DDF, joinTypeParam: JoinType, byColumns: util.List[String],
                    byLeftColumns: util.List[String], byRightColumns: util.List[String]): DDF = {
    this.join(anotherDDF, joinTypeParam, byColumns, byLeftColumns, byRightColumns, null, null)
  }

  @throws(classOf[DDFException])
  override def join(anotherDDF: DDF, joinTypeParam: JoinType, byColumns: util.List[String],
                    byLeftColumns: util.List[String], byRightColumns: util.List[String],
                    leftSuffixParam: String, rightSuffixParam: String): DDF = {
    val joinType = if (joinTypeParam == null) JoinType.INNER else joinTypeParam
    val leftTableName: String = getDDF.getUri
    val rightTableName: String = anotherDDF.getUri
    val rightColumns: util.List[String] = anotherDDF.getSchema.getColumns.map(_.getName)
    val leftColumns: util.List[String] = getDDF.getSchema.getColumns.map(_.getName)

    var joinConditionString: String = ""
    if (byColumns != null && byColumns.nonEmpty) {
      byColumns.foreach(col => joinConditionString += String.format("lt.%s = rt.%s AND ", col, col))
    }
    else {
      if (byLeftColumns != null && byRightColumns != null && byLeftColumns.size == byRightColumns.size && byLeftColumns.nonEmpty) {
        for ( (leftCol, rightCol) <- byLeftColumns zip byRightColumns ) yield {joinConditionString += String.format("lt.%s = rt.%s AND ", leftCol, rightCol)}
      }
      else {
        throw new DDFException(String.format("Left and right column specifications are missing or not compatible"), null)
      }
    }
    joinConditionString = joinConditionString.substring(0, joinConditionString.length - 5)

    // Add suffix to overlapping columns, use default if not provided
    val leftSuffix = if (leftSuffixParam == null || leftSuffixParam.trim.isEmpty) "_l" else leftSuffixParam
    var rightSuffix = if (rightSuffixParam == null || rightSuffixParam.trim.isEmpty) "_r" else rightSuffixParam

    val leftSelectColumns = generateSelectColumns(leftColumns, rightColumns, "lt", leftSuffix)
    val rightSelectColumns = generateSelectColumns(rightColumns, leftColumns, "rt", rightSuffix)

    val executeCommand =
      if (JoinType.LEFTSEMI equals joinType) {
        String.format("SELECT lt.* FROM %s lt %s JOIN %s rt ON (%s)", leftTableName, joinType.getStringRepr, rightTableName, joinConditionString)
      }
      else {
        String.format("SELECT %s,%s FROM %s lt %s JOIN %s rt ON (%s)", leftSelectColumns, rightSelectColumns, leftTableName, joinType.getStringRepr,
          rightTableName, joinConditionString)
      }
    this.getManager.sql2ddf(executeCommand)
  }

  override def merge(anotherDDF: DDF): DDF = {
    val sql = String.format("SELECT * from %s UNION ALL SELECT * from %s", ddf.getUri, anotherDDF.getTableName)
    ddf.sql2ddf(sql)
  }

  private def generateSelectColumns(targetColumns: util.List[String], filterColumnsParam: util.List[String], columnId: String, suffix: String): String = {
    var selectColumns: String = ""
    if (targetColumns == null) {
      selectColumns
    }

    val filterColumns = if (filterColumnsParam == null) new util.ArrayList[String] else filterColumnsParam

    targetColumns.foreach(colName =>
      if (filterColumns.contains(colName)) {
        selectColumns += String.format("%s.%s AS %s%s,", columnId, colName, colName, suffix)
      }
      else {
        selectColumns += String.format("%s.%s,", columnId, colName)
      }
    )
    if (selectColumns.length > 0) {
      selectColumns = selectColumns.substring(0, selectColumns.length - 1)
    }

    selectColumns
  }
}
