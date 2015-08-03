package io.ddf.jdbc.etl

import java.util.Collections

import io.ddf.DDF
import io.ddf.etl.Types.JoinType
import io.ddf.jdbc.BaseSpec
import io.ddf.jdbc.content.{Representations, SqlArrayResult}

import scala.collection.JavaConversions._

class JoinHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  it should "inner join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, null, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
    val collection = rep.result
    collection.foreach(i => println("[" + i.mkString(",") + "]"))
    val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getTableName, "Error").getRows)
    list.size should be(2) // only 2 values i.e 2008 and 2010 have values in both tables
    rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("YEAR") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("R_NAME") should be(true)

  }

  /*it should "left semi join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.LEFTSEMI, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
    val collection = rep.result
    collection.foreach(i => println("[" + i.mkString(",") + "]"))
    val list = seqAsJavaList(collection)
    list.size should be(2) // only 2 values i.e 2008 and 2010 have values in both tables
    val first = list.get(0)
    rep.schema.getNumColumns should be(29) //only left columns should be fetched
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("YEAR") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("R_NAME") should be(false)

  }*/

  it should "left outer join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.LEFT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
    val collection = rep.result
    collection.foreach(i => println("[" + i.mkString(",") + "]"))
    val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getTableName, "Error").getRows)
    list.size should be(3) // 3 distinct values in airline years 2008,2009,2010
    val first = list.get(0)
    rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("YEAR") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("R_NAME") should be(true)
  }

  /*it should "right outer join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.RIGHT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
    val collection = rep.result
    collection.foreach(i => println("[" + i.mkString(",") + "]"))
    val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getTableName, "Error").getRows)
    list.size should be(4) // 4 distinct values in airline years 2007,2008,2010,2011
    val first = list.get(0)
    rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("YEAR") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("R_NAME") should be(true)
  }

  it should "full outer join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.FULL, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult]
    val collection = rep.result
    val list = seqAsJavaList(joinedDDF.sql("SELECT DISTINCT YEAR FROM " + joinedDDF.getTableName, "Error").getRows)
    list.size should be(5) //over all 5 distinct years 2007 - 2011 across both tables
    val first = list.get(0)
    rep.schema.getNumColumns should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("YEAR") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("R_NAME") should be(true)

  }*/
}
