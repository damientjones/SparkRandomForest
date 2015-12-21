package dao

import org.apache.spark.sql.cassandra.CassandraSQLContext
import utility.classes.PropertiesHelper

class ContainerDataDao (val csc:CassandraSQLContext) extends PropertiesHelper {
  private val table = "ml_test.container"
  private def getData(sql:String) = {
    val cSql = sql
    csc.cassandraSql(cSql)
  }
  /** Returns dataframe based on select statement **/
   def getContainerInfo = {
      val sql = s"SELECT * FROM $table"
      getData(sql)
   }
}
