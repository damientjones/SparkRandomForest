package dao

import org.apache.spark.sql.cassandra.CassandraSQLContext

class ContainerDataDao (val csc:CassandraSQLContext) {
  private val table = "ml_test.container_concat"
  private def getData(sql:String) = {
    val cSql = sql
    csc.cassandraSql(cSql)
  }
  /** Returns dataframe based on select statement **/
   def getContainerInfo = {
      val sql = s"SELECT data_values, scan_ops FROM $table"
      getData(sql)
   }
}
