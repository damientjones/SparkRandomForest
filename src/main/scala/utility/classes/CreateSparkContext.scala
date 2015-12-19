/**
 * Created by damien on 9/12/2015.
 */

package utility.classes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.log4j.{Level, Logger}

object CreateSparkContext {

  private var sc : SparkContext = _
  private var conf : SparkConf = _
  private var csc : CassandraSQLContext = _

  def CreateContext {
    //defaults are testApp, local[2]
    if (sc == null){
      val level = Level.ERROR
      Logger.getLogger("org").setLevel(level)
      Logger.getLogger("akka").setLevel(level)
      conf = new SparkConf(true)
        .setAppName("testApp")
        .setMaster("local[2]")
        .set("spark.cassandra.connection.host","127.0.0.1")
        .set("spark.cassandra.auth.username","cassandra")
        .set("spark.cassandra.auth.password","cassandra")
      sc = new SparkContext(conf)
      csc = new CassandraSQLContext(sc)
    }
  }
  def getSparkContext : SparkContext = {
    sc
  }
  def getCSContext : CassandraSQLContext = {
    csc
  }
  def closeSparkContext {
    sc.stop()
  }
}