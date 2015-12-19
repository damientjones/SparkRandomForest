package dao

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.datastax.spark.connector._

class ContainerVectorsDao {
  private val keyspace = "ml_test"
  private val table = "container_vectors"
  private val columns = SomeColumns("column","value_data","index_data")
  def insert(column:String,df:DataFrame) {
    val col = column;
    val rdd : RDD[(String,String,Double)] = df.rdd.map(x=> (col,x.getString(0),x.getDouble(1)))
    rdd.saveToCassandra(keyspace,table,columns)
  }
}
