package app

import dao.{ContainerVectorsDao, ContainerDataDao}
import org.apache.spark.sql.DataFrame
import utility.classes.CreateSparkContext

class MlLibHelper {

  (CreateSparkContext.CreateContext)

  val index = "Index"

  val scanColumn = "scan_ops"
  val scanLabel = "label"
  val scanEncodedColumn = "scan_ops_encode"
  val scanLabelIndex = scanLabel+index;

  val scanDataEncodedColumn = "features"
  val scanDataColumn = "data_values"
  val scanDataLabel = "data_label"
  val scanEncodedIndex = scanDataEncodedColumn+index

  val prediction = "prediction"
  val precision = "precision"

  val impurity = "gini"

  private val csc = CreateSparkContext.getCSContext
  private val cdd = new ContainerDataDao(csc)
  private val cvd = new ContainerVectorsDao

  private def insertVectors (column:String,df:DataFrame) {
    cvd.insert(column,df)
  }

  def getAndSaveData () = {
    val contr = cdd.getContainerInfo.cache
    val scanOpsEncoded = OneHotEncoder.encode(contr,scanColumn,scanLabel,scanEncodedColumn)
    val columnsEncoded = OneHotEncoder.encode(scanOpsEncoded,scanDataColumn,scanDataLabel,scanDataEncodedColumn)
    val distinctOpsValues = columnsEncoded.select(scanColumn,scanLabel).distinct()
    val distinctDataValues = columnsEncoded.select(scanDataColumn,scanDataLabel).distinct()
    insertVectors(scanColumn,distinctOpsValues)
    insertVectors(scanDataColumn,distinctDataValues)
    columnsEncoded.select(scanColumn,scanLabel,scanDataColumn,scanDataEncodedColumn)
  }

}
