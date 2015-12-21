package app

import dao.{ContainerVectorsDao, ContainerDataDao}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import utility.classes.{PropertiesHelper, CreateSparkContext}

class MlLibHelper extends PropertiesHelper {

  CreateSparkContext.CreateContext

  private val csc = CreateSparkContext.getCSContext
  private val cdd = new ContainerDataDao(csc)
  private val cvd = new ContainerVectorsDao

  private def insertVectors (column:String,df:DataFrame) {
    cvd.insert(column,df)
  }

  private def createVector (df : DataFrame, column:String) : DataFrame = {
    val (localLabel, localVector) = getLabelAndVector(column)
    OneHotEncoder.encode(df,column,localLabel,localVector)
  }

  private def processVectors (df : DataFrame) : DataFrame = {
    var vectorDf = df
    columns.map(x=> vectorDf = createVector(vectorDf,x))
    val assembler = new VectorAssembler()
      .setInputCols(columnsVectors)
      .setOutputCol(predictionFeatures)
    assembler.transform(vectorDf)
  }

  protected def getAndSaveData () = {
    val contr = cdd.getContainerInfo.cache

    //Add all columns used for prediction to the vector and create a label for the prediction column
    val columnsEncoded = createVector(processVectors(contr),predictColumn)

    //val distinctOpsValues = columnsEncoded.select(scanColumn,scanLabel).distinct()
    //val distinctDataValues = columnsEncoded.select(scanDataColumn,scanDataLabel).distinct()
    //insertVectors(scanColumn,distinctOpsValues)
    //insertVectors(scanDataColumn,distinctDataV
    columnsEncoded.select(predictionFeatures).foreach(println)

    columnsEncoded
  }

}
