package utility.classes

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Created by damien.jones on 12/20/2015.
 */
trait PropertiesHelper {

  protected val index = "_index"
  protected val vector = "_vector"
  protected val features = "_features"
  protected val label = "_label"

  protected val predictColumn = "scan_ops"
  protected val predictionFeatures = "data_features"

  protected val dataColumns = "mail_owner,entry_zip,entry_wndw,sortation_level"
  protected val columns : Array[String] = dataColumns.split(",")
  protected val columnsVectors : Array[String] = columns.map(x => x+vector)

  protected val prediction = "prediction"
  protected val precision = "precision"

  protected val impurity = "gini"

  protected val numTrees = 100
  protected val maxBins = 10
  protected val maxDepth = 10
  protected val maxCategories = 32

  protected def getLabel (column:String) = {column + label}

  protected def getVector (column:String) = {column + vector}

  protected def getIndex (column:String) = {column + index}

  protected def getLabelAndVector (column:String) : (String,String) = { (getLabel(column), getVector(column)) }

}
