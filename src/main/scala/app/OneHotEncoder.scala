package app

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.DataFrame

/**
 * Created by damien.jones on 12/13/2015.
 */
object OneHotEncoder {
  def encode (df:DataFrame,inputColumnName:String,labelColumn:String,outputColumnName:String) = {
    val indexer = new StringIndexer()
      .setInputCol(inputColumnName)
      .setOutputCol(labelColumn)
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder().setInputCol(labelColumn).
      setOutputCol(outputColumnName)
    val encoded = encoder.transform(indexed)
    encoded
  }
}
