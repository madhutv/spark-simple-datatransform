package org.singaj.simpletrans

import org.apache.spark.sql.Dataset

/**
  * Created by madhu on 9/16/17.
  */
class ImplSimpleTransformer(ds: Dataset[_]) extends SimpleTransformer(ds)



/**
  * Used for implicit conversion from dataset to SimpleTransformer
  */
object ImplSimpleTransformer{
  /**
    * Implicit conversion from Dataset to SimpleTransformer
    * @param ds: Input Dataset
    * @return SimpliTransfomer wrapping Dataset
    */
  implicit def datasetToSimTrans(ds: Dataset[_]): ImplSimpleTransformer = new ImplSimpleTransformer(ds)
}

