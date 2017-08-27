package org.singaj.simpletrans

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.singaj.rules.{MapperConsts, Transformations}


/**
  * Created by madhu on 8/26/17.
  */

class SimpleTransformer {

  /**
    * DirectMap is used when one column simply needs to be copied over to
    * another column.
    * @param ds : Dataset on which transformation is to be performed
    * @param source : Source column name
    * @param dest : Destination column name
    * @return : Transformed Dataset
    */
  def directMap(ds: Dataset[_], source: String, dest: String): Dataset[_] = {
    ds.withColumn(dest, col(source))
  }

  /**
    * DefaultMap is used when a default value is to be populated in all rows
    * @param ds : Dataset on which the transformation needs to be performed
    * @param value : Value to be populated
    * @param dest: Column name of destination
    * @return : Transformed Dataset
    */
  def defaultMap(ds: Dataset[_], value: String, dest: String): Dataset[_] = {
    ds.withColumn(dest, lit(value))
  }

}

object SimpleTransformer extends SimpleTransformer with MapperConsts{

  /**
    * Performs transformations based on List of Transformations and dataset provided
    * @param trans: List of Transformations
    * @param ds: Dataset on which to perform transformations
    * @return Dataset: Transformed Dataset
    */
  def transform(trans: List[Transformations], ds: Dataset[_]): Dataset[_] = {
    trans match {
      case Nil => ds
      case x::xs => {
        val append =  x match {
          case Transformations(DIRECT_MAP, x, y) => directMap(ds, x, y)
          case Transformations(DEFAULT_MAP, x, y) => defaultMap(ds, x, y)
          case _ => ds
        }
        transform(xs, append)
      }
    }
  }

}
