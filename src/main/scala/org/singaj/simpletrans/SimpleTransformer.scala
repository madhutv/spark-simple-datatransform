package org.singaj.simpletrans

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.singaj.rules.{MapperConsts, Transformations}


/**
  * Created by madhu on 8/26/17.
  */

class SimpleTransformer {

  implicit def datasetToSimTrans(ds: Dataset[_]) = SimpleTransformer

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

  /**
    * Performs if else transformations
    * @example ifElse(ds, "case when a = 5 then a * 5 else a end", dest)
    *          The above statement will create a dest column such that if value
    *          in column a = 5 then dest will be 5 * 5 else dest will be value in
    *          column a
    * @param ds: Dataset[_] Dataset on which the transformation needs to be performed
    * @param rule: String
    * @param dest: String Column name of destination
    * @return Transformed Dataset
    */
  def ifElse(ds: Dataset[_], rule: String, dest: String): Dataset[_] = {
    expression(ds, rule, dest)
  }

  /**
    * Concatenates Columns or Strings with column
    * @example concat(ds, "(stock, '*', currency, '$')", stockCurrency)
    *          This will create or replace a column stockCurrency with value as
    *          value in stock column + * + value in currency column + $
    * @param ds: Dataset[_] Dataset on which the transformation needs to be performed
    * @param rule: String
    * @param dest: String Column name of destination
    * @return Transformed Dataset
    */
  def concat(ds: Dataset[_], rule: String, dest: String): Dataset[_] = {
    expression(ds, "concat" + rule , dest)
  }

  /**
    * Populates column with expression provided.
    * @example expression(ds, trim(" Jolly "), jolly) will create column or
    *          replace existing jolly column with Jolly
    * @param ds: Dataset[_] Dataset on which the transformation needs to be performed
    * @param rule: String
    * @param dest: String Column name of destination
    * @return Transformed Dataset
    *
    */
  def expression(ds: Dataset[_], rule: String, dest: String): Dataset[_] = {
    ds.withColumn(dest, expr(rule))
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
          case Transformations(Some(DIRECT_MAP), a, b) => directMap(ds, a, b)
          case Transformations(Some(DEFAULT_MAP), a, b)  => defaultMap(ds, a, b)
          case Transformations(Some(IF_ELSE) | Some(EXPRESSION), a, b)  => expression(ds, a, b)
          case Transformations(Some(CONCAT), a, b) => concat(ds, a, b)
          case Transformations(a, b, c) => expression(ds, b, c)
          case _ => ds
        }
        transform(xs, append)
      }
    }
  }



  def select(ds: Dataset[_], selCols: Array[String]): Dataset[_] = {
    selCols match {
      case Array() => ds
      case _ => ds.selectExpr(selCols: _*)
    }
  }

}
