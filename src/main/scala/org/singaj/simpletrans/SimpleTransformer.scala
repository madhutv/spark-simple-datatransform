package org.singaj.simpletrans

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.singaj.rules._


/**
  * Created by madhu on 8/26/17.
  */

class SimpleTransformer(val ds: Dataset[_]) extends MapperConsts {

  /**
    * DirectMap is used when one column simply needs to be copied over to
    * another column.
    * @param source : Source column name
    * @param dest : Destination column name
    * @param ds :  Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return : Transformed Dataset
    */
  def directMap(source: String, dest: Option[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    expression(source, dest)
  }


  /**
    * DefaultMap is used when a default value is to be populated in all rows
    * @param value : Value to be populated
    * @param dest: Column name of destination
    * @param ds :  Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return : Transformed Dataset
    */
  def defaultMap(value: String, dest: Option[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
   val destColumn: String = getOrThrow(dest, "defaultMap requires dest column")
    ds.withColumn(destColumn , lit(value))
  }

  /**
    * Performs if else transformations
    * @example ifElse("case when a = 5 then a * 5 else a end", dest)
    *          The above statement will create a dest column such that if value
    *          in column a = 5 then dest will be 5 * 5 else dest will be value in
    *          column a
    * @param rule: String
    * @param dest: String Column name of destination
    * @param ds :  Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return Transformed Dataset
    */
  def ifElse( rule: String, dest: Option[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    expression(rule, dest)(ds)
  }

  /**
    * Concatenates Columns or Strings with column
    * @example concat("(stock, '*', currency, '$')", stockCurrency)
    *          This will create or replace a column stockCurrency with value as
    *          value in stock column + * + value in currency column + $
    * @param rule: String
    * @param dest: String Column name of destination
    * @param ds :  Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return Transformed Dataset
    */
  def concat(rule: String, dest: Option[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    expression("concat" + rule , dest)(ds)
  }

  /**
    * Populates column with expression provided.
    * @example expression(trim(" Jolly "), jolly) will create column or
    *          replace existing jolly column with Jolly
    * @param rule: String
    * @param dest: String Column name of destination
    * @param ds :  Dataset on which transformation is to be performed.
    *              This is determined implicitly

    * @return Transformed Dataset
    *
    */
  def expression(rule: String, dest: Option[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    val destColumn: String = getOrThrow(dest, "Expression requires dest column " + rule)
    ds.withColumn(destColumn, expr(rule))
  }


  /**
    * Performs transformations based on List of Transformations and dataset provided
    * @param trans: List of Transformations
    * @param ds :  Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return Dataset: Transformed Dataset
    */
  def stTransform(trans: List[Transformations])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    trans match {
      case Nil => ds
      case x::xs =>
        val append =  x match {
          case SimpleTransformation(Some(DEFAULT_MAP), a, b)  => defaultMap(a, b)(ds)
          case SimpleTransformation(Some(CONCAT), a, b) => concat(a, b)(ds)
          case SimpleTransformation(Some(WHERE), a, b) => stFilter(a)(ds)
          case SimpleTransformation(Some(DROP), a, b) => stFilter("not(" + a + ")")(ds)
          case SimpleTransformation(Some(ORDER_BY), a, b) => stOrderBy(a)(ds)
          case SimpleTransformation(a, b, c) => expression(b, c)(ds)
          case SplitTransformation(a, b, c) => splitTrans(ds, a, b, c)
          case _ => ds
        }
        stTransform(xs)(append)
    }
  }


  def stFilter(a: String)(implicit ds: Dataset[_] = this.ds): Dataset[_] ={
    ds.filter(a)
  }
  /**
    * Selects fields specified
    * @param selCols: Array of columns to select
    * @param ds: Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return
    */

  def stSelect(selCols: Array[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    selCols match {
      case Array() => ds
      case _ => ds.selectExpr(selCols: _*)
    }
  }

  /**
    * Orderby based on string specified in JSON. Default will be ascending order
    * to sort by descending order, have DSC in the JSON
    * @param selCols: String from JSON order by rule
    * @param ds: Dataset which needs to be sorted
    * @return :Dataset[_] Sorted dataset
    */
  def stOrderBy(selCols: String)(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    //Split string by ,
    val obColumns = selCols.split(",").map(_.trim)
    //Check if column string contains DSC
    val cols = obColumns.map(p => {
     if(p.contains("DSC"))
        column(p.split("\\s+")(0).trim).desc
      else
        column(p)
    })

    ds.orderBy(cols: _*)
  }



  /**
    * Private function to handle split transformations. i.e. creating multiple rows
    * based on certain criteria
    * @param ds: Dataset[_] : Input dataset
    * @param cond: Condition to get records that need to be split
    * @param dest_row_trans: SimpleTransformation to be performed on new rows. This will be a
    *                      applied to records that satisfies cond
    * @param source_row_trans: SimpleTransformation: Transformations that need to be applied
    *                        on original rows that satisfied filter criteria
    * @return Returns transformed records
    */
  private def splitTrans(ds: Dataset[_], cond: String, dest_row_trans: List[SimpleTransformation],
                         source_row_trans: Option[List[SimpleTransformation]]) = {
     ds.show
     val transOn = ds.where(cond)
     val destRows = stTransform(dest_row_trans)(transOn).toDF
     val sourceRows = stTransform(source_row_trans.getOrElse(List()))(transOn).toDF
     val filterNotCond = ds.where("not(" + cond + ")").toDF
     filterNotCond union destRows union sourceRows
  }

  private def getOrThrow[A](str: Option[A], msg: String): A = {
    if(str == None)
      throw new Error("Did not find required details " + msg)
    else
      str.get
  }

}

/**
  * Used for implicit conversion from dataset to SimpleTransformer
  */
object SimpleTransformer{
  /**
    * Implicit conversion from Dataset to SimpleTransformer
    * @param ds: Input Dataset
    * @return SimpliTransfomer wrapping Dataset
    */
  implicit def datasetToSimTrans(ds: Dataset[_]): SimpleTransformer = new SimpleTransformer(ds)
}
