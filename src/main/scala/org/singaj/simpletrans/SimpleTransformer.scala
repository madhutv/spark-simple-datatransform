package org.singaj.simpletrans

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.singaj.rules._
import org.singaj.utils.STUtils


/**
  * Created by madhu on 8/26/17.
  */

class SimpleTransformer(val ds: Dataset[_]) extends MapperConsts with STUtils {

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
          case SimpleTransformation(Some(DISTINCT), a, b) => stDistinct(a)(ds)
          case SimpleTransformation(a, b, c) => expression(b, c)(ds)
          case SplitTransformation(a, b, c) => stSplit(a, b, c)(ds)
          case AggTransformation(a, b, c, d, e) => stAggregate(a, b, c, d, e)(ds)
          case _ => ds
        }
        stTransform(xs)(append)
    }
  }

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
    * Filter records based on condition
    * @param cond: Filter condition
    * @param ds: Dataset to be filtered
    * @return : Trasformed dataset
    */
  def stFilter(cond: String)(implicit ds: Dataset[_] = this.ds): Dataset[_] ={
    ds.filter(cond)
  }

  /**
    * Selects fields specified
    * @param selStr: Array of columns to select
    * @param ds: Dataset on which transformation is to be performed.
    *              This is determined implicitly
    * @return
    */

  def stSelect(selStr: Option[String])(implicit ds: Dataset[_] = this.ds): Dataset[_] = {

     val selCols = strToArr(selStr)
    selCols match {
      case Array() => ds
      case _ => ds.selectExpr(selCols: _*)
    }

  }

  /**
    * Orderby based on string specified in JSON. Default will be ascending order
    * to sort by descending order, have DSC in the JSON
    * @param orderBy: String from JSON order by rule
    * @param ds: Dataset which needs to be sorted
    * @return :Dataset[_] Sorted dataset
    */
  def stOrderBy(orderBy: String)(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    //Split string by ,
    val obColumns = strToArr(orderBy)

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
    * Select distinct rows. If "all" is provided then, distict will be applied on all
    * columns. If specific columns are provided, distinct rows will be randomly selected
    * based on columns provided
    * @param dist: String Column seperated column names or "all"
    * @param ds: Dataset: Dataset to which distinct is to be applied
    * @return : Dataset Transformed dataset
    */
  def stDistinct(dist: String)(implicit ds: Dataset[_] = this.ds): Dataset[_] = {
    dist.trim match{
      case ALL => ds.distinct
      case a => ds.dropDuplicates(strToArr(a))
    }
  }



  /**
    * Handle split transformations. i.e. creating multiple rows
    * based on certain criteria
    * @param ds: Dataset[_] : Input dataset
    * @param cond: Condition to get records that need to be split
    * @param dest_row_trans: SimpleTransformation to be performed on new rows. This will be a
    *                      applied to records that satisfies cond
    * @param source_row_trans: SimpleTransformation: Transformations that need to be applied
    *                        on original rows that satisfied filter criteria
    * @return Returns transformed records
    */
  def stSplit(cond: String, dest_row_trans: List[SimpleTransformation],
                         source_row_trans: Option[List[SimpleTransformation]])
                        (ds: Dataset[_] = this.ds): Dataset[_] = {

     //filter rows that match condition specified
     val transOn = ds.where(cond)
     //Apply transformations on resulting rows
     val destRows = stTransform(dest_row_trans)(transOn)

    //check if transformations are required on source rows (original rows). If so,
    //perform transformations
     val sourceRows = stTransform(source_row_trans.getOrElse(List()))(transOn)
    //Get rest of the rows. These will be unioned with transformed rows
     val filterNotCond = dsRemoveOriginal(cond, ds)

     //if resulting rows do not match with source rows, throw an error
     checkColumnMatch(Array(sourceRows.columns, destRows.columns, filterNotCond.columns))

     mergeDS(filterNotCond, destRows, sourceRows)

  }

  /**
    * Aggregate based on rule. Perform transformations like sum, avg, count and so on
    * @param rule: Provided if aggregation need to be performed on a subset of records while
    *             keep rest of records intact
    * @param aggregates: Case class that holds columns, rules and name(if column to be renamed) as parameters
    * @param groupBy: Option groupBy clause, provided if group by is needed
    * @param trans: Additional transformations to be performed on aggregated rows
    * @param keepOriginal: Boolean : This is relevant if rule is not empty. i.e. if aggregation
    *                    needs to performed on subset of records. If keepOriginal is set to true,
    *                    transformed dataset will contain original row, aggregated transformed rows and
    *                    rest of the dataset. If set to false, this original rows will be dropped
    * @param ds: Dataset on which aggregation need to be performed
    * @return : Dataset[_]: aggregated dataset
    */
  def stAggregate(rule: String, aggregates: Aggregates,
                  groupBy: Option[String] = None,
                  trans: Option[List[SimpleTransformation]] = None,
                  keepOriginal: Option[Boolean] = Some(false))
                  (ds: Dataset[_] = this.ds): Dataset[_] = {

    //check if aggregation need to be performed on full dataset or subset of dataset
    val aggOnFull: Boolean = rule.trim == ""

    //get dataset on which aggregation need to be performed
    val transOn = if(aggOnFull) ds else ds.selectExpr(rule)

    //Get groupby, aggregation column, rules and Names as string
    val (groupCols, aggCols, aggRules, aggNames) = (
                                                     strToArr(groupBy).map(expr),
                                                     strToArr(aggregates.column),
                                                     strToArr(aggregates.rule),
                                                     strToArr(aggregates.names)
                                                   )

    //check if aggregation rules are provided by each aggregation columns
    passOrThrow("Aggregate: Number of columns and rules do not match"){
      () => aggCols.length == aggRules.length
    }

    //Get size of names and cols
    val (aggNamesSiz, aggColSiz) = (aggNames.length - 1, aggCols.length - 1)

    //build expression to be used in aggregate
    val aggExpr = for {
                        i <- (0 to aggColSiz).toArray
                        name = if(i <= aggNamesSiz) aggNames(i) else ""
                        exp = aggRules(i) + "(" + aggCols(i) +")" + name
                      } yield expr(exp)

    //workaround to send first column and set of the columns as aggregation takes column, column*
    val temp = aggExpr.drop(1)


    //check if groupBy clause is provided. If so, apply group by, else just aggregate
    val groupAgg: Dataset[_] = groupCols match {
      case Array() => transOn.agg(aggExpr(0), aggExpr(1))
      case _ =>  transOn.groupBy(groupCols: _*).agg(aggExpr(0), temp: _*)
    }

    //perform any data transformations if required after aggregation
    val dsAfterTrans = trans match {
      case None => groupAgg
      case Some(a) => stTransform(a)(groupAgg)
    }

    //check if any kind of merging is required.
    if(!aggOnFull){
      checkColumnMatch(Array(ds.columns, dsAfterTrans.columns))
      if(keepOriginal.getOrElse(false))
        mergeDS(ds, dsAfterTrans)
      else
        mergeDS(dsRemoveOriginal(rule.trim, ds), dsAfterTrans)
    }
    else{
      dsAfterTrans
    }

  }

  /**
    * Keeps the original records in ds or filters them out
    * @param cond: condition: records statisfying which, will be removed
    * @param ds: Dataset for transformation
    * @return :transformed dataset
    */
  private def dsRemoveOriginal(cond: String,  ds: Dataset[_]): Dataset[_] = {
    ds.where("not(" + cond + ")")
  }


  /**
    * Merges given dataset
    * @param ds : Datasets to be merged
    * @return Dataset[_]: Merged dataset
    */
  private def mergeDS(ds: Dataset[_]*) = {
    ds.reduce((ds1, ds2) => ds1.toDF() union ds2.toDF())
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
