package org.singaj.rules

/**
  * Created by madhu on 8/26/17.
  */
class Transformations
case class SimpleTransformation(ttype: Option[String], rule: String, dest: Option[String]) extends Transformations{
  ttype.getOrElse("Expression")
}

/**
  * Case class for SplitTransformation
  * @param name Name of Split transaction
  * @param dest_row_trans: List of simpleTransformations
  * @param source_row_trans: List os SimpleTransformation to source
  */
case class SplitTransformation(name: String, dest_row_trans: List[SimpleTransformation],
                      source_row_trans: Option[List[SimpleTransformation]]) extends Transformations


case class AggTransformation(name: String,
                             aggregates: Aggregates,
                             groupBy: Option[String],
                             additional_trans: Option[List[SimpleTransformation]],
                             keepOriginal: Option[Boolean]
                            ) extends Transformations

case class Aggregates(column: String, rule: String, names: Option[String])