package org.singaj.rules

/**
  * Created by madhu on 8/26/17.
  */
class Transformations
case class SimpleTransformation(ttype: Option[String], rule: String, dest: Option[String]) extends Transformations{
  ttype.getOrElse("Expression")
}

case class SplitTransformation(name: String, dest_row_trans: List[SimpleTransformation],
                      source_row_trans: Option[List[SimpleTransformation]]) extends Transformations

