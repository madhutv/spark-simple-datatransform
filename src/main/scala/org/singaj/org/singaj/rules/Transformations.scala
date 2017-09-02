package org.singaj.rules

/**
  * Created by madhu on 8/26/17.
  */
case class Transformations(ttype: Option[String], rule: String, dest: String){
  ttype.getOrElse("Expression")
}
