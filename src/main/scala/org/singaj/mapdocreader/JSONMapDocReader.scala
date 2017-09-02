package org.singaj.mapdocreader

import scala.io.Source.fromFile
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.types.StructType
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import org.singaj.rules.MapperConsts
import org.singaj.rules.{FieldStructure, Transformations}


/**
  * Created by madhu on 8/26/17.
  */
class JSONMapDocReader(val filePath: String) extends MapDocReader with MapperConsts{

  implicit val formats = DefaultFormats
  /**
    * @constructor Reads file from given path.
    *              On success, parses json file and stores in jsonDoc
    *              On Failure, throws and error
    */
  protected lazy val fileContents: Try[String] = Try(fromFile(filePath).getLines().mkString)
  val jsonDoc = fileContents match {
    case Failure(f) => throw new Error("Cant find file " + filePath +  f)
    case Success(json) => parse(json)
  }

  /**
    * Gets transformations defined in JSON file
    * See resources/sample.json for sample JSON format.
    * In general, tranformations will be within an array element "transforms"
    * Each object within this array may have either 2 or 3 parameters:
    *   ttype: Specifies type of tranformations like DirectMap, DefaultValue or Expression
    *          This element is optional, and if not specified Expression will be assumed
    *   rule: Will contain transformation rule
    *   dest: Name of destination column
    * @return List[Transformations]: List of transformations
    */
  def getTransformations: List[Transformations] = {
    Try {
      val trans= jsonDoc \ TRANSFORMS
      trans.extract[List[Transformations]]
    } match {
      case Failure(f) => println("No transformations specified"); List()
      case Success(t) => t
    }

  }

  /**
    * Gets the columns to be selected.
    * @example : Example of json format: "select": "UPQ1 as UPQ3, UPQ2, PstockCode, Q1, Q1to9 as Q1to92"
    * @return Array of columns name to be selected
    */
  def getSelectColumns: Array[String] = {
    Try{
      val sel= jsonDoc \ SELECT
      sel.extract[String].split(",").map(_.trim)
    } match {
      case Failure(f) => println("Din't find select statement"); Array()
      case Success(s) => s
    }
  }

  /**
    * Reads Field Structure from json and builds StructType. See resources/sample.json for
    * sample json file
    * @return StructType: On Error, Empty StructType will be returned
    *                     On Success, StructType will be generated
    */
  def getFieldStructure: StructType = {
    Try{
      val structInfo= jsonDoc \ STRUCT
      structInfo.extract[List[FieldStructure]]
    } match {
      case Failure(f) =>
        println("Could not retrieve structure proceeding without ", f)
        StructType(List())
      case Success(struct) => StructType(structFieldBuilder(struct, List()))
    }
  }

}
