package org.singaj.mapdocreader

import scala.io.Source.fromFile
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.types.StructType
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.singaj.rules._
import org.singaj.utils.STUtils


/**
  * Created by madhu on 8/26/17.
  */
class JSONMapDocReader(val jsonString: String) extends MapDocReader with MapperConsts {

  implicit val formats = DefaultFormats

  /**
    * Get Input output file formats if needed
    */
  lazy val ioFileFormat: Map[String, String] = getIOFileFormat

  /**
    * @constructor Reads file from given path.
    *              On success, parses json file and stores in jsonDoc
    *              On Failure, throws and error
    */
  val jsonDoc: JValue = parse(jsonString)

  /**
    * Get inputout file formats from JSON.
    * @return
    */
  def getIOFileFormat: Map[String, String] = {
    Try {
      val trans= jsonDoc \ IO_FILE_FORMAT
      trans.extract[Map[String, String]]
    } match {
      case Failure(f) => println("No File Structure specified, using defaults ", f); Map()
      case Success(t) => t
    }
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
    * @return List[SimpleTransform]: List of transformations
    */
   def getTransformations: List[SimpleTransformation] = {
    Try {
      val trans= jsonDoc \ TRANSFORMS
      trans.extract[List[SimpleTransformation]]
    } match {
      case Failure(f) => println("No transformations specified ", f); List()
      case Success(t) => t
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

  /**
    * Get SplitTransformation logic
    * @return
    */
  def getSplitLogic: List[SplitTransformation] = {
    Try{
      val splitLogic = jsonDoc \ SPLIT
      splitLogic.extract[List[SplitTransformation]]
    } match {
      case Failure(f) => throw new Error("Looks like transformation rules for Split is not defined ", f)
      case Success(s) => s
    }
  }


  def getAggLogic: List[AggTransformation] = {
    Try{
      val aggLogic = jsonDoc \ AGGREGATOR
      aggLogic.extract[List[AggTransformation]]
    } match {
      case Failure(f) => throw new Error("Looks like transformation rules for Aggregation is not defined ", f)
      case Success(s) => s
    }
  }

  def getJoinLogic: List[JoinTransformation] = {
    Try{
      val joinLogic = jsonDoc \ JOINS
      joinLogic.extract[List[JoinTransformation]]
    } match {
      case Failure(f) => throw new Error("Looks like transformation rules for Join is not defined ", f)
      case Success(s) => s
    }
  }

}
