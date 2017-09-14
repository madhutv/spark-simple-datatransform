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
class JSONMapDocReader(val filePath: String) extends MapDocReader with MapperConsts with STUtils{

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
  protected lazy val fileContents: Try[String] = Try(fromFile(filePath).getLines().mkString)
  val jsonDoc: JValue = fileContents match {
    case Failure(f) => throw new Error("Cant find file " + filePath +  f)
    case Success(json) => parse(json)
  }

  /**
    * Get inputout file formats from JSON.
    * @return
    */
  def getIOFileFormat: Map[String, String] = {
    Try {
      val trans= jsonDoc \ IO_FILE_FORMAT
      trans.extract[Map[String, String]]
    } match {
      case Failure(f) => println("No transformations specified ", f); Map()
      case Success(t) => t
    }
  }

  /**
    * Get the list of transformation from JSON file.
    * This is the function that is generally called to retrieve all transformations
    * Transformations will be applied in the order in which they are listed in JSON file
    * @return
    */
  def parseTransformations: List[Transformations] = {
    val trans = getTransformations
    parseAllTransformations(trans, List())
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

  /**
    * Private function to parseAllTransformations
    * @param trans : List of SimpleTransform
    * @param outputT: Output Transformation after handling SplitTransformations
    * @return
    */
  private def parseAllTransformations(trans: List[SimpleTransformation], outputT: List[Transformations]): List[Transformations] = {
    /**
      * Get SplitTransformations if any of the transformations are marked as Split
      */
    lazy val splitT: List[SplitTransformation] = getSplitLogic

    /**
      * Get Aggransformations if any of the transformations are marked as aggregates
      */
    lazy val aggT: List[AggTransformation] = getAggLogic

    /**
      * Get Join transformations if any transformations are marked as joins
      */
    lazy val joinT: List[JoinTransformation] = getJoinLogic

    trans match {

      case Nil => outputT.reverse

      case x::xs => val inBet = x match {

        case SimpleTransformation(Some(SPLIT), a, b) =>
          //get split transactions. This will throw and error name is not found.
          val split = getOrThrow(splitT.find(f => f.name == b))
          SplitTransformation(a, split.dest_row_trans, split.source_row_trans) :: outputT
          //get Aggregation transations. This will throw an error if name is not found

        case SimpleTransformation(Some(AGGREGATE), a, b) =>
          val agg = getOrThrow(aggT.find(f => f.name == b))
          AggTransformation(a, agg.aggregates, agg.groupBy, agg.additional_trans, agg.keepOriginal) :: outputT

        case SimpleTransformation(Some(JOIN), filter, name) =>
          val join = getOrThrow(joinT.find(f => f.name == name))
          JoinTransformation(filter, join.embed, join.on,
                             join.joinType, join.hint,
                             join.additional_trans, join.keepOriginal) :: outputT

        case SimpleTransformation(a, b, c) => SimpleTransformation(a, b, c) :: outputT

      }

        parseAllTransformations(xs, inBet)
    }
  }


}
