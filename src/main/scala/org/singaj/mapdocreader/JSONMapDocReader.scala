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
    * Reads Field Structure from json and builds StructType
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
