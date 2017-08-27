package org.singaj.mapdocreader

import scala.io.Source.fromFile
import scala.util.{Failure, Success, Try}
import cats.syntax.either._
import io.circe.{Decoder, Encoder, Json}
import io.circe.parser.parse
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.apache.spark.sql.types.StructType
import org.singaj.rules.MapperConsts
import org.singaj.rules.{FieldStructure, Transformations}


/**
  * Created by madhu on 8/26/17.
  */
class JSONMapDocReader(val filePath: String) extends MapDocReader with MapperConsts{

  /**
    * @constructor Reads file from given path.
    *              On success, parses json file and stores in jsonDoc
    *              On Failure, throws and error
    */
  protected lazy val fileContents: Try[String] = Try(fromFile(filePath).getLines().mkString)
  lazy val jsonDoc: Json = fileContents match {
    case Failure(f) => throw new Error(f)
    case Success(json) => parse(json).getOrElse(Json.Null)
  }

  /**
    * Implicit conversion required by io.circe for handling json to case class conversions
    */
  private implicit val transDecoder: Decoder[Transformations] = deriveDecoder[Transformations]
  private implicit val transEncoder: Encoder[Transformations] = deriveEncoder[Transformations]
  private implicit val structDecoder: Decoder[FieldStructure] = deriveDecoder[FieldStructure]
  private implicit val structEncoder: Encoder[FieldStructure] = deriveEncoder[FieldStructure]

  //Get cursor for Transformations
  private val cursor = jsonDoc.hcursor.downField(TRANSFORMATIONS)

  /**
    * Gets transformations defined in JSON file
    * @return List[Transformations]: List of transformations
    */
  def getTransformations: List[Transformations] = {
    val transformations = cursor.get[List[Transformations]](TRANSFORMS)
    transformations match {
      case Left(failure) => throw new Error("Failed to create Transformation case class " + failure)
      case Right(trans) => trans
    }
  }

  /**
    * Reads Field Structure from json and builds StructType
    * @return StructType: On Error, Empty StructType will be returned
    *                     On Success, StructType will be generated
    */
  def getFieldStructure: StructType = {
    val structInfo = cursor.get[List[FieldStructure]](STRUCT)
    structInfo match {
      case Left(failure) =>
        println("There was and error while retrieving Struct details proceeding without Struct ", failure)
        StructType(List())
      case Right(struct) => StructType(structFieldBuilder(struct, List()))
    }
  }

}
