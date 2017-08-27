package org.singaj.mapdocreader

import scala.io.Source.fromFile
import scala.util.{Try, Failure, Success}
import cats.syntax.either._
import io.circe.{Json, HCursor, Decoder, Encoder}
import io.circe.parser.parse
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}


/**
  * Created by madhu on 8/26/17.
  */
class JSONMapDocReader(filePath: String) extends MapDocReader {

  /**
    * @constructor Reads file from given path.
    *              On success, parses json file and stores in jsonDoc
    *              On Failure, throws and error
    */
  protected val fileContents: Try[String] = Try(fromFile(filePath).getLines().mkString)
  val jsonDoc = fileContents match {
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

  

}
