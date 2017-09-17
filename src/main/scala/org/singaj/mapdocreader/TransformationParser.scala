package org.singaj.mapdocreader

import org.apache.spark.sql.types.StructType
import org.singaj.rules._
import org.singaj.utils.STUtils

/**
  * Created by madhu on 9/16/17.
  */
class TransformationParser(val mapDocReader: MapDocReader) extends InOutFileStructure with STUtils{

  import mapDocReader._
  /**
    * Get Input output file formats if needed
    */

  /**
    * Override input and output file variables if needed.
    */
  override lazy val inputFilePath: String = ioFileFormat.getOrElse(INPUT_FILE_PATH, super.inputFilePath)
  override lazy val inputFileName: String = ioFileFormat.getOrElse(INPUT_FILE_NAME, super.inputFileName)
  override lazy val inputFileFormat: String  = ioFileFormat.getOrElse(INPUT_FILE_FORMAT, super.inputFileFormat)
  override lazy val inputFileDelimiter: String = ioFileFormat.getOrElse(INPUT_FILE_DELIMITER, super.inputFileDelimiter)

  override lazy val outputFilePath: String = ioFileFormat.getOrElse(OUTPUT_FILE_PATH, super.outputFilePath)
  override lazy val outputFileName: String = ioFileFormat.getOrElse(OUTPUT_FILE_NAME, super.outputFileName)
  override lazy val outputFileFormat: String = ioFileFormat.getOrElse(OUTPUT_FILE_FORMAT, super.outputFileFormat)
  override lazy val outputFileDelimiter: String = ioFileFormat.getOrElse(OUTPUT_FILE_DELIMITER, super.outputFileDelimiter)


  override lazy val inputFileHasHeader: Boolean = {
    if(ioFileFormat.contains(INPUT_FILE_HAS_HEADER))
      ioFileFormat.get(INPUT_FILE_HAS_HEADER) == Some("true")
    else super.inputFileHasHeader
  }


  override lazy val outputFileHasHeader: Boolean = {
    if(ioFileFormat.contains(OUTPUT_FILE_HAS_HEADER))
      ioFileFormat.get(OUTPUT_FILE_HAS_HEADER) == Some("true")
    else
      super.outputFileHasHeader
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

  def parseStructTypes: StructType = getFieldStructure

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
          JoinTransformation(filter, join.table, join.on,
            join.joinType, join.hint,
            join.additional_trans, join.keepOriginal) :: outputT

        case SimpleTransformation(a, b, c) => SimpleTransformation(a, b, c) :: outputT

      }

        parseAllTransformations(xs, inBet)
    }
  }


}
