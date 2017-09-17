package org.singaj.mapdocreader

/**
  * Created by madhu on 9/16/17.
  */
class TransformationParser(val mapDocReader: MapDocReader) extends InOutFileStructure {

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


}
