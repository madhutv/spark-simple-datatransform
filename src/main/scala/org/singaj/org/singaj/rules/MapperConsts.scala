package org.singaj.rules

/**
  * Created by madhu on 8/26/17.
  */
trait MapperConsts {
  //consts in MapDocs
  lazy val TRANSFORMATIONS = "transformations"
  lazy val TRANSFORMS = "transforms"
  lazy val STRUCT = "struct"

  //consts used in transformations
  lazy val DIRECT_MAP = "directMap"
  lazy val DEFAULT_MAP = "defaultMap"
  lazy val IF_ELSE = "ifElse"
  lazy val EXPRESSION = "expression"
  lazy val CONCAT = "concat"
  lazy val SELECT = "select"
  lazy val SPLIT = "split"
  lazy val WHERE = "where"
  lazy val DROP = "drop"
  lazy val ORDER_BY = "orderBy"
  lazy val IO_FILE_FORMAT= "inputOutputFile"
  lazy val INPUT_FILE_PATH = "inputFilePath"
  lazy val INPUT_FILE_NAME = "inputFileName"
  lazy val INPUT_FILE_FORMAT = "inputFileFormat"
  lazy val INPUT_FILE_DELIMITER = "inputFileDelimiter"
  lazy val INPUT_FILE_HAS_HEADER = "inputFileHasHeader"
  lazy val OUTPUT_FILE_PATH = "outputFilePath"
  lazy val OUTPUT_FILE_NAME = "outputFileName"
  lazy val OUTPUT_FILE_FORMAT = "outputFileFormat"
  lazy val OUTPUT_FILE_DELIMITER= "outputFileDelimiter"
  lazy val OUTPUT_FILE_HAS_HEADER = "outputFileHasHeader"
}
