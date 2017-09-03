package org.singaj.mapdocreader

/**
  * Created by madhu on 9/3/17.
  */
trait InOutFileStructure {

  def inputFilePath: String =""
  def inputFileName: String =""
  def inputFileFormat: String = "csv"
  def inputFileDelimiter: String = "|"
  def inputFileHasHeader: Boolean = true

  def outputFilePath: String = inputFilePath
  def outputFileName: String = inputFileName + "_output"
  def outputFileFormat: String = "csv"
  def outputFileDelimiter: String = "|"
  def outputFileHasHeader: Boolean = true

}
