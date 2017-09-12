package org.singaj.mapdocreader
import org.apache.spark.sql.types._
import org.singaj.rules.{FieldStructure, MapperConsts, SimpleTransformation, Transformations}
/**
  * Created on 8/26/17.
  * Abstract class to read Mapping Document
  */
abstract class MapDocReader extends InOutFileStructure with MapperConsts{

  /**
    * Input output file format
    */
  val ioFileFormat: Map[String, String]

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
    * Abstract method to retrieve all transformations. This method
    * will however not get transformations like SplitTransformation.
    * Use parseTransformations to get all the transformations
    * @return List[Transformations]: List of SimpleTransform
    *
    */
  def getTransformations: List[SimpleTransformation]

  /**
    * Abstract method to retrieve all FieldStructure
    * This will be used to build StructType for that can be used for spark schema.
    * Currently only following spark datatypes are handled:
    *   StringType
    *   IntegerType
    *   DoubleType
    *   LongType
    *   DateType
    *   BinaryType
    *   BooleanType
    *   TimestampType
    *   FloatType
    *   ByteType
    *   ShortType
    *   CalendarIntervalType
    *   NullType
    * If more complex and nested types are required, StructTypes has to be built the
    * usual spark way
    * @return StructType
    */
  def getFieldStructure: StructType


  /**
    * Get the list of transformation from JSON file.
    * This is the function that is generally called to retrieve all transformations
    * Transformations will be applied in the order in which they are listed in JSON file
    * @return
    */
  def parseTransformations: List[Transformations]

  /**
    * Recursive method takes in List of FieldStructure and generates List of spark StructField
    * @param fs List[FieldStructure]
    * @param sf List[StructField]
    * @return List of StructField
    */
  protected def structFieldBuilder(fs: List[FieldStructure], sf: List[StructField]): List[StructField] = {
    fs match {
      case Nil => sf.reverse
      case x::xs =>
        val structFields =  x match {
          case FieldStructure(a, "StringType", y) => StructField(a, StringType, y) :: sf
          case FieldStructure(a, "IntegerType", y) => StructField(a, IntegerType, y) :: sf
          case FieldStructure(a, "DoubleType", y) => StructField(a, DoubleType, y) :: sf
          case FieldStructure(a, "LongType", y) => StructField(a, LongType, y) :: sf
          case FieldStructure(a, "DateType", y) => StructField(a, DateType, y) :: sf
          case FieldStructure(a, "BinaryType", y) => StructField(a, BinaryType, y) :: sf
          case FieldStructure(a, "BooleanType", y) => StructField(a, BooleanType, y) :: sf
          case FieldStructure(a, "TimestampType", y) => StructField(a, TimestampType, y) :: sf
          case FieldStructure(a, "FloatType", y) => StructField(a, FloatType, y) :: sf
          case FieldStructure(a, "ByteType", y) => StructField(a, ByteType, y) :: sf
          case FieldStructure(a, "ShortType", y) => StructField(a, ShortType, y) :: sf
          case FieldStructure(a, "CalendarIntervalType", y) => StructField(a, CalendarIntervalType, y) :: sf
          case FieldStructure(a, "NullType", y) => StructField(a, NullType, y) :: sf
        }
        structFieldBuilder(xs, structFields )
    }
  }

}
