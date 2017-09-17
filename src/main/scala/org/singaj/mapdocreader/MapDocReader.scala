package org.singaj.mapdocreader
import org.apache.spark.sql.types._
import org.singaj.rules._
/**
  * Created on 8/26/17.
  * Abstract class to read Mapping Document
  */
abstract class MapDocReader extends  MapperConsts{

  /**
    * Input output file format
    */
  val ioFileFormat: Map[String, String]



  def getIOFileFormat: Map[String, String]

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

  def getSplitLogic: List[SplitTransformation]

  def getAggLogic: List[AggTransformation]

  def getJoinLogic: List[JoinTransformation]

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
