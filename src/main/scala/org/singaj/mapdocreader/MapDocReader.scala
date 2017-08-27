package org.singaj.mapdocreader
import org.apache.spark.sql.types._
import org.singaj.rules.{FieldStructure, Transformations}
/**
  * Created on 8/26/17.
  * Abstract class to read Mapping Document
  */
abstract class MapDocReader {

  /**
    * Abstract method to retrieve all transformations
    * @return List[Transformations]: List of transformations
    */
  def getTransformations: List[Transformations]

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
    * Recursive method takes in List of FieldStructure and generates List of spark StructField
    * @param fs List[FieldStructure]
    * @param sf List[StructField]
    * @return List of StructField
    */
  protected def structFieldBuilder(fs: List[FieldStructure], sf: List[StructField]): List[StructField] = {
    fs match {
      case Nil => sf.reverse
      case x::xs => {
        val structFields =  x match {
          case FieldStructure(x, "StringType", y) => StructField(x, StringType, y) :: sf
          case FieldStructure(x, "IntegerType", y) => StructField(x, IntegerType, y) :: sf
          case FieldStructure(x, "DoubleType", y) => StructField(x, DoubleType, y) :: sf
          case FieldStructure(x, "LongType", y) => StructField(x, LongType, y) :: sf
          case FieldStructure(x, "DateType", y) => StructField(x, DateType, y) :: sf
          case FieldStructure(x, "BinaryType", y) => StructField(x, BinaryType, y) :: sf
          case FieldStructure(x, "BooleanType", y) => StructField(x, BooleanType, y) :: sf
          case FieldStructure(x, "TimestampType", y) => StructField(x, TimestampType, y) :: sf
          case FieldStructure(x, "FloatType", y) => StructField(x, FloatType, y) :: sf
          case FieldStructure(x, "ByteType", y) => StructField(x, ByteType, y) :: sf
          case FieldStructure(x, "ShortType", y) => StructField(x, ShortType, y) :: sf
          case FieldStructure(x, "CalendarIntervalType", y) => StructField(x, CalendarIntervalType, y) :: sf
          case FieldStructure(x, "NullType", y) => StructField(x, NullType, y) :: sf
        }
        structFieldBuilder(xs, structFields )
      }
    }
  }

}
